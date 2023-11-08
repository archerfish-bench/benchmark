import logging
import os
import time
import traceback
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import quote_plus

import sqlalchemy as sa
from sqlalchemy import inspect
from tqdm import tqdm

from utils.result_set_match import compare_results
from constants import *
from database_connectors import DatabaseConnector, DatabaseType
from database_connectors import create_connector

class InlineExecutor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def __init__(self, max_workers):
        pass

    def submit(self, fn, *args, **kwargs):
        future = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return future

    def shutdown(self, wait=True):
        pass

from concurrent.futures import Future


class TaskFailure(Exception):
    """
    Raised when any exception happens during processing of the task.
    """

    def __init__(self, message, query_info):
        super().__init__(message)
        self.query_info = query_info


class TaskResult:
    def __init__(self, query_info):
        # incoming query details
        self.query_info = query_info
        self.is_query_generated = False
        self.generated_query = None
        self.is_results_comparison_fine = False
        self.results_comparison_error = None
        self.table_check = False
        self.generated_query_tables = False
        self.golden_query_tables = False
        self.generation_time = -1
        self.generated_query_runtime = -1
        self.golden_query_runtime = -1
        self.query_id = None

    # def __bool__(self):
    #     return self.results_comparison_error is None

    def __str__(self):
        return (f"TaskResult(query_info={self.query_info}, is_query_generated={self.is_query_generated}, "
                f"generated_query={self.generated_query}, is_results_comparison_fine={self.is_results_comparison_fine}, "
                f"results_comparison_error={self.results_comparison_error}, "
                f"generation_time={self.generation_time}, "
                f"generated_query_runtime={self.generated_query_runtime}, golden_query_runtime={self.golden_query_runtime}, "
                f"query_name={self.query_name}, query_id={self.query_id})")

    def __repr__(self):
        return self.__str__()


class BenchmarkBase:
    def __init__(self, workload_data: dict, halt_on_error=False,
                 source_db_connector: Optional[DatabaseConnector] = None,
                 target_db_connector: Optional[DatabaseConnector] = None):
        self.data = workload_data
        self.halt_on_error = halt_on_error
        self.benchmark_config = None

        # Database connectors
        self.source_db_connector = source_db_connector
        self.target_db_connector = target_db_connector

        self.task_results = []
        self.use_threading = True
        self.threads_count = 5

    @abstractmethod
    def setup(self, benchmark_config: dict):
        self.benchmark_config = benchmark_config
        self.threads_count = benchmark_config.get('threads_count', 1)
        databases_config = benchmark_config.get(DATABASES)

        # source and target DB info should be available in configs
        if not databases_config or 'source' not in databases_config or 'target' not in databases_config:
            raise Exception("Please check the config file for source and target database details.")

        self.source_db_connector = create_connector(databases_config[SOURCE])

        # Check if the target database is the same as the source.
        target_config = databases_config[TARGET]
        if target_config.get(SAME_AS_SOURCE):
            self.target_db_connector = self.source_db_connector
        else:
            # different target connector.
            self.target_db_connector = create_connector(target_config)

        logging.info("Successfully setup the benchmark with source and target databases")

    def run_benchmark(self):
        if self.use_threading:
            executor_cls = ThreadPoolExecutor
        else:
            executor_cls = InlineExecutor

        logging.info(f"Starting benchmark with {self.threads_count} threads")
        with executor_cls(max_workers=self.threads_count) as executor:
            # Create and submit tasks to executor
            futures = self._submit_tasks_to_executor(executor)

            # Use tqdm to track progress
            errors = self._track_progress_and_handle_errors(futures, executor)

            print(f"All tasks completed, total:{len(futures)}, errors:{errors}")

    def _submit_tasks_to_executor(self, executor):
        futures = []
        print(f"Submitting {len(self.data)} tasks to executor")
        for key, value in self.data.items():
            future = executor.submit(self.run_query_and_compare, query_info=value)
            futures.append(future)
        logging.info(f"Submitted {len(self.data)} tasks to executor")
        return futures

    def _track_progress_and_handle_errors(self, futures, executor):
        errors = 0
        with tqdm(total=len(futures), unit="task", dynamic_ncols=True, desc="Processing Tasks") as pbar:
            try:
                for future in futures:
                    task_result = future.result()  # Wait for each task to complete
                    self.task_results.append(task_result)
                    if not task_result.is_results_comparison_fine:
                        errors += 1
                        self._handle_task_error(task_result)

                    pbar.update(1)  # Update the progress bar
                    error_rate = self._calculate_error_rate(errors, task_result, pbar)
                    pbar.set_postfix({"Error Rate (%)": f"{error_rate:.2f}"})
            except TaskFailure as e:
                logging.info(f"Task failed with error: {e}, trace: {traceback.format_exc()}")
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
        return errors

    def _calculate_error_rate(self, errors, test_result, pbar):
        # Calculate error rate and update the progress bar description
        errors += int(not test_result.is_results_comparison_fine)
        return (errors / (pbar.n + 1)) * 100

    def _handle_task_error(self, task_result):
        query_name = task_result.query_info[QUERY_NAME]
        if self.halt_on_error:
            print(f"Halting on error: {task_result.is_results_comparison_fine}, query_info:{query_name}")
            raise TaskFailure(
                message="Task failed with error:" + task_result.results_comparison_error,
                query_info=task_result.query_info)

    def get_results(self) -> list:
        return self.task_results

    def run_query_and_compare(self, query_info: dict) -> TaskResult:
        """
        Generate query, run it and get results. Compare with golden query results.
        """
        task_result = self.generate_query(query_info=query_info)
        tables = query_info.get(TABLES, None)
        task_result.golden_query_tables = tables

        if task_result.is_query_generated:
            gen_query_result = None
            golden_query_result = None

            # Run generated query in target database
            try:
                gen_query_result, runtime = self.run_query(task_result.generated_query,
                                                           db_connector=self.target_db_connector)
                task_result.generated_query_runtime = runtime
                logging.info(f"Generated query and runtime "
                             f"{task_result.generated_query}, {task_result.generated_query_runtime}")
            except Exception as e:
                task_result.is_query_generated = False
                task_result.is_results_comparison_fine = False
                task_result.results_comparison_error = str(e)
                logging.error(f"Exception running generated query: {query_info[QUERY_NAME]} : {traceback.format_exc()}")

            # Run golden query in source database
            try:
                golden_query_result, golden_query_runtime = self.run_query(query_info[GOLDEN_QUERY],
                                                                           db_connector=self.source_db_connector)
                task_result.golden_query_runtime = golden_query_runtime
                logging.info(f"Golden query and runtime "
                             f"{task_result.generated_query},  {task_result.generated_query_runtime}")
            except Exception as e:
                task_result.is_results_comparison_fine = False
                task_result.results_comparison_error = str(e)
                logging.error("Exception running golden query: %s", traceback.format_exc())

            # Table Check on need basis
            if tables is not None:
                task_result.golden_query_tables = tables
                self.table_check(task_result=task_result, golden_tables=tables)

            # Compare the results
            if gen_query_result is not None and golden_query_result is not None:
                try:
                    self.compare_query_results(task_result, gen_query_result, golden_query_result, query_info)
                except Exception as e:
                    task_result.is_results_comparison_fine = False
                    task_result.results_comparison_error = str(e)
                    logging.error(f"Error while comparing results for query: {query_info['query_name']} : {str(e)}")
                    logging.error("Exception traceback: %s", traceback.format_exc())

        self.log_results(task_result, query_info)

        if ((not task_result.is_query_generated or not task_result.is_results_comparison_fine)
                and self.halt_on_error):
            self.handle_errors("Task failed!", task_result, query_info)

        return task_result

    def table_check(self, task_result=None, golden_tables=None):
        if (None in (golden_tables, task_result.generated_query_tables) or
                len(golden_tables) != len(task_result.generated_query_tables)):
            task_result.table_check = False
        else:
            gen_tables = {g_table.lower() for g_table in task_result.generated_query_tables if g_table}
            golden_tables_lower = {g_table.lower() for g_table in golden_tables if g_table}
            task_result.table_check = (gen_tables == golden_tables_lower)
            task_result.generated_query_tables = gen_tables

    def compare_query_results(self, task_result: TaskResult, gen_query_result, golden_query_result, query_info: dict):
        """
        Compares the results of the generated and golden queries.
        """
        # compare runtime results of golden query and generated query
        (task_result.is_results_comparison_fine,
         task_result.results_comparison_error) = self._compare_results(query_info, gen_query_result,
                                                                      golden_query_result)

    def log_results(self, task_result: TaskResult, query_info: dict):
        """
        Logs the results of the current task.
        """
        logging.info(
            f"results for query_info: {query_info['query_name']}, "
            f"result_comparison: {task_result.is_results_comparison_fine},"
            f" error: {task_result.results_comparison_error}, query_id: {task_result.query_id}")

    def handle_errors(self, message: str, task_result: TaskResult, query_info: dict):
        """
        Handles any errors that might occur during the task.
        """
        task_result.is_results_comparison_fine = False
        raise TaskFailure(message=message, query_info=query_info)

    def _compare_results(self, query_info, expected_result, golden_result):
        """
        Compare the results of the query with the golden result.
        args:
            expected_result: result of the query
            golden_result: result of the golden query
        """
        source_db_type = self.source_db_connector.get_db_type()
        target_db_type = self.target_db_connector.get_db_type()
        result, error = compare_results(expected_result, golden_result,
                                        query_info.get('comparison_rules'),
                                        source_db_type=source_db_type,
                                        target_db_type=target_db_type)
        return result, error

    @abstractmethod
    def generate_query(self, query_info: dict) -> TaskResult:
        """
        Generate query, run it and get results
        Users can override this method to run queries in a different way in different LLM.
        Args:
            query_info: dictionary of details about query.
        Returns:
            generated_query: generated query
            results: results of the query in a list
            id: query id
            q_gen_time: query generation time
            q_run_time: query run time
        """
        pass

    @abstractmethod
    def run_query(self, query: str, db_connector: DatabaseConnector = None) -> (list, float):
        """
        Run the query and return the results.

        Args:
            query:
            db_connector:
        """
        try:
            if db_connector is None:
                raise Exception("db_connector can not be None")
            (result_list, runtime) = db_connector.execute_and_fetch_all(query)
            return result_list, runtime
        except Exception as e:
            # Handle other exceptions
            logging.error(f"Exception occurred while running query: {str(e)}")
            logging.error("Exception running query: %s", traceback.format_exc())
            raise e

    @abstractmethod
    def cleanup(self):
        if self.source_db_connector is not None:
            self.source_db_connector.cleanup()
        if self.target_db_connector is not None:
            self.target_db_connector.cleanup()

import logging
import openai
import os.path
import time
import traceback

from llama_index import ServiceContext, SQLDatabase
from llama_index.indices.struct_store import NLSQLTableQueryEngine
from llama_index.llms import OpenAI
from overrides import override
from sqlalchemy import create_engine, inspect

from benchmark.base_benchmark import BenchmarkBase, TaskResult

# Assumption is that you will have OPENAI key in your environment variable
openai.api_key = os.environ.get('OPENAI_API_KEY')


class LlamaIndexBenchmark(BenchmarkBase):
    """
    Provides very basic info for generating SQL via llama-index
    """

    def __init__(self, workload_data: dict, halt_on_error=False):
        super().__init__(workload_data, halt_on_error)
        self.llama_index_query_engine = None
        self.table_names = None

    @override
    def create_golden_query_engine(self, benchmark_config_data: dict):
        """
        Create golden query engine for the benchmark
        """
        try:
            connection_uri = benchmark_config_data.get('db_connection', None)
            engine = create_engine(connection_uri, connect_args={'check_same_thread': False})
            inspector = inspect(engine)
            self.table_names = inspector.get_table_names()
            return engine, inspector
        except Exception as ex:
            logging.error(f"Error while connecting to database: {ex}")
            print(f"Unable to connect to DB {connection_uri}. Check if you can modify the connection string in config file with absolute path")
            raise ex

    def setup(self, benchmark_config: dict):
        # This takes care of setting up the golden query engine
        # self.engine should contain the golden query engine
        super().setup(benchmark_config)

        # llama-index
        try:
            llm = OpenAI(temperature=0.5, model="gpt-4-0613")
            service_context = ServiceContext.from_defaults(llm=llm)
            sql_database = SQLDatabase(self.engine)

            self.llama_index_query_engine = NLSQLTableQueryEngine(
                sql_database=sql_database,
                tables=self.table_names,
                service_context=service_context,
            )
        except Exception as ex:
            logging.error(f"Error while initializing llama-index: {ex}")
            raise ex

    def generate_query(self, query_info: dict) -> TaskResult:
        """
           Generate SQL query using llama-index.

           This is just sample and reference implementation.

           This method generates the SQL for the text and returns it.

           Generated query is run by base class to complete the benchmark.

           Args:
               query_info: dictionary of details about query. comparison_rules, expected_query, query, query_info etc
            Returns:
                 result: task result containing generated query, time taken for generation etc
        """

        result = TaskResult(query_info=query_info)
        try:
            stime = time.time()
            response = self.llama_index_query_engine.query(query_info['question'])
            result.generated_query = response.metadata["sql_query"]
            result.is_query_generated = True
            result.generation_time = time.time() - stime
            return result
        except Exception as ex:
            # Handle other exceptions that may occur during query execution
            traceback.print_exc()
            logging.error(f"Error: {ex}")
            result.is_query_generated = False
            result.results_comparison_error = str(ex)

    @override
    def cleanup(self):
        """
        Cleanup the benchmark. In case you need to do any cleanup.
        """
        super().cleanup()

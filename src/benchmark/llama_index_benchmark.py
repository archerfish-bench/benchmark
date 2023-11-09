import logging
import openai
import os.path
import time
import traceback

import tiktoken
from llama_index import ServiceContext, SQLDatabase
from llama_index.callbacks import TokenCountingHandler, CallbackManager, LlamaDebugHandler, CBEventType
from llama_index.indices.struct_store import NLSQLTableQueryEngine
from llama_index.llms import OpenAI
from overrides import override
from sqlalchemy import create_engine, inspect

from base_benchmark import BenchmarkBase, TaskResult

# Assumption is that you will have OPENAI key in your environment variable
openai.api_key = os.environ.get('OPENAI_API_KEY')


class LlamaIndexBenchmark(BenchmarkBase):
    """
    Provides very basic info for generating SQL via llama-index
    """

    def __init__(self, workload_data: dict, halt_on_error=False):
        super().__init__(workload_data, halt_on_error)
        self.llama_debug = None
        self.token_counter = None
        self.llama_index_query_engine = None
        self.table_names = None

    def setup(self, benchmark_config: dict):
        # This takes care of setting up the golden query engine
        # self.engine should contain the golden query engine
        super().setup(benchmark_config)

        print(f"Source conn: {self.source_db_connector.get_db_type()}, target: {self.target_db_connector.get_db_type()}")

        # llama-index
        try:
            self.engine = self.target_db_connector.engine
            inspector = inspect(self.engine)
            self.table_names = inspector.get_table_names()

            model = "gpt-4-0613"
            llm = OpenAI(temperature=0.0, model=model)

            # setup callback handler for getting log info
            self.token_counter = TokenCountingHandler(
                tokenizer=tiktoken.encoding_for_model(model).encode
            )
            self.llama_debug = LlamaDebugHandler(print_trace_on_end=True)

            callback_manager = CallbackManager([self.token_counter, self.llama_debug])

            service_context = ServiceContext.from_defaults(llm=llm, callback_manager=callback_manager)
            sql_database = SQLDatabase(engine=self.engine)

            self.llama_index_query_engine = NLSQLTableQueryEngine(
                sql_database=sql_database,
                tables=self.table_names,
                service_context=service_context,
            )
        except Exception as ex:
            logging.error(f"Error while initializing llama-index: {ex}")
            raise ex

    def __print_debug_info(self):
        logging.info(f"Token counter: total tokens: {self.token_counter.total_embedding_token_count}")
        print(
            "Embedding Tokens: ",
            self.token_counter.total_embedding_token_count,
            "\n",
            "LLM Prompt Tokens: ",
            self.token_counter.prompt_llm_token_count,
            "\n",
            "LLM Completion Tokens: ",
            self.token_counter.completion_llm_token_count,
            "\n",
            "Total LLM Token Count: ",
            self.token_counter.total_llm_token_count,
            "\n",
        )
        print(self.llama_debug.get_event_time_info(CBEventType.LLM))

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
            # Enable this on need basis
            #self.__print_debug_info()
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

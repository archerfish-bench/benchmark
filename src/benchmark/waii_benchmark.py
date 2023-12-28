import logging
import time
import traceback
import uuid

import yaml
from waii_sdk_py import WAII
from waii_sdk_py.query import QueryGenerationRequest, Tweak
from constants import *

from base_benchmark import BenchmarkBase, TaskResult
from waii_utils import add_contexts


def generate_query(question: str, request: QueryGenerationRequest):
    """
    Generate SQL query using WAII
    Args:
        question: text question
        request: QueryGenerationRequest
    """
    logging.info(f"Question: {question}")
    stime = time.time()
    response = WAII.Query.generate(request)
    gen_query = response.query
    tables = response.tables
    table_names = [table.table_name for table in tables] if tables else []
    query_gen_time = (time.time() - stime)
    logging.info(f"Query: {gen_query}")
    return query_gen_time, gen_query, table_names


class WaiiBenchmark(BenchmarkBase):
    """
    WAII provides python SDK for converting text-to-sql and this class, makes use of it.

    It overrides the following methods from base benchmark

        1. setup: to read the config file and initialize WAII
        2. generate_query: to post the text question to WAII and get the SQL back.

    Generated query is run by the base benchmark by default.
    If you want to override, you can do so by overriding the run_query method.
    """

    def setup(self, benchmark_config: dict):
        super().setup(benchmark_config)

        waii_configs = benchmark_config['waii_configs']

        # Initialize WAII
        WAII.initialize(url=waii_configs['url'], api_key=waii_configs['api_key'])
        WAII.Database.activate_connection(waii_configs[WAII_DB_CONNECTION_KEY])
        logging.info(f"Activated connection: {waii_configs[WAII_DB_CONNECTION_KEY]}")

        # Add semantic contexts
        add_contexts(config=waii_configs)

    def get_search_context(self, query_info: dict):
        schemas = query_info['schemas'] or ["*"]
        auto_select_schema = query_info[AUTO_SELECT_SCHEMA]
        search_context = None
        if auto_select_schema == 'false':
            search_context = [{"db_name": '*', "schema_name": s, "table_name": '*'} for s in schemas]
            logging.info(f"search_context: {search_context}")
        return search_context

    def generate_query(self, query_info: dict) -> TaskResult:
        """
           Generate SQL query using WAII, if tweaks are provided, generate tweaked queries as well.
           Args:
               query_info: dictionary of details about query. comparison_rules, expected_query, query, query_info etc
            Returns:
                 result: task result containing generated query
        """

        result = TaskResult(query_info=query_info)
        try:
            query_name = query_info[QUERY_NAME]
            question = query_info[QUESTION]

            search_context = self.get_search_context(query_info=query_info)

            gen_query, query_id = self.generate_initial_query(question, result, search_context)

            # If there are tweaks in payload, generate tweaked queries as well
            if 'tweaks' in query_info and query_info[TWEAKS] is not None:
                previous_sql = gen_query
                tweaks = [Tweak(sql=previous_sql, ask=question)]
                for tweak in query_info[TWEAKS]:
                    logging.info(f"Generating Tweak for {query_name}: {tweak}, gen_query: {gen_query}")
                    tweak_request = QueryGenerationRequest(uuid=query_id,
                                                           search_context=search_context,
                                                           ask=tweak,
                                                           dialect='snowflake',
                                                           tweak_history=tweaks
                                                           )
                    (gen_query_time, gen_query, tables) = generate_query(question=tweak, request=tweak_request)
                    logging.info(f"Query {query_name} after tweak: {gen_query}")
                    result.generation_time = gen_query_time
                    result.generated_query = gen_query
                    result.generated_query_tables = tables
                    tweaks.append(Tweak(sql=gen_query, ask=tweak))

            return result
        except yaml.YAMLError as e:
            # Handle YAML parsing errors
            logging.error(f"YAML Parsing Error: {e}")
            result.is_query_generated = False
            result.results_comparison_error = str(e)
            return result
        except Exception as ex:
            # Handle other exceptions that may occur during query execution
            logging.error(f"Error: {ex}")
            result.is_query_generated = False
            result.results_comparison_error = "generated_query:" + str(ex)
            return result

    def generate_initial_query(self, question, result, search_context):
        # Generate the query
        query_id = str(uuid.uuid4())
        (gen_query_time, gen_query, tables) = generate_query(question=question,
                                                     request=QueryGenerationRequest(uuid=query_id,
                                                                                    search_context=search_context,
                                                                                    ask=question,
                                                                                    dialect='snowflake'))
        result.generation_time = gen_query_time
        result.generated_query = gen_query
        result.is_query_generated = True
        result.query_id = query_id
        result.generated_query_tables = tables
        return gen_query, query_id

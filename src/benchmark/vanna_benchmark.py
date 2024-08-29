import os.path
import time

import snowflake
from transformers import BertTokenizer, BertModel
from vanna.milvus import Milvus_VectorStore
from vanna.openai import OpenAI_Chat

from benchmark.base_benchmark import BenchmarkBase, TaskResult


class BERTEmbeddingFunction:
    """
    This will be passed as embedding function to Milvus.
    Milvus still needs to update their openAI to latest model. Until then we will use BERT.
    """

    def __init__(self):
        # Load pre-trained BERT model and tokenizer
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.model = BertModel.from_pretrained('bert-base-uncased')

    def encode_documents(self, texts):
        return self._encode(texts)

    def encode_queries(self, texts):
        return self._encode(texts)

    def _encode(self, texts):
        embeddings = []
        for text in texts:
            inputs = self.tokenizer(text, return_tensors='pt', max_length=512, truncation=True, padding='max_length')
            outputs = self.model(**inputs)
            # Get the mean of the last hidden state to represent the embedding
            embedding = outputs.last_hidden_state.mean(dim=1).detach().numpy().flatten()
            embeddings.append(embedding)
        return embeddings


class MyVanna(Milvus_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        if config:
            # init openAI
            OpenAI_Chat.__init__(self, config=config)

            # init milvus
            config['embedding_function'] = BERTEmbeddingFunction()
            Milvus_VectorStore.__init__(self, config)
            print("Initialized Milvus with BERT Embedding Function")


def read_context_from_file(file_path: str):
    """
    Args:
        file_path: should be complete path, where contexts are stored
    Returns:

    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found. Provide complete path of file.")
    with open(file_path, "r") as f:
        return f.read()


class VannaBenchmark(BenchmarkBase):
    """
    - VannaBenchmark class for benchmarking Vanna.
    - Supports snowflake for now, as we need to provide the schema for training.
    - We pass the DDLs for entire schema and any context information that can help in improving accuracy of the query gen.
    """

    def setup(self, benchmark_config: dict):
        """
        Setup method for VannaBenchmark
        Args:
            benchmark_config: dict
        """
        super().setup(benchmark_config)
        ddl, context = self.get_ddl_for_schema(benchmark_config=benchmark_config)

        print(f"Got the ddl : {ddl}")
        print(f"Got the context : {context}")

        # now setup Vanna with openAI gpt-4o
        open_ai_key = self.get_from_config('open_ai_key')
        self.vn_milvus = MyVanna(config={'api_key': open_ai_key, 'model': 'gpt-4o'})

        conn_params = self.parse_snowflake_conn_str(self.get_from_config('snowflake_conn_str'))

        self.vn_milvus.connect_to_snowflake(username=conn_params.get('user'),
                                       password=conn_params.get('password'),
                                       account=conn_params.get('account'),
                                       warehouse=conn_params.get('warehouse'),
                                       database=conn_params.get('database'),
                                       schema=conn_params.get('schema'))
        print(f"Vanna with milvus has been setup..")

        self.vn_milvus.train(ddl=ddl, documentation=context)
        print(f"Vanna has been trained on the DB schema and context..")

    def parse_snowflake_conn_str(self, conn_str: str):
        """
        Parse a Snowflake connection string and return a dictionary of parameters.
        """
        # user=myuser password=mypassword account=myaccount warehouse=mywarehouse database=mydatabase schema=myschema
        params = dict(param.split('=') for param in conn_str.split())
        return params

    def get_ddl_for_schema(self, benchmark_config: dict):
        """
        For a given schema, get its entire ddl
        """

        def connect_to_snowflake():
            try:
                conn_str = self.get_from_config('snowflake_conn_str')
                if conn_str:
                    conn_params = self.parse_snowflake_conn_str(conn_str)
                    conn = snowflake.connector.connect(
                        user=conn_params.get('user'),
                        password=conn_params.get('password'),
                        account=conn_params.get('account'),
                        warehouse=conn_params.get('warehouse'),
                        database=conn_params.get('database'),
                        schema=conn_params.get('schema')
                    )
                    return conn
                else:
                    raise ValueError("Connection string is missing in the configuration.")
            except Exception as e:
                print(f"Error establishing Snowflake connection: {e}")
                return None

        conn = connect_to_snowflake()
        schema = self.get_from_config('schema')
        print(f"Connected to db. schema: {schema}")
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN SCHEMA {schema}")
        tables = cur.fetchall()
        ddl_collection = ""
        for table in tables:
            table_name = table[1]
            cur.execute(f"SELECT GET_DDL('TABLE', '{schema}.' || '{table_name}')")
            ddl = cur.fetchone()[0]
            ddl_collection += ddl + "\n\n"

        cur.close()
        conn.close()

        # Read the context
        context = read_context_from_file(self.get_from_config('context_file'))
        print(f"Read all the DDL and the context file details. Good to go.")
        return ddl_collection, context

    def get_from_config(self, key):
        return self.benchmark_config.get('vanna_configs', {}).get(key, '')

    def generate_query(self, query_info: dict) -> TaskResult:
        result = TaskResult(query_info=query_info)
        stime = time.time()
        try:
            sql = self.vn_milvus.generate_sql(query_info['question'],allow_llm_to_see_data=True)
            print(f"Generated SQL: {sql}")
            result.generated_query = sql
            result.is_query_generated = True
        except Exception as e:
            print(f"Error generating SQL: {e}")
            result.error = str(e)
            result.is_query_generated = False
            result.results_comparison_error = "generated_query:" + str(e)
        finally:
            result.generation_time = (time.time() - stime)
        return result




import json
import logging
import subprocess
import tempfile
import time
import traceback

from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict
from urllib.parse import urlparse

import sqlalchemy
import re
from overrides import override
from sqlalchemy.exc import SQLAlchemyError


class DatabaseType(Enum):
    SQLALCHEMY = "sqlalchemy"

# Base class for all connectors


class DatabaseConnector(ABC):

    def __init__(self, config: Dict):
        self.type = config.get('type')

    @abstractmethod
    def execute_and_fetch_all(self, query: str) -> tuple[List[Dict], float]:
        """
        Execute the query and return the results along with runtime.
        Args:
            query: Query to be executed
        Returns: Tuple of results and runtime
        """
        pass

    def cleanup(self):
        pass

    def get_db_type(self):
        return self.type


# Connector for SQLAlchemy databases
class SqlAlchemyConnector(DatabaseConnector):

    def __init__(self, config: Dict):
        super().__init__(config)
        connection_string = config.get('connection')
        if not connection_string:
            raise ValueError("The 'connection' key is missing in the configuration.")

        # Parse the connection string to get the scheme (database type)
        parsed_url = urlparse(connection_string)
        db_type = parsed_url.scheme

        # Add check_same_thread=False if the database is SQLite
        connect_args = {}
        if db_type == "sqlite":
            connect_args["check_same_thread"] = False

        self.engine = sqlalchemy.create_engine(connection_string, connect_args=connect_args)
        self.connection = self.engine.connect()

    @override
    def execute_and_fetch_all(self, query: str) -> tuple[List[Dict], float]:
        try:
            # if the query is str, need to wrap it in text(str)
            if isinstance(query, str):
                query = sqlalchemy.text(query)

            stime = time.time()
            result = self.connection.execute(query)
            rows = result.fetchall()

            column_names = result.keys()

            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                row_dict = dict(zip(column_names, row))
                data.append(row_dict)
            return data, (time.time() - stime)
        except SQLAlchemyError as e:
            # traceback.print_exc()
            logging.error(f"SQLAlchemy Exception occurred while running query: {str(e)}")
            raise e
        except Exception as e:
            # Handle other exceptions
            logging.error(f"Exception occurred while running query: {str(e)}")
            raise e

    @override
    def cleanup(self):
        if self.engine:
            self.engine.dispose()


def create_connector(db_config: Dict) -> DatabaseConnector:
    """
    Factory to create connectors
    Args:
        db_config:
    Returns: one of the database connectors
    """
    if db_config['type'] == DatabaseType.SQLALCHEMY.value.lower():
        return SqlAlchemyConnector(db_config)

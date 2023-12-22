import json
import logging
import os
import uuid

from waii_sdk_py import WAII
from waii_sdk_py.semantic_context import SemanticStatement, ModifySemanticContextRequest

from constants import WAII_CONTEXT, WAII_DB_CONNECTION_KEY


def add_contexts(config: dict):
    """
    Add semantic contexts to WAII
    Args:
        benchmark_config: benchmark config
    """
    context_file = config.get(WAII_CONTEXT)
    if context_file is not None:
        context_file_path = get_context_file_path(context_file)
        # check if it is absolute path
        if os.path.exists(context_file):
            context_file_path = context_file
            print(f"Using context file {context_file_path}")
        if os.path.exists(context_file_path):
            with open(context_file_path, 'r') as f:
                sem_context_array = json.load(f)
                statements = []
                for ctx in sem_context_array:
                    stmt = ctx['statement']
                    scope = ctx['scope']
                    statements.append(SemanticStatement(statement=stmt, scope=scope, id=str(uuid.uuid4())))

                # Finally add all contexts
                response = WAII.SemanticContext.modify_semantic_context(
                    ModifySemanticContextRequest(updated=statements))
                logging.info(f"Added {len(response.updated)} contexts")
                print(f"Added {len(response.updated)} contexts")
        else:
            print(f"Context file {context_file_path} does not exist. Skipping adding contexts.")

        logging.info(f"Added sem contexts in Waii: {config[WAII_DB_CONNECTION_KEY]}")


def get_context_file_path(context_file: str) -> str:
    """
    Get the absolute path of the context file.
    Args:
        context_file: context file name
    """

    # Get the directory of the current script (benchmark.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "../../resources/config/", context_file)

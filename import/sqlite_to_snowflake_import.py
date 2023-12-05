import argparse
import json
import os
import sqlite3
import traceback
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from snowflake.sqlalchemy import URL


def get_engine(db_type, db_credentials_file):
    with open(db_credentials_file, 'r') as file:
        all_credentials = json.load(file)

    credentials = all_credentials.get('snowflake')
    if not credentials:
        raise ValueError(f"Unsupported or missing credentials for database type: {db_type}")

    return create_engine(URL(**credentials))


def import_sqlite_to_db(db_type, db_credentials, base_directory, dev_folders=None):
    engine = get_engine(db_type, db_credentials)
    session = sessionmaker(bind=engine)()

    # User should have created SPIDER_DEV database or relevant database earlier as part of requirement.
    # Details should be there in db_config.json

    for folder in os.listdir(base_directory):
        if dev_folders and (dev_folders != "*") and folder not in dev_folders:
            continue

        # base_folder/dev_folder/table.sqlite
        folder_path = os.path.join(base_directory, folder)
        if os.path.isdir(folder_path) and f"{folder}.sqlite" in os.listdir(folder_path):
            sqlite_file_path = os.path.join(folder_path, f"{folder}.sqlite")
            sqlite_conn = sqlite3.connect(sqlite_file_path)
            sqlite_conn.text_factory = lambda b: b.decode(errors='ignore')

            table_name = ""
            try:

                # Drop schema in Snowflake if it exists
                drop_stmt = f"DROP SCHEMA IF EXISTS {folder.lower()} CASCADE;"
                print(drop_stmt)
                session.execute(drop_stmt)
                session.commit()

                # Create schema in Snowflake
                create_stmt = f"CREATE SCHEMA IF NOT EXISTS {folder.lower()};"
                print(create_stmt)
                session.execute(create_stmt)
                session.commit()

                # Set the search path to the target schema if specified
                use_schema_stmt = f"USE SCHEMA {folder.lower()};"
                print(use_schema_stmt)
                session.execute(use_schema_stmt)
                session.commit()

                # Get the list of tables in the SQLite database
                tables = sqlite_conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

                # Import each table to database
                for table_name, in tables:
                    if table_name == 'sqlite_sequence':
                        continue

                    df = pd.read_sql_query(f"SELECT * FROM '{table_name}';", sqlite_conn)

                    # Rename columns to lower case
                    df.rename(columns={col: col.lower() for col in df.columns}, inplace=True)

                    df.to_sql(
                        table_name.lower(),
                        engine,
                        schema=folder.lower(),
                        if_exists="replace",
                        index=False,
                        method="multi",
                        chunksize=10000
                    )

                    print(f"Successfully imported {table_name.lower()} to schema: {folder.lower()}")

            except IntegrityError as e:
                print(f"IntegrityError for table {table_name}: {e}. Proceeding further")
            except Exception as e:
                traceback.print_exc()
                print(f"Error importing {folder}: {e}")

            finally:
                sqlite_conn.close()

    engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import SQLite databases to another DBMS.')
    parser.add_argument('base_directory', type=str, help='Base directory containing the SQLite databases')
    parser.add_argument('--schemas', type=str, nargs='+', default='*',
                        help='List of specific folders to process or "*" to process all folders')

    args = parser.parse_args()

    BASE_DIRECTORY = args.base_directory
    SQLITE_FOLDERS = set(args.schemas) if args.schemas != '*' else '*'

    import_sqlite_to_db(db_type='snowflake', db_credentials='db_config.json',
                        base_directory=BASE_DIRECTORY, dev_folders=SQLITE_FOLDERS)

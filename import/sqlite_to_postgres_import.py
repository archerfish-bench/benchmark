"""
Simple program to import SQLite databases into PostgreSQL.

Install pgloader via "brew install pgloader"

usage: sqlite_to_postgres_import.py [-h] [--schemas [SCHEMAS ...]] base_dir
e.g: python sqlite_to_postgres_import.py /tmp/spider/spider/database/ --schemas car_1 flight_2

base_dir is the place where sqlite files are stored in spider
"""
import argparse
import json
import os
import sqlite3
import subprocess

import psycopg2


def run_pgloader(sqlite_db_path, postgres_connection_params):
    """
        Imports data from a SQLite database to PostgreSQL using pgloader.

        Args:
            sqlite_db_path (str): Path to the SQLite database file.
            postgres_connection_params (dict): PostgreSQL database connection parameters.
    """
    try:
        pgloader_cmd = [
            'pgloader', sqlite_db_path,
            f'postgresql:///{postgres_connection_params["database"]}?'
            f'host={postgres_connection_params["host"]}&'
            f'port={postgres_connection_params["port"]}'
        ]
        subprocess.run(pgloader_cmd, check=True)
        print(f"SQLite data from {sqlite_db_path} imported into PostgreSQL successfully.")
    except Exception as e:
        print(f"Error importing SQLite data from {sqlite_db_path}: {e}")


def create_schema_if_not_exists(schema_name, postgres_connection_params):
    """
       Creates a schema in the PostgreSQL database if it does not already exist.

       Args:
           schema_name (str): The name of the schema to be created.
           postgres_connection_params (dict): PostgreSQL database connection parameters.
    """
    try:
        conn = psycopg2.connect(**postgres_connection_params)
        cursor = conn.cursor()
        drop_schema_sql = f"DROP SCHEMA IF EXISTS {schema_name};"
        cursor.execute(drop_schema_sql)
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        cursor.execute(create_schema_sql)
        conn.commit()
        conn.close()
        print(f"Schema {schema_name} created successfully.")
    except Exception as e:
        print(f"Error creating/dropping schema {schema_name}: {e}")


def move_tables(table_names, target_schema, postgres_connection_params):
    """
        Moves tables from the public schema to the target schema in PostgreSQL.

        Args:
            table_names (list): List of table names to be moved.
            target_schema (str): The target schema where tables should be moved.
            postgres_connection_params (dict): PostgreSQL database connection parameters.
    """
    try:
        conn = psycopg2.connect(**postgres_connection_params)
        cursor = conn.cursor()

        for table_name in table_names:
            move_table_sql = f"ALTER TABLE public.{table_name} SET SCHEMA {target_schema};"
            cursor.execute(move_table_sql)

        conn.commit()
        conn.close()

        print("Tables moved to the target schema successfully.")
    except Exception as e:
        print(f"Error moving tables: {e}")


def get_sqlite_table_names(sqlite_db_path):
    """
        Retrieve names of all tables from SQLite database.

        Args:
            sqlite_db_path (str): Path to the SQLite database file.

        Returns:
            list: A list of table names.
    """
    try:
        sqlite_conn = sqlite3.connect(sqlite_db_path)
        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        sqlite_conn.close()
        return [name[0] for name in table_names]
    except Exception as e:
        print(f"Error retrieving SQLite table names: {e}")
        return []


def import_schemas(base_dir, schemas, postgres_connection_params):
    """
       Import schemas from SQLite databases to PostgreSQL.

       Args:
           base_dir (str): The base directory containing SQLite files.
           schemas (list): List of schemas to import, or '*' for all.
           postgres_connection_params (dict): PostgreSQL database connection parameters.
    """
    if not os.path.exists(base_dir):
        print(f"Error: {base_dir} does not exist.")
        return
    if not os.path.isdir(base_dir):
        print(f"Error: {base_dir} is not a directory.")
        return
    for folder in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder)
        if os.path.isdir(folder_path) and (schemas == '*' or folder in schemas):
            sqlite_db_path = os.path.join(folder_path, f"{folder}.sqlite")
            if os.path.isfile(sqlite_db_path):
                print(f"Creating schema {folder} in PostgreSQL...")
                create_schema_if_not_exists(folder, postgres_connection_params)

                print(f"\tImporting to public schema")
                run_pgloader(sqlite_db_path=sqlite_db_path, postgres_connection_params=postgres_connection_params)

                print(f"\tGetting table names to be moved")
                table_names = get_sqlite_table_names(sqlite_db_path)

                print(f"\tMoving tables to {folder} schema in PostgreSQL...")
                move_tables(table_names, folder, postgres_connection_params)


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Import SQLite databases into PostgreSQL')
    parser.add_argument('base_dir', help='Base directory containing SQLite files')
    parser.add_argument('--schemas', nargs='*', default='*',
                        help='List of schemas to import, or "*" for all (default: "*")')

    # Parse arguments
    args = parser.parse_args()

    # Load database connection parameters from db_config.json
    with open('db_config.json', 'r') as f:
        postgres_connection_params = json.load(f).get('postgres')

    # Import schemas
    import_schemas(args.base_dir, args.schemas, postgres_connection_params)


if __name__ == "__main__":
    main()

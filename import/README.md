# Pre-installation Requirements
Ensure the following prerequisites are met before running the script:

### Python Installation
- Install Python 3.6 or a later version.

### Dependency Installation
- Install `pgloader` using brew:
  ```bash
  brew install pgloader  
  ```
- Install python dependencies:
  ```bash
  pip3 install -r requirements.txt
  ```  

# SQLite to PostgreSQL Import Script

## Running the script
`sqlite_to_postgres_import` script uses `pgloader` to import SQLite databases from a specified data directory into a PostgreSQL database. 

Follow these steps for execution:
 
1. Create a database in postgres (e.g spider_dev)
2. Update db_config.json with your PostgreSQL connection details.
2. Use the following command to import sqlite files to postgres database

```
usage: sqlite_to_postgres_import.py [-h] [--schemas [SCHEMAS ...]] base_dir

e.g
-- imports all folders into postgres
python sqlite_to_postgres.py $SPIDER_HOME_DIR/database

-- to import specific schemas from sqlite to postgres. Following command will port car_1 and flight_2 schemas to postgres.
python sqlite_to_postgres.py $SPIDER_HOME_DIR/database --schemas car_1 flight_2
```
This takes care of PK/FK constraints as well.


# SQLite to Snowflake Import Script
`sqlite_to_snowflake_import` imports schemas and data from a specified data directory into a Snowflake database.
For PK/FK constraints, run `spider_constraints.sql` in Snowflake after running the import script.

```
usage: sqlite_to_snowflake_import.py [-h] [--schemas SCHEMAS [SCHEMAS ...]]
                                     base_directory
e.g
-- imports all folders into snowflake
python sqlite_to_snowflake_import.py $SPIDER_HOME_DIR/database

-- to import specific schemas from sqlite to snowflake. Following command will port car_1 and flight_2 schemas to snowflake.
python sqlite_to_snowflake_import.py $SPIDER_HOME_DIR/database --schemas car_1 flight_2
```

You can use the relevant PK/FK constraints from `spider_constraints.sql` to create constraints in Snowflake.
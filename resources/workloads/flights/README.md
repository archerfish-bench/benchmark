
# DataSet: Flights
Download data from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7

# SQL for creating tables

Change to the database and schema that you want to create the tables in.

```sql
-- create schema if needed
create schema if not exists tweakit_perf_db.flights;
use schema tweakit_perf_db.flights;


-- create file format spec
CREATE OR REPLACE FILE FORMAT my_flights_csv_format 
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
SKIP_HEADER = 1 
ESCAPE = '\\' 
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;


-- create staging folder
CREATE OR REPLACE TEMPORARY STAGE tweakit_perf_db_stage FILE_FORMAT = my_flights_csv_format;


-- treat NA/None accordingly
ALTER FILE FORMAT my_flights_csv_format SET NULL_IF = ('None');
ALTER FILE FORMAT my_flights_csv_format SET NULL_IF = ('');
ALTER FILE FORMAT my_flights_csv_format SET NULL_IF = ('NA');


-- clean up any existing files
remove @tweakit_perf_db_stage/carriers.csv;
remove @tweakit_perf_db_stage/airports.csv;
remove @tweakit_perf_db_stage/plane-data.csv;
remove @tweakit_perf_db_stage/2008.csv;
remove @tweakit_perf_db_stage/2007.csv;
remove @tweakit_perf_db_stage/2006.csv;
remove @tweakit_perf_db_stage/2005.csv;
remove @tweakit_perf_db_stage/2004.csv;
remove @tweakit_perf_db_stage/2003.csv;
remove @tweakit_perf_db_stage/2002.csv;

-- add files to staging
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/carriers.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/airports.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/plane-data.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2008.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2007.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2006.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2005.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2004.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2003.csv @tweakit_perf_db_stage;
PUT file:///Users/rajeshbalamohan/Downloads/flights_harvard/2002.csv @tweakit_perf_db_stage;


-- Create relevant tables

CREATE OR REPLACE TABLE airports (
    iata VARCHAR(100),
    airport VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(500),
    country VARCHAR(100),
    lat FLOAT,
    long FLOAT
);


CREATE OR REPLACE TABLE carriers (
    Code VARCHAR(100),
    Description VARCHAR(255)
);


CREATE OR REPLACE  TABLE planedata (
    tailnum VARCHAR(100),
    type VARCHAR(500),
    manufacturer VARCHAR(500),
    issue_date DATE,
    model VARCHAR(500),
    status VARCHAR(200),
    aircraft_type VARCHAR(500),
    engine_type VARCHAR(500),
    year INT
);


CREATE OR REPLACE TABLE flightperformance (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier VARCHAR(100),
    FlightNum INT,
    TailNum VARCHAR(100),
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin VARCHAR(100),
    Dest VARCHAR(100),
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode VARCHAR(500),
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
);



-- Import the dataset into tables

COPY INTO tweakit_perf_db.flights.carriers  
FROM @tweakit_perf_db_stage/carriers.csv  
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';

COPY INTO tweakit_perf_db.flights.airports  
FROM @tweakit_perf_db_stage/airports.csv
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';

COPY INTO tweakit_perf_db.flights.planedata  
FROM @tweakit_perf_db_stage/plane-data.csv
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';

COPY INTO tweakit_perf_db.flights.FLIGHTPERFORMANCE  
FROM @tweakit_perf_db_stage/2008.csv  
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';

COPY INTO tweakit_perf_db.flights.FLIGHTPERFORMANCE  
FROM @tweakit_perf_db_stage/2007.csv  
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';

COPY INTO tweakit_perf_db.flights.FLIGHTPERFORMANCE  
FROM @tweakit_perf_db_stage/2006.csv  
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';

COPY INTO tweakit_perf_db.flights.FLIGHTPERFORMANCE  
FROM @tweakit_perf_db_stage/2005.csv  
FILE_FORMAT = (FORMAT_NAME = my_flights_csv_format)  
ON_ERROR = 'skip_file';


-- Grant relevant permissions
GRANT USAGE ON SCHEMA TWEAKIT_PERF_DB.flights TO ROLE TWEAKIT_PERF_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA TWEAKIT_PERF_DB.flights TO ROLE TWEAKIT_PERF_ROLE;
GRANT CREATE TABLE ON SCHEMA TWEAKIT_PERF_DB.flights TO ROLE TWEAKIT_PERF_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA tweakit_perf_db.flights TO ROLE TWEAKIT_USER_ROLE;

```

# Sanity checks
```text
select count(*) from airports;
+----------+
| COUNT(*) |
|----------|
|     3376 |
+----------+
1 Row(s) produced. Time Elapsed: 0.659s

select count(*) from CARRIERS;
+----------+
| COUNT(*) |
|----------|
|     1491 |
+----------+
1 Row(s) produced. Time Elapsed: 0.402s

select count(*) from FLIGHTPERFORMANCE;
+----------+
| COUNT(*) |
|----------|
| 24124950 |
+----------+
1 Row(s) produced. Time Elapsed: 0.275s

select count(*) from PLANEDATA;
+----------+
| COUNT(*) |
|----------|
|     4480 |
+----------+
1 Row(s) produced. Time Elapsed: 0.375s


```


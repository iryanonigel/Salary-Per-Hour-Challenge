# SALARY PER HOUR ETL

ETL code for salary_per_hour table.

## Prerequisites

Before running the script, ensure you have the following prerequisites installed:

- Python (3.9+ Recommended)
- Postgresql (15+ Recommended)
- All python modules in requirements.txt

## Getting Started

To get started with the script, follow these steps:

1. Clone the repository:

2. Make sure to keep all files in the same directory before running the script. 
The script relies on specific file paths and dependencies within the project directory.

Here is an example of how your files should be organized:

- csv_to_pg.py: The main script file for ingest data from csv to postgres incrementally (take arguments from config file)
- pg_to_pg.py: The main script file for transformation in postgres using query (take arguments from config file)
- pipeline.sh: The main script file for executing all script based on pipeline
- config/ : A directory to store config file to execute python script
- creds/ : A directory to store credentials file for postgres database connection
- data/ : A directory to store folder containing csv files (only for csv_to_pg)
- ddl/: A directory to store DDL query to define the destination table (only for csv_to_pg)
- sql/: A directory to store SQL query for transformation in postgres (only for pg_to_pg)

3. To ingest data from csv to postgres incrementally, you need to run csv_to_pg.py. Before running the script, you need to set up these following things:
- Set up schemas in postgres to store your table (in this example you need to create schema named 'di', you can modify later through config file and DDL query)
- Put your postgres credentials json into /creds directory, you can also modify the existing json file (postgres.json)
- Put your csv files into csv folder in /data directory, each csv folder for one table destination
Notes: csv files need to have consistent format (ex: prefix_%Y%M%d), so it can be ordered from old to latest file, otherwise the program will confuse to distinguish the latest file
- Put your DDL sql file into ddl/ directory, make sure: 
   - the query has the same data format with csv (except ingesttime & ingest_from)
   - the query is correspond to destination table definition (table_name and schema_name)
   - the query has ingesttime & ingest_from column
   - you can modify the existing file for the table schema and table name if you have different destination table
   - Set up your config file in config/ directory, your config need to have this structure (example: employees.json):
         {
         "csv_folder": "employees", -- replace values with you csv folder name
         "credentials_file": "postgres.json", -- replace values with your creds file name
         "table_name": "employees", -- replace values with your destination table name
         "schema_name": "di", -- replace values with your destination table schema name
         "ddl": "employees.sql" -- replace values with your DDL sql file name
         }
   - For running the csv_to_pg.py script, you can use this bash command (example: employees & timesheets):
      `python csv_to_pg.py --config employees.json` 
      `python csv_to_pg.py --config timesheets.json`
      you can replace --config parameter with your config file name and make sure your config have the correct structure

4. After all csv has been loaded to postgres, you can do some transformation with sql in postgres, you need to run pg_to_pg.py to trigger the sql script. Before running the script, you need to set up these following things:
   - Set up schemas in postgres to store your table (in this example you need to create schema named 'dm', you can modify later through config file)
   - Put your postgres credentials json into /creds directory, you can also modify the existing json file (postgres.json)
   - Put your sql transformation script into /sql directory, Note that it is only support DML query. Therefore, you dont have to define the destination table because you only need to define it in config file and the structure of table rely on the DML query itself
   - Set up your config file in config/ directory, your config need to have this structure (example: salary_per_hour.json):
      {
      "credentials_file": "postgres.json", -- replace values with your creds file name
      "table_name": "salary_per_hour", -- replace values with your destination table name
      "schema_name": "dm", -- replace values with your destination table schema name
      "query_file": "salary_per_hour.sql", -- replace values with your query file name
      "method": "overwrite" -- replace values with your ingest method (append / overwrite)
      }
   - For running the pg_to_pg.py script, you can use this bash command (example: salary_per_hour):
      `python pg_to_pg.py --config salary_per_hour.json` 
      you can replace --config parameter with your config file name and make sure your config have the correct structure
   - Warning: temporary table with name `{table_name}_temp` will be use to stage data, so make sure you dont have important data in that table because it will be remove after process

5. You can also run the pipeline.sh to execute each bash command or you can pass the bash command to your orchestration or scheduler

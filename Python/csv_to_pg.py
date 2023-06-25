import os
import pandas as pd
from sqlalchemy import create_engine
import json
import logging
import argparse

def open_json_file(file_path):
    """
    Opens and reads a JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data.
    """
    with open(file_path, 'r') as json_file:
        json_data = json.load(json_file)
        return json_data

def check_table_if_exists(ddl_path, engine):
    """
    Check existence of table and automatically create the table if not exists based on ddl query.

    Args:
        ddl path (str): The path to the ddl query file.
        engine: The PostgreSQL connection engine

    Returns:
        str: The existence status of table.
    """    
    
    # Get a database connection
    conn = engine.connect()
    
    # Read the SQL query from the file
    try:
        with open(ddl_path, 'r') as file:
            query = file.read()
    except:
        logging.error("Ddl file is not exists")
        exit()
      
    try:
        # Execute the query
        conn.execute(query)
        
        # Close the connection
        conn.close()
        
        return 'Table Not Exists'
    except:
        return 'Table Exists'

def check_csv_files_to_ingest(csv_folder_path, table_name, table_schema, engine):
    """
    Check csv files from folder path that need to be ingest to destination table based on last ingest file.

    Warnings: table need to have from_ingest column to check last ingest file

    Args:
        csv_folder_path (str): The path to csv folder containing csv files
        table_name (str): The destination table name
        table_schema (str): The destination schema name
        engine: The PostgreSQL connection engine.

    Returns:
        list: The list of file path to ingest to destination table.
    """
    # Check last ingest file
    try:
        last_ingest_file  = pd.read_sql("""SELECT max(ingest_from) FROM {}.{}""".format(table_schema, table_name), engine).values[0][0]
    except:
        logging.error("Table do not has ingest_from column")
        exit()
    
    # List all files from csv folder
    try:
        all_files = os.listdir(csv_folder_path)
    except:
        logging.error("Csv folder is not exists")
        exit()
    
    # Return all files if last_ingest_file is None
    if last_ingest_file == None:
        return [os.path.join(csv_folder_path, i) for i in all_files] 
    
    # List all files that need to be ingest to table
    files_to_ingest = [i for i in all_files if i > last_ingest_file]
    file_path_to_ingest = [os.path.join(csv_folder_path, i) for i in files_to_ingest]
    
    return file_path_to_ingest

def ingest_csv_files_to_postgres(files, table_name, table_schema, engine):
    """
    Ingest all csv files from list containing all csv file path to destination table by append.

    Args:
        files (list): The list containing all csv file path
        table_name (str): The destination table name
        table_schema (str): The destination schema name
        engine: The PostgreSQL connection engine.

    Returns:
        None.
    """
    output = []
    
    # Reading csv files into dataframe and combine them
    for i in files:
        df = pd.read_csv(i)
        df['ingesttime'] = pd.Timestamp.now()
        df['ingest_from'] = os.path.basename(i)
        output.append(df)
    
    # Ingest dataframe if it contains some data 
    if len(output) > 0:   
        final_table = pd.concat(output)  
        final_table.to_sql(destination_table_name, engine, destination_table_schema, 'append', False)
        logging.info("Data has been ingest successfully")
    else:
        logging.info("No data to ingest")   

if __name__ == "__main__":
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument('--config', dest='config', type=str, help='config json file')

    # Parse the command-line arguments
    args = parser.parse_args()

    # Access the values of the arguments
    config_file = args.config

    # Read config
    try:
        config = open_json_file(os.path.join(os.getcwd(), 'config', config_file))
    except:
        logging.error("Config file is not exists")
        exit()

    # Configuration parameters
    csv_folder_path = os.path.join(os.getcwd(), 'data', config['csv_folder'])
    credentials_file_path = os.path.join(os.getcwd(), 'creds', config['credentials_file'])
    destination_table_name = config['table_name']
    destination_table_schema = config['schema_name']
    ddl_path = os.path.join(os.getcwd(), 'ddl', config['ddl'])
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Set up connection to database
    try:
        credentials = open_json_file(credentials_file_path)
    except:
        logging.error("Creds file is not exists")
        exit()
        
    engine = create_engine('postgresql+psycopg2://%(user)s:%(password)s@%(host)s/%(database)s' % credentials)
    logging.info("Connected successfully to database")

    # Checking table if exists
    if check_table_if_exists(ddl_path, engine) == 'Table Not Exists':
        logging.info("Destination table is not exists, table is successfully created")
        
        # Ingest all files to table
        files = [os.path.join(csv_folder_path, i) for i in os.listdir(csv_folder_path)]
        ingest_csv_files_to_postgres(files, destination_table_name, destination_table_schema, engine)

    elif check_table_if_exists(ddl_path, engine) == 'Table Exists':
        logging.info("Destination table is already exists, checking data to ingest")

        # Checking and ingest data to table
        files = check_csv_files_to_ingest(csv_folder_path, destination_table_name, destination_table_schema, engine)
        ingest_csv_files_to_postgres(files, destination_table_name, destination_table_schema, engine)

from sqlalchemy import create_engine
from csv_to_pg import open_json_file
import logging
import os
import argparse

def transform_data(query_path, engine, table_name, schema_name, method):
    """
    Transform data from table in database and loaded back to destination table in database.
    
    Warning: temporary table with name `{table_name}_temp` will be use to stage data, so make sure you dont have important data in that table because it will be remove after process

    Args:
        query_path (str): The path to query file
        engine: The PostgreSQL connection engine
        table_name (str): The destination table name
        table_schema (str): The destination schema name
        method (str): Method to ingest data to destination table (append / overwrite).

    Returns:
        None.
    """
    # Get a database connection
    conn = engine.connect()
    
    # Read the SQL query from the file
    try:
        with open(query_path, 'r') as file:
            query = file.read()
    except:
        logging.error("Query file is not exists")
        exit()
    
    if method == 'overwrite':
        # Execute the query
        conn.execute("""DROP TABLE IF EXISTS {}.{}_temp""".format(schema_name, table_name))
        
        try:
            conn.execute("""CREATE TABLE {}.{}_temp AS ({})""".format(schema_name, table_name, query))
            logging.info("Query successfully executed")
        except:
            logging.error("Query transformation error")
            conn.close()
            exit()
        
        conn.execute("""DROP TABLE IF EXISTS {}.{}""".format(schema_name, table_name))
        
        conn.execute("""ALTER TABLE {}.{}_temp RENAME TO {}""".format(schema_name, table_name, table_name))
        
        logging.info("Destination table has been replaced with new data")
    elif method == 'append':
        # Execute the query
        try:
            conn.execute("""INSERT INTO {}.{} {}""".format(schema_name, table_name, query))
            logging.info("Query successfully executed and loaded into destination table")
        except:
            logging.error("Query transformation error or destination table is not exists")
            conn.close()
            exit()  
    else:
        logging.error("Method is not supported (only append or overwrite)")
    
    # Close the connection
    conn.close()    

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
    credentials_file_path = os.path.join(os.getcwd(), 'creds', config['credentials_file'])
    destination_table_name = config['table_name']
    destination_table_schema = config['schema_name']
    query_path = os.path.join(os.getcwd(), 'sql', config['query_file'])
    method = config['method']
    
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
    
    # Data transformation
    transform_data(query_path, engine, destination_table_name, destination_table_schema, method)


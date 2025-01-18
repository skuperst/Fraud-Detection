import logging
import os
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

# Initiate logging
logging.basicConfig(level=logging.INFO)

def create_database_connection():
    try:
        # Load environment variables from the .env file
        load_dotenv()

        # Access the variables
        logging.info("Loading credentials ...")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASS = os.getenv('DB_PASS')
        INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME')

        # Initialize Cloud SQL Connector
        logging.info("Initializing Cloud SQL connector...")  
        connector = Connector()
        
        # Create a connection to the database using pg8000 driver
        logging.info("Connecting to the database...")
        connection = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
        )
        
        # Manually create and manage the cursor lifecycle
        cursor = connection.cursor()
        
        # Log successful connection
        logging.info("Database connection established successfully.")

        return cursor, connection

    except Exception as e:
        # Log any errors encountered during the connection process
        logging.error(f"Failed to create database connection: {e}", exc_info=True)
        raise

# Function that processes the data produced by DAG 1 and inserts it into PostgreSQL
def insert_data_into_postgresql(ti):
    api_request_dict = ti.xcom_pull(task_ids='make_api_request')  # Pull data from DAG 1 (task 'make_api_request')

    if not api_request_dict:
        logging.info("No data returned from API, skipping insertion.")
        return

    try:
        table_name = 'transactions_table'
        # Establish connection to the table
        cursor, connection = create_database_connection()

        # Extract column names and values
        columns = ' ,'.join([col+'_name' if col in ['first', 'last'] else col for col in api_request_dict.keys()])
        placeholders = ', '.join(['%s'] * len(api_request_dict))
        # Create the SQL query
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        values = tuple(api_request_dict.values())

        # Execute the query
        cursor.execute(query, values)
        
        # Commit the transaction after the query is inserted
        connection.commit()
        logging.info(f"Data inserted (CCN: {api_request_dict['cc_num']}) into {table_name}.")

    except Exception as e:
        logging.error(f"Error while inserting data into PostgreSQL: {str(e)}")
    
    finally:
        # Check if cursor is not None before closing
        if cursor:
            cursor.close()
        # Check if connection is not None before closing
        if connection:
            connection.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),  # Starting date of the DAG
}

# Define the DAG
with DAG(
    'insert_data_in_table_dag',  # The unique ID for the DAG
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered manually or after DAG 1 finishes
    end_date=datetime(2025, 2, 1),  # End date (stop running after Feb 1, 2025)
) as dag2:

    # Sensor: Wait for DAG 1 (task 'make_api_request') to finish before running DAG 2
    wait_for_dag1 = ExternalTaskSensor(
        task_id='wait_for_dag1',  # Task ID for the sensor
        external_dag_id='make_api_request_dict_dag',  # The DAG to wait for
        external_task_id='make_api_request',  # The task within DAG 1 to wait for
        timeout=600,  # Maximum wait time (600 seconds = 10 minutes)
        poke_interval=60,  # Time interval to wait between checks (60 seconds)
    )

    # Task: Insert data into PostgreSQL if available
    insert_task = PythonOperator(
        task_id='insert_data_into_postgresql',  # Task ID
        python_callable=insert_data_into_postgresql  # Function to call
    )

    # Set task dependencies: First, wait for DAG 1 to finish, then insert data into PostgreSQL
    wait_for_dag1 >> insert_task

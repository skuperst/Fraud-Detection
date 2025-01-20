import logging
import os
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
import requests
import pandas as pd
import pickle
import xgboost
import json
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Initiate logging
logging.basicConfig(level=logging.INFO)

# Produce the model prediction for one API request (in a  dict form)
def predict_fraud(api_request_dict):

    try:

        # Convert the json input into a dataframe
        df = pd.DataFrame([api_request_dict])

        # Remove 'trans_num' from the columns as it's not used in the model
        df.drop(columns=['trans_num'], inplace=True)

        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, 'modelling/category_dict.pkl')
        # Load the categorical dictionary
        with open(file_path, 'rb') as file:
            category_dict = pickle.load(file)

        # Convert object columns to categorical columns
        for col, categories in category_dict.items():
            if col in df.columns:
                # Convert the column in df to categorical using the categories from category_dict
                df[col] = pd.Categorical(df[col], categories=categories)

        # Load the model
        xgboost_model = xgboost.Booster()
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, 'modelling/xgboost_model.json')
        xgboost_model.load_model(file_path)
    
        prediction = int(xgboost_model.predict(xgboost.DMatrix(df.drop('is_fraud', axis=1), enable_categorical=True))[0])

        logging.info("Model prediction was successfully produced.")

    except Exception as e:
        logging.error(f"Model prediction issues: {e}.", exc_info=True)
        raise

    return prediction

def make_api_request():
    try:
        # Define the URL and headers
        url = 'https://real-time-payments-api.herokuapp.com/current-transactions'
        headers = {'accept': 'application/json'}

        # Send the GET request
        response = requests.get(url, headers=headers)

        # Check the response status
        if response.status_code == 200:
            response_text_dict = json.loads(response.json())
            api_request_dict = dict(zip(response_text_dict['columns'], response_text_dict['data'][0]))
            logging.info("Request received via API.")
        else:
            logging.error("API request failed.")

        # Impose excatly the same columns (order included) as in the large dataset
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, 'modelling/expected_columns.json')
        with open(file_path, 'rb') as file:
            expected_columns = json.load(file)
        api_request_dict = {column: api_request_dict[column] for column in expected_columns}

        # Added the predicted value
        api_request_dict['is_fraud_predicted'] = predict_fraud(api_request_dict)

        logging.info("Prepared a new database entry from the API request.")

        return api_request_dict

    except Exception as e:
        # Log any errors encountered during the prediction               
        logging.error(f"Error during API request: {str(e)}")
        return None  # Return None if an exception occurs


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
def insert_data_into_postgresql():

    api_request_dict = make_api_request()

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
    'start_date': datetime(2025, 1, 17),  # Starting date of the DAG
}


# Define the DAG
with DAG(
    dag_id='single_task_dag',  # Unique ID for the DAG
    default_args=default_args,
    schedule_interval='* * * * *',  # Schedule to run every minute
    catchup=False,  # Disable backfilling
) as dag:

    # Define the single task
    single_task = PythonOperator(
        task_id='insert_api_data_into_gcloud_postgresql',  # Unique ID for the task
        python_callable=insert_data_into_postgresql,  # Function to call
    )















# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 17),  # Starting date of the DAG
}

# Define the DAG
with DAG(
    'make_api_request_dict_dag',  # The unique ID for the DAG
    default_args=default_args,
    schedule_interval='* * * * *',  # Run every minute
    end_date=datetime(2026, 1, 22),  # End date (stop running after Jan 15, 2025)
) as dag1:

    # Task: Make an API request and produce results
    t1 = PythonOperator(
        task_id='make_api_request',  # Task ID
        python_callable=make_api_request,  # Function to call
    )

    # 't1' is the only task in this DAG, which simulates the API request and processes the result.
    t1
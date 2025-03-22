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
from datetime import datetime, timedelta

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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

####
def get_api_request(**kwargs):
    try:
        # Define the URL and headers
        #url = 'https://real-time-payments-api.herokuapp.com/current-transactions'
        url = 'http://45.80.24.139:11113/current-transactions'
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

        # Push the api_request_dict to XCom
        kwargs['ti'].xcom_push(key='api_request_dict', value=api_request_dict)

        return
    
    except Exception as e:
        # Log any errors encountered during the prediction               
        logging.error(f"Error during API request: {str(e)}")
        return None  # Return None if an exception occurs

def add_prediction(**kwargs):
    
    # Pull the 'api_request_dict' from the XCom of the previous task ('get_api_request')
    api_request_dict = kwargs['ti'].xcom_pull(task_ids='get_new_api_request', key='api_request_dict')

    if not api_request_dict:
        logging.info("No data returned from API, skipping prediction.")
        return
    
    try:
        # Impose exactly the same columns (order included) as in the large dataset
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, 'modelling/expected_columns.json')
        with open(file_path, 'rb') as file:
            expected_columns = json.load(file)
        api_request_dict = {column: api_request_dict[column] for column in expected_columns}

        # Add the predicted value
        api_request_dict['is_fraud_predicted'] = predict_fraud(api_request_dict)

        logging.info("Prepared a new database entry from the API request.")

        # Push the api_request_dict to XCom
        kwargs['ti'].xcom_push(key='api_request_dict', value=api_request_dict)

        return 

    except Exception as e:
        # Log any errors encountered during the prediction               
        logging.error(f"Error during ML prediction: {str(e)}")
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


def insert_data_into_postgresql(**kwargs):
        
    # Pull the 'api_request_dict' from the XCom of the previous task ('get_api_request')
    api_request_dict = kwargs['ti'].xcom_pull(task_ids='add_prediction_to_data', key='api_request_dict')

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

    return


def send_alert_email(**kwargs):
    
    # Pull the 'api_request_dict' from the XCom of the previous task ('get_api_request')
    api_request_dict = kwargs['ti'].xcom_pull(task_ids='add_prediction_to_data', key='api_request_dict')

    if not api_request_dict:
        logging.info("No data returned from API, skipping alert.")
        return
    
    if api_request_dict['is_fraud_predicted']:

        logging.info("Fraud alert! Email will be sent.")

        load_dotenv()
        # Access the variables
        sender_email = os.getenv('SENDER_EMAIL')
        receiver_email = os.getenv('RECEIVER_EMAIL')
        app_password = os.getenv('APP_PASSWORD')

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = "Fraud Alert"

        body = "Wake up! Fraud alert for credit card {} and transaction number {}.".format(api_request_dict['cc_num'], api_request_dict['trans_num'])
        msg.attach(MIMEText(body, "plain"))

        try:
            # Connection to Gmail's SMTP server
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()  # Secure the connection
            server.login(sender_email, app_password)  # Authentification

            # Send the mail
            text = msg.as_string()
            server.sendmail(sender_email, receiver_email, text)

            print("E-mail envoyé avec succès!")
        except Exception as e:
            print(f"Erreur lors de l'envoi : {e}")
        finally:
            server.quit()  # Close connection

    else:

        logging.info("No fraud alert this time.")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 17),
    'retries': 1,  # Reduced retries to 1
    'retry_delay': timedelta(minutes=1),  # Keep delay before retrying
}

# Define the DAG
with DAG(
    dag_id='api_to_postgresql_dag',  # DAG ID
    default_args=default_args,
    schedule_interval='* * * * *',  # Run every minute
    max_active_runs=1,  # Prevent multiple DAG runs at the same time
    catchup=False,  # Don't backfill
    dagrun_timeout=timedelta(minutes=5)  # Increased timeout to 5 minutes
) as dag:

    # Task A: Get an API request and pass it on as a dictionary
    task_a = PythonOperator(
        task_id='get_new_api_request',  # Task ID
        python_callable=get_api_request,  # Function to call
        provide_context=True,  # Allow passing of Airflow context to the function
    )

    # Task B: Add the ML prediction to the API data
    task_b = PythonOperator(
        task_id='add_prediction_to_data',  # Task ID
        python_callable=add_prediction,  # Function to call
        provide_context=True,  # Allow passing of Airflow context to the function
    )

    # Task C: Insert data into PostgreSQL, depends on Task A
    task_c = PythonOperator(
        task_id='insert_data_into_gcloud_postgresql',  # Task ID
        python_callable=insert_data_into_postgresql,  # Function to call
        provide_context=True,  # Allow passing of Airflow context to the function
    )

    # Task D: Send alert if fraud is detected, depends on Task A
    task_d = PythonOperator(
        task_id='send_alert_email_if_fraud',  # Task ID
        python_callable=send_alert_email,  # Function to call
        provide_context=True,  # Allow passing of Airflow context to the function
    )

    # Set task dependencies (Task C and D run after Task A and then B complete)
    task_a >> task_b >> [task_c, task_d]  # Task B waits for Task A to finish before running
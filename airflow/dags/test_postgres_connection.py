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


# Load environment variables from .env file
load_dotenv()

# Function to create database connection
def create_database_connection():
    try:
        # Access the variables
        logging.info("Loading credentials ...")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASS = os.getenv('DB_PASS')
        INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME')

        # Initialize Cloud SQL Connector (this part can be omitted for testing purpose)
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

        # Execute a simple query to test the connection (e.g., select current time)
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        logging.info(f"Query result: {result}")

        # Clean up
        cursor.close()
        connection.close()
        logging.info("Connection closed.")

    except Exception as e:
        # Log any errors encountered during the connection process
        logging.error(f"Failed to create database connection: {e}", exc_info=True)
        raise


# Airflow DAG definition
dag = DAG(
    'test_postgres_connection',
    description='Test custom connection to PostgreSQL database',
    schedule_interval=None,  # Don't run periodically, only trigger manually
    start_date=datetime(2025, 3, 21),
    catchup=False,  # Don't backfill
)

# PythonOperator to run the create_database_connection function
test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=create_database_connection,  # The function to run
    dag=dag,
)

test_connection_task

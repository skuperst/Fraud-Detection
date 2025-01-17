import logging
import os
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
import requests
import pandas as pd
import pickle
import xgboost
import json

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

def predict_fraud(api_request_dict):

    try:

        # Convert the json input into a dataframe
        df = pd.DataFrame([api_request_dict])

        # Load the categorical dictionary
        with open('modelling/category_dict.pkl', 'rb') as file:
            category_dict = pickle.load(file)

        # Convert object columns to categorical columns
        for col, categories in category_dict.items():
            if col in df.columns:
                # Convert the column in df to categorical using the categories from category_dict
                df[col] = pd.Categorical(df[col], categories=categories)

        # Load the model
        xgboost_model = xgboost.Booster()
        xgboost_model.load_model('modelling/xgboost_model.json')
    
        prediction = int(xgboost_model.predict(xgboost.DMatrix(df.drop('is_fraud', axis=1), enable_categorical=True))[0])

    except Exception as e:
        logging.error("Model prediction issues: {e}.", exc_info=True)
        raise

    return prediction

def make_api_request_dict():

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
        with open('modelling/expected_columns.json', 'rb') as file:
            expected_columns = json.load(file)
        api_request_dict = {column: api_request_dict[column] for column in expected_columns}

        # Added the predicted value
        api_request_dict['is_fraud_predicted'] = predict_fraud(api_request_dict)

        logging.info("Prepared a new database entry.")

    except Exception as e:
        # Log any errors encountered during the prediction               
        logging.error("Failed to prepare a new database entry: {e}", exc_info=True)
        raise

    return api_request_dict

def insert_data_in_table():

    table_name = 'transactions_table'

    api_request_dict = make_api_request_dict() 

    try:
        cursor, connection = create_database_connection()

        # Extract column names and values
        columns = ' ,'.join([col+'_name' if col in ['first', 'last'] else col for col in api_request_dict.keys()])
        placeholders = ', '.join(['%s'] * len(api_request_dict))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        values = tuple(api_request_dict.values())
        print(query)

        # Execute the query
        cursor.execute(query, values)
        
        # Commit the transaction after the query is inserted
        connection.commit()
        logging.info(f"Data inserted into {table_name} successfully.")

    except Exception as e:
        logging.error("Error:", e)
    
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    insert_data_in_table()

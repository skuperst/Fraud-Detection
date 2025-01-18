import logging
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
import requests
import pandas as pd
import pickle
import xgboost
import json

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
        with open('modelling/expected_columns.json', 'rb') as file:
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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 17),  # Starting date of the DAG
}

# Define the DAG
with DAG(
    'make_api_request_dict_dag',  # The unique ID for the DAG
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    end_date=datetime(2025, 1, 22),  # End date (stop running after Jan 15, 2025)
) as dag1:

    # Task: Make an API request and produce results
    t1 = PythonOperator(
        task_id='make_api_request',  # Task ID
        python_callable=make_api_request,  # Function to call
    )

    # 't1' is the only task in this DAG, which simulates the API request and processes the result.
    t1
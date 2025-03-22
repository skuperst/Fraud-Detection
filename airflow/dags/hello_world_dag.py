from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Define a simple function to be run by the task
def print_hello():
    logging.info("Hello from Airflow!")

# Define the DAG
dag = DAG(
    'hello_world_dag',  # DAG name
    description='A simple Airflow DAG',
    schedule_interval='* * * * *',  # Run every minute
    start_date=datetime(2025, 3, 21),  # Start date for the DAG
    catchup=False,  # Don't backfill past runs
)

# Define a task that calls the print_hello function
task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Set task dependencies (in this case, just one task)
task

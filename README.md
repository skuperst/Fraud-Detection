# Automatic Fraud Detection

The project comprises two main parts. First, it utilizes credit card transaction data provided by the European Central Bank to develop an XGBoost-based machine learning model for predicting fraudulent payments. Second, it retrieves real-time payment data via an API, enriches it with fraud predictions, and stores the results in a PostgreSQL database hosted on Google Cloud SQL. Additionally, an alert email is triggered whenever a fraud detection occurs. The second part of the process is orchestrated using Airflow.

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)

---

## Overview
This project features a data pipeline built using Apache Airflow to manage the **ETL** (Extract, Transform, Load) processes.  The **Transformation** step relies on an **XGBoost** model developed in the Jupyter notebook `modelling/training.ipynb`. To execute the notebook, you would need to download the complete training dataset provided by the European Central Bank ([link](https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv)). However, this is not mandatory, as the `modelling`  folder already contains the three essential files for generating predictions:

   - The `expected_columns.json` file lists the columns that are shared between the API requests and the full training dataset.

   - The `category_dict.pkl.json` file holds a dictionary that maps categories in the data's object column.

   - The `xgboost.ipynb` file contains the pre-trained XGBoost model, which requires the aforementioned dictionary for proper functionality.

---

## Prerequisites

Ensure you have the following installed:
- **Python 3.8+**
- **pip** (Python package manager)
- **virtualenv** or **venv** (Python virtual environment tools)
- **A PostgreSQL database hosted on Google Cloud SQL**
- **The standard Apache Airflow prerequisites**

## Setup Instructions

1. Clone the repository:

```bash
git clone https://github.com/skuperst/Fraud-Detection.git
cd Fraud-Detection
```

2. Create a Python virtual environment in the newly created folder:
   ```bash
   python3 -m venv venv
   ```

3. Activate the virtual environment:

   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```

4. Install dependencies from `requirements.txt`:

     ```bash
     pip install --no-cache-dir -r requirements.txt
     ```

5. Create a PostgreSQL database on Google Cloud SQL (with ChatGpt's help 😊):

   - Create a Cloud SQL Instance.
     
   - Configure Networking.

   - Create a Database.
     
   - Set Up Users.

   - (Optional) Connect to Your Database with `psql` to test the connection.

6. Verify that your .env file looks like this:

      ```bash
      GOOGLE_APPLICATION_CREDENTIALS = GOOGLE_CREDENTIALS.json
      DB_NAME = <your_database_name>
      DB_USER = <your_user_name>
      DB_PASS = <your_password>
      INSTANCE_CONNECTION_NAME = <your_project_name>:<your_region>:<your_database_name>
      ```
   with `GOOGLE_CREDENTIALS.json` saved in the same folder.

7.  Add the sender and receiver email addresses, along with your Google App Password, to the .env file:

      ```bash
      SENDER_EMAIL = sender@gmail.com
      RECEIVER_EMAIL = receiver@gmail.com
      APP_PASSWORD = abcd efgh ijkl mnop
      ```

8. Uncomment the `create_new_table(<your_table>)` line in `gcloud_test.py` and run `python gcloud_test.py`  to create the database table you will use for your uploads (if it doesn't already exist). Afterward, verify that the table has been created by running the script again with the `create_new_table(<your_table>)` line still commented out.

9. Create an Airflow user with the following command:

   ```bash
      airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com

   ```

10. Start the Airflow webserver in one terminal:

   ```bash
   airflow webserver --port 8080
   ```

11. In another terminal, start the Airflow scheduler:

   ```bash
   airflow scheduler
   ```

12. Open a web browser and navigate to `http://localhost:8080`. In **Admin-> Connections**:

   - Connection ID: Choose a name for the connection.
   - Connection Type: Select **Postgres**.
   - Host: Enter the Cloud SQL instance's Public or Private IP (depending on your configuration).
   - Database: Enter your database name, `<your_database_name> `.
   - Login: Enter your user name, `<your_user_name> `.
   - Password: Enter your password, `<your_password> `.
   - Port: Default is 5432.

13. Run the DAG called **api_to_postgresql_dag** defined in  **main_dag.py**.

14. (Optional) Connect **Tableau** to the cloud database to see the results.
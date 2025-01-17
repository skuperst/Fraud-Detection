import os
from google.cloud.sql.connector import Connector
import pg8000.native  # PostgreSQL driver
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Access the variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME')

def create_database_connection():

    # Initialize Cloud SQL Connector
    connector = Connector()
    # Create a connection to the database using pg8000 driver
    connection = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )
    # Manually create and manage the cursor lifecycle
    cursor = connection.cursor()
    
    return cursor, connection

def test_database_connection():
    try:
        cursor, connection = create_database_connection()
        try:
            # Create a test table if it doesn't exist
            cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, data TEXT);")
            
            # Insert a test row into the table
            cursor.execute("INSERT INTO test_table (data) VALUES (%s);", ("Test data",))
            connection.commit()

            # Fetch and print all rows from the table
            cursor.execute("SELECT * FROM test_table;")
            results = cursor.fetchall()
            print("Data in table:", results)
        finally:
            cursor.close()  # Close the cursor manually

    except Exception as e:
        print("Error:", e)

def list_tables_in_database():
    try:
        cursor, connection = create_database_connection()
        try:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = cursor.fetchall()
            print("Tables in the database:", tables)
        finally:
            cursor.close()
    except Exception as e:
        print("Error:", e)

def verify_data_in_test_table():
    try:
        cursor, connection = create_database_connection()
        try:
            cursor.execute("SELECT * FROM test_table;")
            results = cursor.fetchall()
            print("Data in test_table:", results)
        finally:
            cursor.close()
    except Exception as e:
        print("Error:", e)

def insert_more_data():
    try:
        cursor, connection = create_database_connection()
        try:
            cursor.execute("INSERT INTO test_table (data) VALUES (%s);", ("Another test data",))
            connection.commit()
            print("Data inserted successfully!")
        finally:
            cursor.close()
    except Exception as e:
        print("Error:", e)

def create_new_table():
    try:        
        cursor, connection = create_database_connection()
        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS another_table (
                id SERIAL PRIMARY KEY,
                description TEXT
            );
            """)
            connection.commit()
            print("New table created successfully!")
        finally:
            cursor.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
#    test_database_connection()
#    list_tables_in_database()
    verify_data_in_test_table()
#    insert_more_data()
#    create_new_table()
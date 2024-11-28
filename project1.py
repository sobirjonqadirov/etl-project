import logging
import os
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import time

logging.basicConfig(
    filename="etl_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# Retrieve database credentials from .env (change .env with yours)
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Folder path containing CSV files
csv_folder = 'csv_files'

def process_csv(file_path, table_name, cursor):
    """Process a CSV file and load it into a MySQL table."""
    try:
        file_size = os.path.getsize(file_path) / (1024)  # File size in KB
        logging.info(f"Started processing file {file_path} ({file_size:.2f} KB).")

        data = pd.read_csv(file_path)
        logging.info(f"Successfully loaded data from {file_path}. Rows: {len(data)}.")
    except Exception as e:
        logging.error(f"Error loading CSV file {file_path}: {e}")
        return

    columns = ", ".join([f"`{col}` VARCHAR(255)" for col in data.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {columns}
    );
    """
    try:
        cursor.execute(create_table_query)
        logging.info(f"Table '{table_name}' created or already exists.")
    except mysql.connector.Error as err:
        logging.error(f"Error creating table {table_name}: {err}")
        return

    try:
        cursor.execute(f"TRUNCATE TABLE `{table_name}`;")
        logging.info(f"Table '{table_name}' truncated.")
    except mysql.connector.Error as err:
        logging.error(f"Error truncating table {table_name}: {err}")
        return

    try:
        total_inserted = 0
        for _, row in data.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            insert_query = f"""
            INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in data.columns])})
            VALUES ({placeholders});
            """
            cursor.execute(insert_query, tuple(row))
            total_inserted += 1
        logging.info(f"Data from {file_path} successfully written to table '{table_name}'. Total rows inserted: {total_inserted}.")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting data into table {table_name}: {err}")
        cursor.connection.rollback()


def process_files(csv_folder, cursor):
    """Process all files in the specified folder."""
    for file in os.listdir(csv_folder):
        if file.endswith('.csv'):  # Change this line to include other file types
            file_path = os.path.join(csv_folder, file)
            table_name = os.path.splitext(file)[0]
            logging.info(f"Processing file: {file} into table: {table_name}")
            process_csv(file_path, table_name, cursor)  # Replace this function if you want to work with other data types


def connect_to_database():
    """Connect to MySQL database with retry logic."""
    attempt = 0
    while attempt < 3:
        try:
            con = mysql.connector.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            logging.info("Successfully connected to the database!")
            return con
        except mysql.connector.Error as err:
            attempt += 1
            logging.error(f"Database connection attempt {attempt} failed: {err}")
            if attempt == 3:
                logging.critical(f"Failed to connect to database after {attempt} attempts. Aborting ETL process.")
            time.sleep(5)  # Wait for 5 seconds before retrying
    return None


try:
    logging.info("ETL process started.")

    # Connect to database
    con = connect_to_database()
    if con is None:
        logging.critical("Failed to connect to the database. Aborting ETL process.")
        exit(1)

    cursor = con.cursor()

    # Process files from the folder
    process_files(csv_folder, cursor)

except mysql.connector.Error as err:
    logging.critical(f"Database Error: {err}")
except Exception as e:
    logging.critical(f"General Error: {e}")
finally:
    if con.is_connected():
        con.close()
        logging.info("Database connection closed.")
    logging.info("ETL process completed.")

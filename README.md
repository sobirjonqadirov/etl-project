# ETL Process for POS Data

This project provides an ETL (Extract, Transform, Load) process for handling CSV files generated daily by a POS (Point of Sale) system. The ETL process loads data from CSV files into a MySQL database, handling errors and logging all relevant activities for auditing purposes.

## Features

- Loads daily CSV files into a MySQL database.
- Handles different CSV files dynamically (i.e., files with changing names).
- Creates tables in the database if they don't exist.
- Truncates existing tables before inserting new data.
- Robust error handling with detailed logging.
- Logging of key activities for audit and debugging purposes.

## Requirements

To run this project, you'll need the following Python packages:
- `mysql-connector-python`: MySQL connector for Python.
- `pandas`: Data manipulation and CSV file reading.
- `python-dotenv`: Load environment variables from `.env` file.
- `logging`: Standard Python library (logging module for tracking process).

Install the necessary dependencies by running:
```bash
pip install -r requirements.txt
```

## Setup

1. **Clone this repository**:
```bash
git clone https://github.com/sobirjonqadirov/etl-project.git
```

2. **Create a `.env` file** to store your database credentials and other sensitive information. Example:

```printenv
DB_HOST=localhost 
DB_PORT=3306 
DB_NAME=your_db_name 
DB_USER=your_db_user 
DB_PASSWORD=your_db_password
```

Make sure to replace `your_db_name`, `your_db_user`, and `your_db_password` with your actual database credentials.

3. **Prepare your CSV files**: Place your CSV files in the `csv_files` folder.

4. **Run the script**:

Execute the script using Python:

```bash
python project1.py
```

The script will process all `.csv` files in the `csv_files` folder and load the data into the corresponding tables in your MySQL database.

NOTE: Setup windows task scheduler manually to periodically run this script

## Error Handling

If an error occurs during the process (e.g., while connecting to the database or inserting data), detailed error logs are generated in the `etl_log.log` file.

## Logging

All important steps, such as data loading, table creation, truncation, and data insertion, are logged for tracking purposes. The log file will be created in the same directory as the script.

## Future Enhancements

- **Support for additional file types**: Currently, the script processes `.csv` files, but you can easily modify the code to support other formats such as Excel (`.xlsx`).

## License

This project is open-source and available under the MIT License. See the LICENSE file for more details.
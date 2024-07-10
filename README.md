# Crypto Data Loader

This project fetches cryptocurrency data from the CoinGecko API and stores it in a PostgreSQL database. The script performs the following tasks:

1. Fetches cryptocurrency data from the CoinGecko API.
2. Connects to a PostgreSQL database.
3. Creates a table to store the cryptocurrency data if it doesn't already exist.
4. Drops the existing table (if any).
5. Inserts the fetched data into the table.

## Prerequisites

- Python 3.6 or higher
- PostgreSQL
- `requests` library
- `pandas` library
- `psycopg2` library
- `python-dotenv` library

## Installation

##
1. Clone the repository:
   ```sh
   git clone https://github.com/sachanta99/crypto-data-loader.git
   cd crypto-data-loader

##
2. Create a virtual environment and activate it:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`

##
3. Install the required packages:
   ```sh 
   pip install -r requirements.txt

##
4. Create a .env file in the project directory with the following content:

   ```sh
   DB_NAME=your_db_name
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_HOST=your_db_host
   DB_PORT=your_db_port

##
5. Usage

    Run the script:
    ```sh
    python load_data_using_api.py
    
##
Code Explanation
fetch_crypto_data Function

Fetches cryptocurrency data from the CoinGecko API and returns it as a DataFrame.
connect_to_db Function

Connects to the PostgreSQL database using credentials from environment variables and returns the connection object.
create_table Function

Creates the crypto_prices table in the database if it doesn't already exist.
drop_table Function

Drops the crypto_prices table from the database.
insert_data Function

Inserts data into the crypto_prices table.
main Function

Main function to fetch cryptocurrency data, connect to the database, drop the existing table, create a new table, insert data, and close the connection.

##
Running Tests
Tests are written using pytest. To run the tests:

Install pytest:
```sh
pip install pytest


##
Run the tests:
```sh
pytest test_load_data_using_api.py

##
License
This project is licensed under the MIT License.

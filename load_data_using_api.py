import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from typing import Optional
from pandas import DataFrame

# Load environment variables from .env file
load_dotenv()


def fetch_crypto_data() -> DataFrame:
    """
    Fetch cryptocurrency data from the CoinGecko API and return it as a DataFrame.

    Returns:
        DataFrame: A DataFrame containing the cryptocurrency data.
    """
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"
        response = requests.get(url)
        response.raise_for_status()  # Check for request errors
        data = response.json()
        df = pd.json_normalize(data)
        return df[['id', 'symbol', 'name', 'current_price']]
    except requests.RequestException as e:
        print(f"Error fetching crypto data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def connect_to_db() -> Optional[psycopg2.extensions.connection]:
    """
    Connect to the PostgreSQL database using credentials from environment variables.

    Returns:
        Optional[psycopg2.extensions.connection]: A connection object or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Error connecting to the database: {e}")
        return None


def create_table(conn: psycopg2.extensions.connection) -> None:
    """
    Create the crypto_prices table in the database if it doesn't already exist.

    Args:
        conn (psycopg2.extensions.connection): The database connection.
    """
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id VARCHAR(30) PRIMARY KEY,
            symbol VARCHAR(30),
            name VARCHAR(50),
            current_price FLOAT
        );
        """
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
    except psycopg2.DatabaseError as e:
        print(f"Error creating table: {e}")


def drop_table(conn: psycopg2.extensions.connection) -> None:
    """
    Drop the crypto_prices table from the database.

    Args:
        conn (psycopg2.extensions.connection): The database connection.
    """
    try:
        drop_query = "DROP TABLE IF EXISTS crypto_prices;"
        cur = conn.cursor()
        cur.execute(drop_query)
        conn.commit()
        cur.close()
    except psycopg2.DatabaseError as e:
        print(f"Error dropping table: {e}")


def insert_data(conn: psycopg2.extensions.connection, data: DataFrame) -> None:
    """
    Insert data into the crypto_prices table.

    Args:
        conn (psycopg2.extensions.connection): The database connection.
        data (DataFrame): The data to be inserted into the table.
    """
    if data.empty:
        print("No data to insert.")
        return

    try:
        insert_query = """
        INSERT INTO crypto_prices (id, symbol, name, current_price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        cur = conn.cursor()
        for index, row in data.iterrows():
            cur.execute(insert_query, (row['id'], row['symbol'], row['name'], row['current_price']))
        conn.commit()
        cur.close()
    except psycopg2.DatabaseError as e:
        print(f"Error inserting data: {e}")


def main() -> None:
    """
    Main function to fetch cryptocurrency data, connect to the database,
    drop the existing table, create a new table, insert data, and close the connection.
    """
    data = fetch_crypto_data()
    conn = connect_to_db()
    if conn:
        drop_table(conn)
        create_table(conn)
        insert_data(conn, data)
        conn.close()


if __name__ == "__main__":
    main()

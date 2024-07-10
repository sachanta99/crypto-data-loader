import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
import json
def fetch_crypto_data():

    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"

    #querystring = {"symbol": ticker, "region": "US"}

    #headers = {
    #    "X-RapidAPI-Host": "yh-finance.p.rapidapi.com",
    #    "X-RapidAPI-Key": "your_rapidapi_key"
    #}

    #response = requests.request("GET", url, headers=headers, params=querystring)

    response = requests.get(url)
    response = response.json()
    #json_str = json.dumps(response, indent=4)
    #print(json_str)

    # convert to pandas dataframe
    df = pd.json_normalize(response)

    return df[['id', 'symbol', 'name', 'current_price']]

def connect_to_db():
    conn = psycopg2.connect(
        dbname="classicmodels",
        user="nl2sql",
        password="Lehnex!@345",
        host="localhost",
        port="5432"
    )
    return conn

def create_table(conn):
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

def drop_table(conn):
    drop_query = """
    DROP TABLE crypto_prices;
    """
    cur = conn.cursor()
    cur.execute(drop_query);

def insert_data(conn, data):
    insert_query = """
    INSERT INTO crypto_prices (id, symbol, name, current_price)
    VALUES (%s, %s, %s, %s);
    """
    cur = conn.cursor()
    for index, row in data.iterrows():
        cur.execute(insert_query, (row['id'], row['symbol'], row['name'], row['current_price']))
    conn.commit()
    cur.close()

def main():
    data = fetch_crypto_data()
    conn = connect_to_db()
    drop_table(conn)
    create_table(conn)
    insert_data(conn, data)
    conn.close()

if __name__ == "__main__":
    main()

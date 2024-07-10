import pytest
import pandas as pd
from load_data_using_api import fetch_crypto_data, connect_to_db, create_table, drop_table, insert_data


@pytest.fixture
def sample_data():
    """
    Fixture to provide sample data for testing.

    Returns:
        DataFrame: A sample DataFrame with cryptocurrency data.
    """
    return pd.DataFrame({
        'id': ['bitcoin', 'ethereum'],
        'symbol': ['btc', 'eth'],
        'name': ['Bitcoin', 'Ethereum'],
        'current_price': [40000.0, 2500.0]
    })


def test_fetch_crypto_data():
    """
    Test the fetch_crypto_data function to ensure it returns a DataFrame with the correct structure and data.
    """
    data = fetch_crypto_data()
    assert isinstance(data, pd.DataFrame)  # Check if the returned data is a DataFrame
    assert not data.empty  # Check if the DataFrame is not empty
    assert 'id' in data.columns  # Check if 'id' column is present
    assert 'symbol' in data.columns  # Check if 'symbol' column is present
    assert 'name' in data.columns  # Check if 'name' column is present
    assert 'current_price' in data.columns  # Check if 'current_price' column is present


def test_connect_to_db():
    """
    Test the connect_to_db function to ensure it establishes a database connection.
    """
    conn = connect_to_db()
    assert conn is not None  # Check if the connection is successfully established
    conn.close()  # Close the connection after the test


def test_create_table():
    """
    Test the create_table function to ensure it creates the crypto_prices table in the database.
    """
    conn = connect_to_db()
    if conn:
        create_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('nl2sql.crypto_prices');")  # Check if the table exists
        result = cur.fetchone()
        assert result[0] == 'crypto_prices'  # Verify the table name
        cur.close()
        conn.close()  # Close the connection after the test


def test_drop_table():
    """
    Test the drop_table function to ensure it drops the crypto_prices table from the database.
    """
    conn = connect_to_db()
    if conn:
        drop_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('nl2sql.crypto_prices');")  # Check if the table exists
        result = cur.fetchone()
        assert result[0] is None  # Verify the table has been dropped
        cur.close()
        conn.close()  # Close the connection after the test


def test_insert_data(sample_data):
    """
    Test the insert_data function to ensure it inserts data into the crypto_prices table.

    Args:
        sample_data (DataFrame): Sample data to be inserted.
    """
    conn = connect_to_db()
    if conn:
        create_table(conn)
        insert_data(conn, sample_data)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM nl2sql.crypto_prices;")  # Count the number of rows in the table
        result = cur.fetchone()
        assert result[0] == len(sample_data)  # Verify the number of rows matches the sample data length
        cur.close()
        conn.close()  # Close the connection after the test

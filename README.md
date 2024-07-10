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

1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/crypto-data-loader.git
   cd crypto-data-loader


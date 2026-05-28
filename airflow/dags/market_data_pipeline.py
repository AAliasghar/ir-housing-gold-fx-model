"""
Market Data Pipeline DAG

This DAG orchestrates the extraction, transformation, and loading (ETL) of gold price data.
Data Flow:
1. Create the necessary database schema and table if they don't exist.
2. Scrape gold price data from Yahoo Finance (using GLD ticker) starting from the last available date in the database.
3. Clean and prepare the data (select Close price, flatten columns, add currency).
4. Save the prepared data to a temporary CSV file.
5. Load the data from the CSV into the PostgreSQL database table 'raw.gold_prices'.

The DAG runs daily, with retries on failure.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import os


def scrape_market_data():
    """
    Task: Scrape Gold Price Data from Yahoo Finance

    Step-by-step execution:
    1. Connect to the PostgreSQL database using Airflow connection.
    2. Query the database to find the latest date in the gold_prices table.
    3. Determine the start date for scraping: either the day after the latest date or 20 years ago if no data exists.
    4. Use yfinance to download GLD (Gold ETF) data from the start date to present, at monthly intervals.
    5. If no new data is found, create an empty CSV and exit.
    6. Clean the data: select only the 'Close' price, reset index to get date, rename columns to 'date' and 'price', add 'currency' column as 'USD'.
    7. Save the cleaned DataFrame to /tmp/gold_data.csv for the next task to consume.

    Data Flow: Raw yfinance data -> Cleaned DataFrame -> CSV file
    """
    import yfinance as yf
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine
    import pandas as pd
    from datetime import datetime, timedelta

    # 1. Check the database for the latest date
    conn = BaseHook.get_connection("postgres_default")
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_url)

    try:
        last_date_query = "SELECT MAX(date) FROM raw.gold_prices"
        last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]
    except Exception:
        last_date = None

    # 2. Determine the start date
    if last_date:
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        start_date = (datetime.now() - timedelta(days=20 * 365)).strftime("%Y-%m-%d")

    # 3. Fetch data (Switching to GLD for better stability)
    # Using 'period' can sometimes be more reliable than 'start' for long histories
    data = yf.download("GLD", start=start_date, interval="1mo")

    if data.empty:
        print("No new data found. Check if the ticker GLD is available.")
        open("/tmp/gold_data.csv", "w").close()
        return

    # 4. Clean and Prepare
    # yfinance sometimes returns a multi-index header; we flatten it here
    df = data[["Close"]].reset_index()

    # Ensure columns are simple strings
    df.columns = ["date", "price"]
    df["currency"] = "USD"

    # Save for the next task
    df.to_csv("/tmp/gold_data.csv", index=False)
    print(f"Prepared {len(df)} records for loading.")


def load_data_to_postgres():
    """
    Task: Load Prepared Data into PostgreSQL Database

    Step-by-step execution:
    1. Check if the temporary CSV file exists and has content. If not, skip loading.
    2. Establish connection to PostgreSQL using Airflow connection.
    3. Read the cleaned data from /tmp/gold_data.csv into a pandas DataFrame.
    4. Use SQLAlchemy engine to append the data to the 'raw.gold_prices' table.
    5. Log the number of rows successfully loaded.

    Data Flow: CSV file -> pandas DataFrame -> PostgreSQL table 'raw.gold_prices'
    """
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine

    # Check if the file has data (not just an empty file)
    if (
        not os.path.exists("/tmp/gold_data.csv")
        or os.stat("/tmp/gold_data.csv").st_size == 0
    ):
        print("No new data to load. Skipping.")
        return

    conn = BaseHook.get_connection("postgres_default")
    connection_string = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
    connection_string = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    df = pd.read_csv("/tmp/gold_data.csv")
    engine = create_engine(connection_string)

    # Append new data to the existing table
    df.to_sql("gold_prices", engine, schema="raw", if_exists="append", index=False)
    print(f"Successfully loaded {len(df)} rows to raw.gold_prices")


with DAG(
    dag_id="market_data_scraper",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:
    """
    DAG Configuration:
    - dag_id: Unique identifier for the DAG.
    - start_date: The date from which the DAG can start running.
    - schedule_interval: Runs daily.
    - catchup: False means it won't run for past dates.
    - default_args: Default settings for tasks, including 2 retries with 1-minute delay.

    Task Dependencies (Data Flow):
    1. create_table: Ensures the database schema and table exist before data extraction.
    2. extract_data: Scrapes and prepares data, outputs to CSV.
    3. load_data: Consumes the CSV and loads data into the database.

    Execution Flow: create_table >> extract_data >> load_data
    """

    create_table = PostgresOperator(
        task_id="create_gold_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.gold_prices (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                price FLOAT,
                currency TEXT
            );
        """,
    )

    extract_data = PythonOperator(
        task_id="scrape_gold_price", python_callable=scrape_market_data
    )

    load_data = PythonOperator(
        task_id="load_gold_to_postgres", python_callable=load_data_to_postgres
    )

    # Define task dependencies: Ensure table exists before scraping, and scrape before loading
    create_table >> extract_data >> load_data

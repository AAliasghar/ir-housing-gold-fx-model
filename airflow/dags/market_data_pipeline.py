from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import os


def scrape_market_data():
    import yfinance as yf
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine
    import pandas as pd
    from datetime import datetime, timedelta

    # 1. Check the database for the latest date
    conn = BaseHook.get_connection("postgres_default")
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
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

    create_table >> extract_data >> load_data

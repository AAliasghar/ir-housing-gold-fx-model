"""
Inflation Data Pipeline DAG

This DAG orchestrates the ingestion and loading of Iran's inflation (CPI) data from the World Bank API.
Data Flow:
1. Create the necessary database schema if it doesn't exist.
2. Fetch inflation rate data from World Bank API for Iran (country code: IRN, indicator: FP.CPI.TOTL.ZG).
3. Transform the raw API response into a clean format with standardized date and inflation rate values.
4. Load the cleaned data into PostgreSQL table 'raw.inflation_rates', replacing the entire table with fresh history.

The DAG runs monthly to check for updated inflation data from the World Bank.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine


def ingest_inflation_data():
    """
    Task: Ingest Iran Inflation Data from World Bank API

    Step-by-step execution:
    1. Connect to the PostgreSQL database using Airflow connection.
    2. Query the World Bank API for Iran's inflation rate (CPI) data.
       - Indicator: FP.CPI.TOTL.ZG (Inflation, consumer prices - annual %)
       - Country: IRN (Iran)
       - Returns historical annual inflation rates.
    3. Extract the data list from the API response (at index 1 of the JSON response).
    4. Transform the raw data:
       - Filter out entries with null values.
       - Standardize dates from yearly format (YYYY) to YYYY-01-01 format.
       - Convert inflation rate values to float.
       - Extract indicator ID and set source as 'World Bank'.
    5. Convert the list of records into a pandas DataFrame and ensure date column is datetime type.
    6. Load the entire DataFrame into PostgreSQL table 'raw.inflation_rates' in the 'raw' schema.
       - Uses 'replace' mode to refresh the entire table with the latest history from World Bank.
       - This approach is efficient for small reference tables.
    7. Log the number of years of inflation history loaded.

    Data Flow: World Bank API -> JSON response -> Cleaned records list -> pandas DataFrame -> PostgreSQL table 'raw.inflation_rates'
    """
    print("--- 📉 Starting Iran Inflation (CPI) Ingestion ---")

    # 1. Database Connection
    conn = BaseHook.get_connection("postgres_default")
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
    engine = create_engine(db_url)

    # 2. Fetch from World Bank API
    # Indicator: FP.CPI.TOTL.ZG (Inflation, consumer prices - annual %)
    # Country: IRN (Iran)
    url = "https://api.worldbank.org/v2/country/IRN/indicator/FP.CPI.TOTL.ZG?format=json&per_page=100"

    try:
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json()[1]  # The data list is at index 1

        # 3. Transform to clean 'Raw' format
        records = []
        for entry in raw_data:
            if entry["value"] is not None:
                records.append(
                    {
                        "date": f"{entry['date']}-01-01",  # Standardize to YYYY-MM-DD
                        "inflation_rate": float(entry["value"]),
                        "indicator_id": entry["indicator"]["id"],
                        "source": "World Bank",
                    }
                )

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])

        # 4. Load to Postgres (Overwriting raw history is fine for this small table)
        df.to_sql(
            "inflation_rates",
            engine,
            schema="raw",
            if_exists="replace",  # We replace the raw table to refresh full history
            index=False,
        )
        print(f"✅ Success: Loaded {len(df)} years of inflation history.")

    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        raise


with DAG(
    dag_id="inflation_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",  # We check for updates monthly
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
) as dag:
    """
    DAG Configuration:
    - dag_id: Unique identifier for the DAG.
    - start_date: The date from which the DAG can start running.
    - schedule: Runs monthly to check for updated inflation data from World Bank.
    - catchup: False means it won't run for past dates.
    - default_args: Default settings for tasks, including 1 retry with 10-minute delay.

    Task Dependencies (Data Flow):
    1. create_schema: Ensures the 'raw' schema exists in PostgreSQL before data ingestion.
    2. load_inflation: Fetches inflation data from World Bank API and loads it into the database.

    Execution Flow: create_schema >> load_inflation
    """

    create_schema = SQLExecuteQueryOperator(
        task_id="create_raw_schema",
        conn_id="postgres_default",
        sql="CREATE SCHEMA IF NOT EXISTS raw;",
    )

    load_inflation = PythonOperator(
        task_id="ingest_world_bank_inflation", python_callable=ingest_inflation_data
    )

    # Define task dependencies: Ensure schema exists before ingesting data
    create_schema >> load_inflation

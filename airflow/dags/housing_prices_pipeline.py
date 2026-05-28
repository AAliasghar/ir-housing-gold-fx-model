"""
Housing Prices Pipeline DAG

This DAG orchestrates the ingestion of historical Tehran housing price data from a CSV file into PostgreSQL.
Data Flow:
1. Create the necessary database schema if it doesn't exist.
2. Locate and read the Tehran housing seed data CSV file (handles both Docker and local environments).
3. Load the CSV data directly into PostgreSQL table 'raw.housing_historical_raw', replacing any existing data.

The DAG runs once (@once schedule) since it's for loading historical seed data that doesn't change.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook


def ingest_housing_seeds():
    """
    Task: Ingest Tehran Housing Historical Seed Data from CSV to PostgreSQL

    Step-by-step execution:
    1. Resolve the file path for the housing data CSV, checking both Docker and local paths.
       - Docker path: /opt/airflow/data/tehran_housing_seeds.csv
       - Local path: Relative to the script directory (../../data/tehran_housing_seeds.csv)
       - Raises FileNotFoundError if neither path exists.
    2. Read the CSV file into a pandas DataFrame using pd.read_csv().
    3. Establish database connection using Airflow's 'postgres_default' connection.
       - Constructs SQLAlchemy engine URL from connection details.
    4. Load the DataFrame into PostgreSQL table 'raw.housing_historical_raw'.
       - Uses 'replace' mode to overwrite any existing data (appropriate for seed data).
       - Stores in 'raw' schema for organization and later processing by dbt/PySpark.
    5. Log the number of rows successfully ingested.

    Data Flow: CSV file -> pandas DataFrame -> PostgreSQL table 'raw.housing_historical_raw'
    """
    # 1. Path Resolution (Handles both Docker and Local runs)
    docker_path = "/opt/airflow/data/tehran_housing_seeds.csv"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.normpath(
        os.path.join(script_dir, "..", "..", "data", "tehran_housing_seeds.csv")
    )

    file_path = docker_path if os.path.exists(docker_path) else local_path

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Housing seed file not found at: {file_path}")

    print(f"📖 Reading data from: {file_path}")
    df = pd.read_csv(file_path)

    # 2. Get connection details from Airflow's Connection URI
    # This uses the 'postgres_default' ID you have in Airflow
    conn = BaseHook.get_connection("postgres_default")

    # Constructing the SQLAlchemy engine
    # Note: Inside Docker, host is usually 'postgres' or 'localhost' depending on your networking
    # db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
    engine = create_engine(db_url)

    # 3. Load to Postgres
    # Using the 'raw' schema to keep things organized for dbt/PySpark later
    df.to_sql(
        name="housing_historical_raw",
        con=engine,
        schema="raw",
        if_exists="replace",
        index=False,
    )

    print(f"✅ Successfully ingested {len(df)} rows into raw.housing_historical_raw")


# --- DAG Definition ---

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "housing_ingestion_pipeline",
    default_args=default_args,
    description="Ingest Tehran housing historical seed data into Postgres",
    schedule_interval="@once",  # Since it's historical seed data, run once or manually
    catchup=False,
    tags=["market", "housing", "raw"],
) as dag:
    """
    DAG Configuration:
    - dag_id: "housing_ingestion_pipeline" - Unique identifier for the DAG.
    - default_args: Default settings for all tasks including owner, start date, 1 retry with 5-minute delay.
    - description: Explains the DAG's purpose - ingesting historical Tehran housing data.
    - schedule_interval: "@once" - Runs only once since it's for loading static historical seed data.
    - catchup: False - Won't run for past dates.
    - tags: Categorization tags for easier filtering in Airflow UI.

    Task Dependencies (Data Flow):
    1. create_schema: Ensures the 'raw' schema exists in PostgreSQL before data loading.
    2. load_housing_data: Reads CSV file and loads housing data into the database.

    Execution Flow: create_schema >> load_housing_data
    """

    # Task 1: Ensure the 'raw' schema exists in the DB
    create_schema = SQLExecuteQueryOperator(
        task_id="ensure_raw_schema",
        conn_id="postgres_default",
        sql="CREATE SCHEMA IF NOT EXISTS raw;",
    )

    # Task 2: Run the Python ingestion logic
    load_housing_data = PythonOperator(
        task_id="ingest_housing_seeds_to_postgres",
        python_callable=ingest_housing_seeds,
    )

    # Define task dependencies: Ensure schema exists before ingesting data
    create_schema >> load_housing_data

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine


def ingest_inflation_data():
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

    create_schema = SQLExecuteQueryOperator(
        task_id="create_raw_schema",
        conn_id="postgres_default",
        sql="CREATE SCHEMA IF NOT EXISTS raw;",
    )

    load_inflation = PythonOperator(
        task_id="ingest_world_bank_inflation", python_callable=ingest_inflation_data
    )

    create_schema >> load_inflation

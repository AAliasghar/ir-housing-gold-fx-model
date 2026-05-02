from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import jdatetime
from sqlalchemy import create_engine


def sync_market_rates():
    print("--- 🚀 Starting USD/IRR Market Rate Sync ---")

    # 1. Connection using Airflow's internal 'postgres_default'
    conn = BaseHook.get_connection("postgres_default")
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
    engine = create_engine(db_url)

    # 2. Check for the last loaded date
    last_date = None
    try:
        query = "SELECT MAX(date) FROM raw.fx_rates"
        with engine.connect() as connection:
            last_date = connection.execute(query).scalar()
        print(f"🔎 Database check complete. Last recorded date: {last_date}")
    except Exception as e:
        print(f"⚠️ Table might be empty or missing. Error check: {e}")

    # 3. Fetch Data from Archive
    url = "https://raw.githubusercontent.com/SamadiPour/rial-exchange-rates-archive/data/currency/usd.json"
    print(f"🌐 Requesting historical archive from: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Connection Failed. HTTP Status: {response.status_code}")
        return

    raw_data = response.json()
    records = []

    # 4. Filter and Transform
    print("⚙️ Processing archive data...")
    for jalali_date, prices in raw_data.items():
        y, m, d = map(int, jalali_date.split("/"))
        g_date = jdatetime.date(y, m, d).togregorian()
        g_timestamp = pd.Timestamp(g_date)

        # Logic: If DB is empty, take EVERYTHING. If not, only take newer data.
        if last_date is None or g_timestamp > pd.Timestamp(last_date):
            records.append(
                {
                    "date": g_date,
                    "rate": float(prices["sell"]) * 10,  # Ensure Rial (Toman * 10)
                    "base_currency": "USD",
                    "target_currency": "IRR",
                }
            )

    # 5. Load phase
    if records:
        df = pd.DataFrame(records)
        print(f"📦 PREPARING LOAD: Found {len(df)} records to insert.")
        if last_date is None:
            print("🏁 Status: INITIAL LOAD DETECTED (Loading full history 2012-Today)")
        else:
            print(f"📈 Status: INCREMENTAL UPDATE (New records since {last_date})")

        df.to_sql(
            "fx_rates",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",  # Essential for fast bulk loading
        )
        print(f"✅ Success: {len(df)} rows written to raw.fx_rates.")
    else:
        print("😴 Nothing to do. Database is already in sync with the archive.")


with DAG(
    dag_id="fx_currency_scraper",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_fx_table",
        conn_id="postgres_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.fx_rates (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                rate FLOAT,
                base_currency TEXT,
                target_currency TEXT
            );
        """,
    )

    sync_fx = PythonOperator(
        task_id="sync_market_rates", python_callable=sync_market_rates
    )

    create_table >> sync_fx

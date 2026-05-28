from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import jdatetime
import yfinance as yf
from sqlalchemy import create_engine


def sync_market_rates():
    print("--- 🚀 Starting USD/IRR Market Rate Sync ---")

    # 1. Connection using Airflow's internal 'postgres_default'
    conn = BaseHook.get_connection("postgres_default")
    host = conn.host if conn.host else "postgres"
    port = conn.port if conn.port else 5432
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{host}:{port}/{conn.schema}"
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

    all_records = []

    # 3. LEGACY LOAD: 2006 - 2011 (Yahoo Finance)
    # We only do this if the table is totally empty (initial load)
    if last_date is None:
        print("📜 Phase 1: Fetching Legacy Data from Yahoo Finance (2006-2011)...")
        try:
            ticker = "USDIRR=X"
            legacy_data = yf.download(
                ticker,
                start="2006-01-01",
                end="2010-12-31",
                interval="1d",
                progress=False,
            )

            if not legacy_data.empty:
                # yfinance often puts 'Date' in the index. reset_index makes it a column.
                legacy_df = legacy_data.reset_index()

                # We dynamically find the 'Close' and 'Date' columns regardless of capitalization
                date_col = [c for c in legacy_df.columns if "date" in str(c).lower()][0]
                close_col = [c for c in legacy_df.columns if "close" in str(c).lower()][
                    0
                ]

                for _, row in legacy_df.iterrows():
                    # Flatten the value in case yfinance returns a Series/MultiIndex
                    rate_val = float(row[close_col])

                    all_records.append(
                        {
                            "date": row[date_col],
                            "rate": rate_val,
                            "base_currency": "USD",
                            "target_currency": "IRR",
                        }
                    )
                print(f"✅ Loaded {len(legacy_df)} monthly legacy records.")
        except Exception as e:
            print(f"⚠️ Yahoo Finance pull failed: {e}. Moving to Phase 2.")

    # 4. MARKET LOAD: 2012 - Present (Bonbast Archive)
    print("🌐 Phase 2: Requesting Market Archive (2012-Present)...")
    url = "https://raw.githubusercontent.com/SamadiPour/rial-exchange-rates-archive/data/currency/usd.json"

    response = requests.get(url)
    if response.status_code == 200:
        raw_data = response.json()
        print("⚙️ Filtering and transforming market data...")

        for jalali_date, prices in raw_data.items():
            y, m, d = map(int, jalali_date.split("/"))
            g_date = jdatetime.date(y, m, d).togregorian()
            g_timestamp = pd.Timestamp(g_date)

            # Only append if date is newer than the DB or if it's a fresh start
            if last_date is None or g_timestamp > pd.Timestamp(last_date):
                all_records.append(
                    {
                        "date": g_date,
                        "rate": float(prices["sell"]) * 10,  # Toman to Rial
                        "base_currency": "USD",
                        "target_currency": "IRR",
                    }
                )
    else:
        print(f"❌ Archive Connection Failed. Status: {response.status_code}")

    # 5. Load phase
    if all_records:
        df = pd.DataFrame(all_records)
        df["date"] = pd.to_datetime(df["date"])

        # Log status
        if last_date is None:
            print(f"🏁 INITIAL LOAD: Totaling {len(df)} records (Legacy + Market).")
        else:
            print(
                f"📈 INCREMENTAL UPDATE: Found {len(df)} new records since {last_date}."
            )

        df.to_sql(
            "fx_rates",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
        )
        print(f"✅ Success: Data written to raw.fx_rates.")
    else:
        print("😴 Nothing to do. Database is up to date.")


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

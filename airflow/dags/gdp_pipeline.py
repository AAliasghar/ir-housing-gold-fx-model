from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine


def ingest_gdp_data():
    print("--- 🏦 Starting Iran GDP Ingestion ---")

    # 1. Database Connection
    conn = BaseHook.get_connection("postgres_default")
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@postgres:5432/{conn.schema}"
    engine = create_engine(db_url)

    # 2. Indicators to Fetch
    # NY.GDP.MKTP.KD.ZG = GDP Growth (annual %)
    # NY.GDP.MKTP.CD = GDP (current US$)
    indicators = {
        "NY.GDP.MKTP.KD.ZG": "gdp_growth_pct",
        "NY.GDP.MKTP.CD": "gdp_current_usd",
    }

    all_dfs = []

    try:
        for code, name in indicators.items():
            print(f"🌐 Fetching {name} ({code}) from World Bank...")
            url = f"https://api.worldbank.org/v2/country/IRN/indicator/{code}?format=json&per_page=100"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()[1]

            temp_records = []
            for entry in data:
                if entry["value"] is not None:
                    temp_records.append(
                        {
                            "date": f"{entry['date']}-01-01",
                            "value": float(entry["value"]),
                            "indicator": name,
                        }
                    )

            all_dfs.append(pd.DataFrame(temp_records))

        # 3. Combine and Pivot
        # We combine them so we have one row per year with both columns
        raw_df = pd.concat(all_dfs)
        df_pivoted = raw_df.pivot(
            index="date", columns="indicator", values="value"
        ).reset_index()
        df_pivoted["date"] = pd.to_datetime(df_pivoted["date"])
        df_pivoted["country_code"] = "IRN"

        print(f"⚙️ Transformation complete. Prepared {len(df_pivoted)} years of data.")

        # 4. Load to Postgres
        df_pivoted.to_sql(
            "gdp_data",
            engine,
            schema="raw",
            if_exists="replace",  # Refreshing full history for raw macroeconomic data
            index=False,
        )
        print("✅ Success: Data written to raw.gdp_data.")
        print(df_pivoted.head(5))  # Log the first few rows for verification

    except Exception as e:
        print(f"❌ GDP Ingestion failed: {e}")
        raise


with DAG(
    dag_id="gdp_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="ensure_raw_schema",
        conn_id="postgres_default",
        sql="CREATE SCHEMA IF NOT EXISTS raw;",
    )

    load_gdp = PythonOperator(
        task_id="ingest_world_bank_gdp", python_callable=ingest_gdp_data
    )

    create_schema >> load_gdp

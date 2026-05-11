import pandas as pd
from sqlalchemy import create_engine
import os


# 1. Keep your main function as is, but handle the connection carefully
def ingest_housing_seeds(standalone_engine=None):
    print("🏠 Testing Tehran Housing Seed Ingestion...")

    # Use relative path for local testing, absolute path for Docker
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = "data/tehran_housing_seeds.csv"
    # file_path = os.path.join(script_dir, "..", "data", "tehran_housing_seeds.csv")
    # Go up TWO levels: from dags -> airflow -> project_root
    file_path = os.path.normpath(os.path.join(script_dir, "..", "..", "data", "tehran_housing_seeds.csv"))

    if not os.path.exists(file_path):
        print(f"❌ Error: File not found at {file_path}")
        return

    df = pd.read_csv(file_path)

    # If we passed an engine (local test), use it. Otherwise, use Airflow's Hook.
    if standalone_engine:
        engine = standalone_engine
    else:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("postgres_default")
        db_url = (
            f"postgresql://{conn.login}:{conn.password}@localhost:5432/{conn.schema}"
        )
        engine = create_engine(db_url)

    # Preview the data to prove it works
    print("✅ Data Loaded Successfully:")
    print(df.head())

    # Uncomment this only if your local Postgres is running and accessible
    # df.to_sql("housing_historical_raw", engine, schema="raw", if_exists="replace", index=False)


# 2. THIS IS THE KEY PART FOR YOUR TERMINAL TEST
if __name__ == "__main__":
    # We create a dummy engine or a local one to avoid the Airflow Import Error
    # If you just want to see the PRINT output, we don't even need a real connection
    print("🚀 Running standalone test...")

    # Mocking the engine for a print-only test
    mock_engine = create_engine("postgresql://user:pass@localhost:5432/db")

    ingest_housing_seeds(standalone_engine=mock_engine)

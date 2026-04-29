from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator


# This is a simple test function
def hello_airflow():
    print("The pipeline is awake and ready to scrape!")


# Define the "Brain" of the DAG
# with DAG(
#     dag_id="market_data_scraper",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval="@daily",
#     catchup=False,
# ) as dag:

#     test_task = PythonOperator(
#         task_id="check_connection", python_callable=hello_airflow
#     )

with DAG(
    dag_id="market_data_scraper",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task 1: Create a table for Gold prices
    create_table = PostgresOperator(
        task_id="create_gold_table",
        postgres_conn_id="postgres_default",  # We will set this in the UI next
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

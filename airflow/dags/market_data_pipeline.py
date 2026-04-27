from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# This is a simple test function
def hello_airflow():
    print("The pipeline is awake and ready to scrape!")


# Define the "Brain" of the DAG
with DAG(
    dag_id="market_data_scraper",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="check_connection", python_callable=hello_airflow
    )

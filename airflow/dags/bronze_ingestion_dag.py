from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import sys
import os

# Add spark_jobs to path so we can import the functions
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'spark_jobs'))
from spark_jobs.bronze_ingestion import download_binance_data, verify_loading

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_ingestion_dag',
    default_args=default_args,
    description='Downloads Binance data and verifies it with Spark',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:

    @task
    def ingest_data():
        # This calls the requests script to download data
        download_binance_data()

    @task
    def verify_data():
        # This calls the spark script to check the file
        verify_loading()

    # Define the flow: Download -> Verify
    ingest_data() >> verify_data()
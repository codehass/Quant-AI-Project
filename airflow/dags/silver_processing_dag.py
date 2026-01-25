from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# FIXED: Import the logic from your script
from spark_jobs.silver_processing import run_silver

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_processing_dag',
    default_args=default_args,
    description='Reads Bronze, Transforms, and writes to Postgres',
    schedule_interval='10,25,40,55 * * * *', # Runs 10 mins after Bronze (offset)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'spark', 'postgres'],
) as dag:

    run_silver_task = PythonOperator(
        task_id='process_silver_layer',
        python_callable=run_silver
    )

    run_silver_task
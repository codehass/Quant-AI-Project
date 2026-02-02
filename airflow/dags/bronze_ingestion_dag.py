from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # <--- NEW IMPORT
from datetime import datetime, timedelta
import sys
import os

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
    schedule_interval=timedelta(minutes=15), # <--- KEEPS RUNNING AUTOMATICALLY
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:

    @task
    def ingest_data():
        download_binance_data()

    @task
    def verify_data():
        verify_loading()

    # --- NEW TRIGGER TASK ---
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_layer',
        trigger_dag_id='silver_processing_dag', # Must match Silver DAG ID exactly
        wait_for_completion=False
    )

    # --- DEFINE FLOW ---
    # After verify_data finishes, it triggers silver
    ingest_data() >> verify_data() >> trigger_silver
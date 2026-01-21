# bronze ingestion dag
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    dag_id='bronze_ingestion_dag',  # Remember this ID!
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)
def bronze_flow():
    
    ingest_task = SparkSubmitOperator(
        task_id='ingest_bronze',
        application='/opt/airflow/dags/scripts/bronze_ingestion.py',
        conn_id='spark_default',
        verbose=True
    )

bronze_flow()
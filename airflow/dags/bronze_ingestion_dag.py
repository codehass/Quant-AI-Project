# bronze ingestion dag
from airflow.decorators import dag
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from spark_jobs.bronze_ingestion import run_bronze
from datetime import datetime

@dag(
    dag_id='bronze_ingestion_dag',  # Remember this ID!
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)
# run as a python operator
def bronze_flow():
    run_bronze()

bronze_flow()
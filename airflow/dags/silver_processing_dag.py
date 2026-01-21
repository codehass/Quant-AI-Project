# silver processing dag
from airflow.decorators import dag
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from spark_jobs.silver_processing import run_silver  

@dag(
    dag_id='silver_processing_dag',
    start_date=datetime(2026, 1, 21),
    schedule_interval='@daily', # Must match Bronze schedule exactly
    catchup=False
)
def silver_flow():

    run_silver()


silver_flow()
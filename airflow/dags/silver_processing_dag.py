# silver processing dag
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

@dag(
    dag_id='silver_processing_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily', # Must match Bronze schedule exactly
    catchup=False
)
def silver_flow():

    # 1. The Watchman (Sensor)
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='bronze_ingestion_dag', # Must match ID in file 1
        external_task_id=None, # None means "wait for the whole DAG to finish"
        timeout=600, # Fail if Bronze takes longer than 10 mins
        mode='reschedule' # Saves resources while waiting
    )

    # 2. The Worker (Spark Job)
    process_task = SparkSubmitOperator(
        task_id='process_silver',
        application='/opt/airflow/dags/scripts/silver_processing.py',
        conn_id='spark_default',
        verbose=True
    )

    # 3. Dependency
    wait_for_bronze >> process_task

silver_flow()
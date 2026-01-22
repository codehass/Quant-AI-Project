# silver processing dag
# silver_processing_dag.py
from airflow.decorators import dag, task  # <--- Added 'task'
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from spark_jobs.silver_processing import run_silver

@dag(
    dag_id='silver_processing_dag',
    start_date=datetime(2026, 1, 21),
    schedule_interval='@daily',
    catchup=False
)
def silver_flow():

    # The Sensor: Waits for Bronze DAG to finish
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='bronze_ingestion_dag', 
        external_task_id=None,
        check_existence=True,
        timeout=600,
        mode='reschedule'
    )

    # The Task Wrapper: Runs your Spark code
    @task(task_id='run_silver_spark_job')
    def execute_silver():
        run_silver()

    # The Dependency: Sensor >> Task
    # We call the task function execute_silver() to create the task instance
    wait_for_bronze >> execute_silver()

# Instantiate the DAG
silver_flow()
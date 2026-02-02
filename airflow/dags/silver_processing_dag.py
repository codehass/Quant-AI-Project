from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- WRAPPER FUNCTION ---
# This forces Airflow to reload the code every time the task runs
def execution_wrapper():
    import sys
    import os
    # Ensure correct path so Python can find the module
    sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'spark_jobs'))
    
    from spark_jobs.silver_processing import run_silver
    run_silver()

with DAG(
    'silver_processing_dag',
    default_args=default_args,
    description='Reads Bronze, Transforms, and writes to Postgres',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'spark', 'postgres'],
) as dag:

    # Use the wrapper here, NOT the direct function
    run_silver_task = PythonOperator(
        task_id='process_silver_layer',
        python_callable=execution_wrapper
    )

    trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_training',
        trigger_dag_id='train_btc_price_model',
        wait_for_completion=False
    )

    run_silver_task >> trigger_ml
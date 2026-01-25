from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Ensure Airflow can find your custom module
sys.path.append("/opt/airflow")
from spark_jobs.ml_training import train_model  

# --- CONFIGURATION FOR DOCKER ---
# These paths match the volumes in your docker-compose
JDBC_JAR_PATH = os.getenv("JDBC_JAR_PATH_env")
MODEL_OUTPUT_PATH = os.getenv("MODEL_OUTPUT_PATH_env")
DB_HOST_int = os.getenv("DATABASE_HOST_int")      # Internal service name
DB_PORT_int = os.getenv("DATABASE_PORT_int")      # Internal port
DB_NAME = os.getenv("DATABASE_NAME", "airflow")
DB_USER = os.getenv("DATABASE_USER", "airflow")
DB_PASS = os.getenv("DATABASE_PASSWORD", "airflow")

def train_model_task():
    # Call the imported function with the DAG variables
    train_model(
        jdbc_jar_path=JDBC_JAR_PATH,
        model_output_path=MODEL_OUTPUT_PATH,
        db_host=DB_HOST_int,
        db_port=DB_PORT_int,
        db_name=DB_NAME,
        db_user=DB_USER,
        db_pass=DB_PASS
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'train_btc_price_model',
    default_args=default_args,
    description='Trains Random Forest model on Silver data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    training_task = PythonOperator(
        task_id='train_model_spark',
        python_callable=train_model_task
    )

    training_task
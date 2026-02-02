from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
from spark_jobs.ml_training import train_model  

sys.path.append("/opt/airflow")

# --- CONFIG FOR DOCKER ---
JDBC_JAR_PATH = "/opt/airflow/postgresql-42.6.0.jar"
MODEL_OUTPUT_PATH = "/opt/airflow/ml/models/btc_price_predictor_1"
DB_HOST = os.getenv("DATABASE_HOST", "postgres") # Added default "postgres" just in case
DB_PORT = os.getenv("DATABASE_PORT", "5432")
DB_NAME = os.getenv("DATABASE_NAME", "airflow")
DB_USER = os.getenv("DATABASE_USER", "airflow")
DB_PASS = os.getenv("DATABASE_PASSWORD", "airflow")

def train_model_task():
    train_model(
        jdbc_jar_path=JDBC_JAR_PATH,
        model_output_path=MODEL_OUTPUT_PATH,
        db_host=DB_HOST,
        db_port=DB_PORT,
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
    schedule_interval=None, # <--- CHANGED: Stops it from running by itself
    catchup=False
) as dag:

    training_task = PythonOperator(
        task_id='train_model_spark',
        python_callable=train_model_task
    )

    training_task
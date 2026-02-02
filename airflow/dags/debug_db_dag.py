from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def list_tables_task():
    # 1. Get connection details from Environment (Same as your Spark Job)
    db_host = os.getenv("DATABASE_HOST", "postgres")
    db_user = os.getenv("DATABASE_USER", "airflow")
    db_password = os.getenv("DATABASE_PASSWORD", "airflow")
    db_name = os.getenv("DATABASE_NAME", "airflow")
    db_port = os.getenv("DATABASE_PORT", "5432")

    print(f"--- CONNECTING TO: {db_host}:{db_port} DB: {db_name} ---")

    try:
        # 2. Connect
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cur = conn.cursor()

        # 3. Query all table names in the 'public' schema
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        
        tables = cur.fetchall()
        
        print("\n--- FOUND THESE TABLES ---")
        found_silver = False
        for table in tables:
            print(f" - {table[0]}")
            if table[0] == 'silver_market_data':
                found_silver = True
        
        print("--------------------------")
        
        if found_silver:
            print("SUCCESS: 'silver_market_data' exists!")
        else:
            print("FAILURE: 'silver_market_data' is MISSING.")
            # Verify if maybe it is in a different schema?
            cur.execute("SELECT schema_name FROM information_schema.schemata;")
            schemas = cur.fetchall()
            print(f"Available Schemas: {schemas}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"CONNECTION ERROR: {e}")
        raise e

with DAG(
    'debug_database_connection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    check_db = PythonOperator(
        task_id='list_all_tables',
        python_callable=list_tables_task
    )
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv


load_dotenv()

def get_spark_session(app_name="BinanceSilver"):
    
    postgres_jar_path = "/opt/airflow/postgresql-42.6.0.jar"
    
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars", postgres_jar_path) \
        .config("spark.driver.extraClassPath", postgres_jar_path) \
        .getOrCreate()

def run_silver():
    spark = get_spark_session()
    
    # --- CONFIGURATION ---
    input_path = os.getenv("input_path_silver", "/opt/airflow/data/bronze_layer") 
    output_path = os.getenv("output_path_silver", "/opt/airflow/data/silver_layer")
    
    # --- DEFINE DB CONNECTION (CORRECTED) ---
    db_host = os.getenv("DATABASE_HOST", "postgres") # Default to service name
    db_user = os.getenv("DATABASE_USER", "airflow")
    db_password = os.getenv("DATABASE_PASSWORD", "airflow")
    db_name = os.getenv("DATABASE_NAME", "airflow")
    db_port = "5432"

    # FIXED: Construct URL cleanly using db_name, NOT db_user
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    
    # EXPLICIT: Use public schema to ensure visibility
    db_table = "public.silver_market_data"

    print(f"--- DATABASE SETTINGS ---")
    print(f"URL: {jdbc_url}")
    print(f"User: {db_user}")
    print(f"Table: {db_table}")

    # --- READ BRONZE ---
    # Append filename if input_path is just a folder
    if not input_path.endswith(".parquet"):
        bronze_file = os.path.join(input_path, "btc_minute_data.parquet")
    else:
        bronze_file = input_path

    print(f"Reading bronze data from: {bronze_file}")
    
    try:
        df_bronze = spark.read.parquet(bronze_file)
    except Exception as e:
        print(f"Error reading Bronze data: {e}")
        spark.stop()
        raise e 

    # --- LOGIC ---
    rows_before = df_bronze.count()
    df_clean = df_bronze.dropDuplicates(["open_time"])
    print(f"Removed {rows_before - df_clean.count()} duplicates.")

    window = Window.orderBy("open_time")
    window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)
    window_10 = Window.orderBy("open_time").rowsBetween(-9, 0)

    df_clean = df_clean.withColumn("close_t_plus_10", F.lead("close", 10).over(window))
    df_clean = df_clean.withColumn("return", 
                        (F.col("close") - F.lag("close", 1).over(window)) / F.lag("close", 1).over(window))
    df_clean = df_clean.withColumn("MA_5", F.avg("close").over(window_5))
    df_clean = df_clean.withColumn("MA_10", F.avg("close").over(window_10))
    df_clean = df_clean.withColumn("taker_ratio", 
                        F.col("taker_buy_base_volume") / F.col("volume"))

    df_silver = df_clean.filter(
        F.col("close_t_plus_10").isNotNull() &
        F.col("return").isNotNull()
    )

    if "ignore" in df_silver.columns:
        df_silver = df_silver.drop("ignore")

    # --- OUTPUT 1: PARQUET ---
    print(f"Writing Silver Parquet to: {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)

    # --- OUTPUT 2: POSTGRES DATABASE (JDBC) ---
    print(f"Writing to Database Table: {db_table}")
    
    jdbc_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    try:
        df_silver.write.jdbc(
            url=jdbc_url,
            table=db_table,
            mode="overwrite", 
            properties=jdbc_properties
        )
        print("Database write successful.")
        
    except Exception as e:
        print(f"Failed to write to DB: {e}")
        spark.stop()
        raise e 
    
    spark.stop()

if __name__ == "__main__":
    run_silver()
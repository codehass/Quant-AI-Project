import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv
    
load_dotenv()

def get_spark_session(app_name="BinanceSilver"):
    # DEFINE THE JAR PATH : This is the "Tool" file we mounted via Docker volume
    postgres_jar_path = os.getenv("JDBC_JAR_PATH_env")
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars", postgres_jar_path) \
        .config("spark.driver.extraClassPath", postgres_jar_path) \
        .getOrCreate()


def run_silver():
    spark = get_spark_session()
    
    # --- CONFIGURATION ---
    input_path = os.getenv("input_path_silver")
    output_path = os.getenv("output_path_silver")
    
    # --- DEFINE DB CONNECTION
    # This is the "Address" of the database (from Docker Compose env)
    jdbc_url = os.getenv("JDBC_URL_env")
    # db_user = os.getenv("POSTGRES_USER", "airflow")     # Matches docker-compose
    # db_password = os.getenv("POSTGRES_PASSWORD", "airflow") # Matches docker-compose
    
    db_user = os.getenv("DATABASE_USER")     # Matches docker-compose
    db_password = os.getenv("DATABASE_PASSWORD") # Matches docker-compose
    db_table = "silver_market_data"


    print(f"Reading bronze data from: {input_path}")
    try:
        df_bronze = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading Bronze data: {e}")
        return 

    # --- LOGIC ---
    # -> Deduplication
    rows_before = df_bronze.count()
    df_clean = df_bronze.dropDuplicates(["open_time"])
    print(f"Removed {rows_before - df_clean.count()} duplicates.")

    # -> Window Definitions
    window = Window.orderBy("open_time")
    window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)
    window_10 = Window.orderBy("open_time").rowsBetween(-9, 0)

    # -> Feature Engineering
    df_clean = df_clean.withColumn("close_t_plus_10", F.lead("close", 10).over(window))
    
    df_clean = df_clean.withColumn("return", 
                    (F.col("close") - F.lag("close", 1).over(window)) / F.lag("close", 1).over(window))

    df_clean = df_clean.withColumn("MA_5", F.avg("close").over(window_5))
    df_clean = df_clean.withColumn("MA_10", F.avg("close").over(window_10))

    df_clean = df_clean.withColumn("taker_ratio", 
                    F.col("taker_buy_base_volume") / F.col("volume"))

    # -> Filter Nulls (Clean-up)
    df_silver = df_clean.filter(
        F.col("close_t_plus_10").isNotNull() &
        F.col("return").isNotNull()
    )

    # -> Drop unnecessary columns
    if "ignore" in df_silver.columns:
        df_silver = df_silver.drop("ignore")

    # --- OUTPUT 1: PARQUET ---
    print(f"Writing Silver Parquet to: {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)

    # --- OUTPUT 2: POSTGRES DATABASE (JDBC) ---
    print(f"Writing to Database Table: {db_table} at {jdbc_url}")
    
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

if __name__ == "__main__":
    run_silver()

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Optional: Load .env if running locally, ignore if missing in Docker
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_spark_session(app_name="BinanceSilver"):
    # 1. DEFINE THE JAR PATH
    # This is the "Tool" file we mounted via Docker volume
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
    input_path = "/opt/airflow/data/bronze_layer"
    output_path = "/opt/airflow/data/silver_layer"

    # 2. DEFINE DATABASE CONNECTION
    # This is the "Address" of the database (from Docker Compose env)
    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/airflow")
    db_user = os.getenv("POSTGRES_USER", "airflow")     # Matches docker-compose
    db_password = os.getenv("POSTGRES_PASSWORD", "airflow") # Matches docker-compose
    db_table = "silver_market_data"

    print(f"Reading bronze data from: {input_path}")
    try:
        df_bronze = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading Bronze data: {e}")
        return 

    # --- LOGIC ---
    
    # 1. Deduplication
    rows_before = df_bronze.count()
    df_clean = df_bronze.dropDuplicates(["open_time"])
    print(f"Removed {rows_before - df_clean.count()} duplicates.")

    # 2. Window Definitions
    window = Window.orderBy("open_time")
    window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)
    window_10 = Window.orderBy("open_time").rowsBetween(-9, 0)

    # 3. Feature Engineering
    df_clean = df_clean.withColumn("close_t_plus_10", F.lead("close", 10).over(window))
    
    df_clean = df_clean.withColumn("return", 
                    (F.col("close") - F.lag("close", 1).over(window)) / F.lag("close", 1).over(window))

    df_clean = df_clean.withColumn("MA_5", F.avg("close").over(window_5))
    df_clean = df_clean.withColumn("MA_10", F.avg("close").over(window_10))

    df_clean = df_clean.withColumn("taker_ratio", 
                    F.col("taker_buy_base_volume") / F.col("volume"))

    # 4. Filter Nulls (Clean-up)
    df_silver = df_clean.filter(
        F.col("close_t_plus_10").isNotNull() &
        F.col("return").isNotNull()
    )

    # 5. Drop unnecessary columns
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
#============================================================>
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as spark_sum, when
# from pyspark.sql.window import Window
# from pyspark.sql import functions as F

# def run_silver():
#     spark = SparkSession.builder.appName("BinanceSilver").getOrCreate()

#     # --- INPUT: read from Bronze 
#     input_path = "/opt/airflow/data/bronze_layer"
#     print(f"Reading bronze data from: {input_path}")
#     df_bronze = spark.read.parquet(input_path)

# # --- Logic
#     # -> Null Checks
#     print("Checking for nulls...")
#     null_counts = df_bronze.select([
#         spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
#         for c in df_bronze.columns
#     ])
#     null_counts.show()

#     # -> Deduplication
#     print("Deduplicating...")
#     rows_before = df_bronze.count()
#     df_clean = df_bronze.dropDuplicates(["open_time"])
    
#     duplicates = rows_before - df_clean.count()
#     print(f"Removed {duplicates} duplicates.")

    
#     # --- target column
#     # Définir la fenêtre ordonnée par temps
#     window = Window.orderBy("open_time")
#     df_clean = df_clean.withColumn("close_t_plus_10", F.lead("close", 10).over(window))


#     # --- Creating features values
#     # -> Variations de prix (returns) :
#     df_clean = df_clean.withColumn("return", 
#                    (F.col("close") - F.lag("close", 1).over(window)) / F.lag("close", 1).over(window))


#     # -> Moyennes mobiles (5, 10 minutes)
#     # Définir les fenêtres pour les moyennes mobiles
#     window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)       # 5 dernières minutes (incluant la ligne actuelle)
#     window_10 = Window.orderBy("open_time").rowsBetween(-9, 0)      # 10 dernières minutes

#     df_clean = df_clean.withColumn("MA_5", F.avg("close").over(window_5))
#     df_clean = df_clean.withColumn("MA_10", F.avg("close").over(window_10))

#     # -> Volume et intensité de trading
#     df_bronze = df_bronze.withColumn("taker_ratio", 
#                    F.col("taker_buy_base_volume") / F.col("volume"))

#     # Afficher le schéma pour vérifier
#     df_bronze.printSchema()
#     df_bronze.show(20)

#     # -> Inspection of new columns
#     # Check for nulls
#     null_counts = df_bronze.select([
#         spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
#         for c in df_bronze.columns
#     ])
#     null_counts.show()    


#     # -> Filter null values in return and taker_ratio
#     df_clean = df_bronze.filter(
#     F.col("close_t_plus_10").isNotNull() &
#     F.col("return").isNotNull()
# )
#     # -> Inspection
#     # Check for nulls
#     null_counts = df_clean.select([
#         spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
#         for c in df_clean.columns
#     ])
#     null_counts.show()

#     # -> drop 'ignore'
#     df_silver = df_clean.drop('ignore')

#     # OUTPUT : Write to Silver Layer
#     output_path = "/opt/airflow/data/silver_layer"
#     df_silver.write.mode("overwrite").parquet(output_path)
#     print(f"Clean data written to: {output_path}")

#     spark.stop()

# if __name__ == "__main__":
#     run_silver()
#____________________________________________
# from .bronze_ingestion import load_raw_data
# from pyspark.sql.functions import col, sum as spark_sum, when

# def check_nulls():
#     df_bronze=load_raw_data()
#     null_counts = df_bronze.select([
#     spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
#     for c in df_bronze.columns
# ])
#     return null_counts.show()

# def drop_duplicates():
#     print("Déduplication...")
#     rows_before = df_bronze.count()
#     df_bronze = df_bronze.dropDuplicates(["open_time"])
#     duplicates = rows_before - df_bronze.count()
#     print(f" {duplicates} doublons supprimés")
#     return duplicates

# def raw_to_silver():
#     df_bronze=load_raw_data()
#     df_bronze_silver=df_bronze.check_nulls()
#     df_silver=df_bronze_silver.drop_duplicates()
#     return df_silver




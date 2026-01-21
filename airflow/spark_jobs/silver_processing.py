# silver processing script
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when

def run_silver():
    spark = SparkSession.builder.appName("BinanceSilver").getOrCreate()

    # 1. INPUT: Read what Bronze created
    input_path = "/opt/airflow/data/bronze_layer"
    print(f"Reading bronze data from: {input_path}")
    df_bronze = spark.read.parquet(input_path)

    # 2. LOGIC: Null Checks
    print("Checking for nulls...")
    null_counts = df_bronze.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_bronze.columns
    ])
    null_counts.show()

    # 3. LOGIC: Deduplication
    print("Deduplicating...")
    rows_before = df_bronze.count()
    df_clean = df_bronze.dropDuplicates(["open_time"])
    
    duplicates = rows_before - df_clean.count()
    print(f"Removed {duplicates} duplicates.")

    # 4. OUTPUT: Write to Silver Layer
    output_path = "/opt/airflow/data/silver_layer"
    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"Clean data written to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    run_silver()
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




# bronze ingestion functions
import os
from pyspark.sql import SparkSession

def run_bronze():
    spark = SparkSession.builder.appName("BinanceBronze").getOrCreate()

    # this path to match where your Docker container sees the file
    source_path = "/opt/airflow/data/btc_minute_data.parquet"
    
    print(f"Reading source from: {source_path}")
    df_raw = spark.read.parquet(source_path)

    # 2. SHOW (For logs)
    print("Source Schema:")
    df_raw.printSchema()

    # 3. OUTPUT: Write to Bronze Layer so Silver can find it
    output_path = "/opt/airflow/data/bronze_layer"
    df_raw.write.mode("overwrite").parquet(output_path)
    print(f"Data successfully written to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    run_bronze()
#_____________________________________{}
# from pyspark.sql import SparkSession
# from dotenv import load_dotenv
# import os 

# load_dotenv()

# def load_raw_data():
#     spark = SparkSession.builder \
#     .appName("BinancePipeline") \
#     .getOrCreate()
#     bronze_path=os.getenv("bronze_path_env") 
#     df_bronze = spark.read.parquet(bronze_path)
#     return df_bronze

# def verify_loading():
#     df_bronze=load_raw_data()
#     return df_bronze.head(5)

    
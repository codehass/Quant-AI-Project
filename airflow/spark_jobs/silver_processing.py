# silver processing script

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run_silver():
    
    # --- Initialize Spark
    spark = SparkSession.builder.appName("BinanceSilverProcessing").getOrCreate()
    
    # --- config Paths
    input_path = "/opt/airflow/data/bronze_layer"
    output_path = "/opt/airflow/data/silver_layer"
    
    # Read Bronze Data
    print(f"Reading data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    # Data Cleaning (Deduplication)
    initial_count = df.count()
    df = df.dropDuplicates(["open_time"])
    print(f"Dropped {initial_count - df.count()} duplicate rows.")
    
    # --- Feature Engineering
    # -> Define Windows
    w_time = Window.orderBy("open_time")
    w_5min = w_time.rowsBetween(-4, 0)
    w_10min = w_time.rowsBetween(-9, 0)
    
    df_transformed = df.withColumn("close_t_plus_10", F.lead("close", 10).over(w_time)) \
                       .withColumn("return", (F.col("close") - F.lag("close", 1).over(w_time)) / F.lag("close", 1).over(w_time)) \
                       .withColumn("MA_5", F.avg("close").over(w_5min)) \
                       .withColumn("MA_10", F.avg("close").over(w_10min)) \
                       .withColumn("taker_ratio", F.col("taker_buy_base_volume") / F.col("volume"))

    # -> Filter Invalid Data (Null Targets)
    df_clean = df_transformed.filter(
        F.col("close_t_plus_10").isNotNull() & 
        F.col("return").isNotNull()
    ).drop("ignore")
    
    # -> Write to Silver
    print(f"Writing {df_clean.count()} rows to {output_path}...")
    df_clean.write.mode("overwrite").parquet(output_path)
    
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




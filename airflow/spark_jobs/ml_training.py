from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

def train_model(jdbc_jar_path, model_output_path, db_host, db_port, db_name, db_user, db_pass):
    print(f"🚀 Starting Model Training in Airflow on {db_host}:{db_port}...")

    # --- 1. START SPARK SESSION ---
    # We increase memory because Airflow + Spark is heavy
    spark = SparkSession.builder \
        .appName("Airflow_Model_Training") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", jdbc_jar_path) \
        .getOrCreate()
    
    print("✅ Spark Session Created")

    # --- 2. LOAD DATA ---
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_pass,
        "driver": "org.postgresql.Driver"
    }

    print(f"📥 Loading data from {jdbc_url}...")
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table="silver_market_data", 
            properties=connection_properties
        )
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        spark.stop()
        raise e

    # --- 3. PREPARE DATA ---
    df = df.orderBy("open_time")
    
    # Simple Train/Test Split logic
    total_rows = df.count()
    train_size = int(total_rows * 0.8)
    
    window = Window.orderBy("open_time")
    df = df.withColumn("row_id", row_number().over(window))
    
    train_df = df.filter(col("row_id") <= train_size).drop("row_id", "open_time", "close_time")
    test_df = df.filter(col("row_id") > train_size).drop("row_id", "open_time", "close_time")

    # --- 4. TRAIN MODEL ---
    feature_cols = [
        'open', 'high', 'low', 'close', 'volume', 
        'quote_asset_volume', 'number_of_trades', 
        'taker_buy_base_volume', 'taker_buy_quote_volume', 
        'return', 'MA_5', 'MA_10', 'taker_ratio'
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    rf = RandomForestRegressor(featuresCol="features", labelCol="close_t_plus_10", numTrees=50, maxDepth=10)
    
    pipeline = Pipeline(stages=[assembler, rf])
    
    print("🤖 Training Model...")
    model = pipeline.fit(train_df)
    
    # --- 5. EVALUATE ---
    test_pred = model.transform(test_df)
    rmse = RegressionEvaluator(labelCol="close_t_plus_10", metricName="rmse").evaluate(test_pred)
    print(f"📊 Model Test RMSE: {rmse}")

    # --- 6. SAVE MODEL ---
    print(f"💾 Saving to shared volume: {model_output_path}")
    model.write().overwrite().save(model_output_path)
    
    spark.stop()
    print("✅ Training Complete.")
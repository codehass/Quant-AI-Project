# bronze ingestion functions
import requests
import pandas as pd
import os
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv

load_dotenv()

# --- Configuration
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 600

# --- DOWNLOAD FUNCTION (The "Ingestion")
def download_binance_data():
    bronze_path ="/opt/airflow/data/bronze_layer"
    
    # Ensure the directory exists
    os.makedirs(bronze_path, exist_ok=True)
    
    # Define full file path
    file_path = os.path.join(bronze_path,"btc_minute_data.parquet")
    print(f"Downloading data to: {file_path}")

    # Call Binance API
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": LIMIT}
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Binance API Error: {response.status_code}")

    data = response.json()

    # Define columns
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]

    # Convert to Pandas DataFrame
    df = pd.DataFrame(data, columns=columns)

    # Convert numeric columns
    numeric_cols = [
        "open", "high", "low", "close", "volume",
        "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)

    # Convert timestamps
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    # --- FIX: Force Microsecond Precision for Spark Compatibility ---
    df["open_time"] = df["open_time"].astype("datetime64[us]")
    df["close_time"] = df["close_time"].astype("datetime64[us]")
    
    # Save to Parquet (Overwrite existing file)
    df.to_parquet(file_path, engine="pyarrow", index=False)

# --- SPARK SESSION
def get_spark_session(app_name: str = "BinancePipeline") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

# --- READ FUNCTION (The "Verification")
def load_raw_data() -> DataFrame:
    spark = get_spark_session()
    bronze_path = os.getenv("bronze_path_env", "/opt/airflow/data/bronze_layer")
    
    # Spark reads the whole folder or specific parquet file
    # We point it to the file we just downloaded
    file_path = os.path.join(bronze_path, "btc_minute_data.parquet")    
    df_bronze = spark.read.parquet(file_path)
    return df_bronze

# --- VERIFY LOADING
def verify_loading():
    print("Verifying data with Spark...")
    df_bronze = load_raw_data()
    df_bronze.show(5)
    return "Verification Complete"


if __name__ == "__main__":
    # Test run: download then verify
    download_binance_data()
    verify_loading()


    
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import os


script_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.abspath(
    os.path.join(script_dir, "..", "models", "btc_price_predictor")
)


class BTCPredictor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("BTCBackendSession").getOrCreate()
        self.model = PipelineModel.load(model_path)

    def predict(self, data_dict):
        features = data_dict.model_dump()
        df_test = self.spark.createDataFrame([features])
        df_test = df_test.withColumnRenamed("return_", "return")
        predictions = self.model.transform(df_test)
        return predictions.select("prediction").first()["prediction"]


# predictor = BTCPredictor()

# result = predictor.predict(
# {
#     "open": 93159.02,
#     "high": 93172.0,
#     "low": 93138.57,
#     "close": 93172.0,
#     "volume": 8.92988,
#     "quote_asset_volume": 831779.0943744,
#     "number_of_trades": 2330,
#     "taker_buy_base_volume": 4.95472,
#     "taker_buy_quote_volume": 461507.3373702,
#     "return": 1.39224292052001,
#     "MA_5": 93182.796,
#     "MA_10": 93187.11,
#     "taker_ratio": 0.5548473215765497,
# }
# )

# print("close_t_plus_10 (BTC):", result)

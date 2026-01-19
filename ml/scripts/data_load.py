import requests
import pandas as pd

SYMBOL = "BTCUSDT"
INTERVAL = "1m"  # Intervalle d'une minute
LIMIT = 600  # Nombre de lignes à récupérer


def data_collection_api():
    response = requests.get(
        url="https://api.binance.com/api/v3/klines",
        params={"symbol": SYMBOL, "interval": INTERVAL, "limit": LIMIT},
    )

    if response.status_code != 200:
        print(f"Erreur {response.status_code}")
        return None

    data = response.json()

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    # Créer un DataFrame pandas
    df = pd.DataFrame(data, columns=columns)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    return df


df = data_collection_api()

df.to_parquet("btc_minute_data.parquet", engine="pyarrow", index=False)

print("Fichier Parquet sauvegarde")

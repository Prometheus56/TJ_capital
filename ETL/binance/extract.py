import pandas as pd
import requests
from utils.milliseconds_converter import convert_to_milliseconds

base_URL = "https://api.binance.com"
endpoint = "/api/v3/klines"

def get_binance_data(ids, start_time, end_time, interval="1d"):
    start_time_ms, end_time_ms = convert_to_milliseconds(f"{start_time} 00:00:00", f"{end_time} 00:00:00")

    params = {
        "symbol" : f"{ids}USDT",
        "interval" : interval,
        "startTime" : start_time_ms,
        "endTime" : end_time_ms
        }

    response = requests.get(base_URL + endpoint, params= params)
    response.raise_for_status()
    data = response.json()

    columns = [
        "Open Time", "Open", "High", "Low", "Close", "Volume",
        "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base Volume", "Taker Buy Quote Volume", "Ignore"
    ]
    df = pd.DataFrame(data, columns= columns)
    return df

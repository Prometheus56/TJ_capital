import pandas as pd
import requests
from datetime import datetime, timezone

class Binance:
    def __init__(self):
        self.base_URL = "https://api.binance.com"
        self.endpoint = "/api/v3/klines"

    def binance_download(self, ids, start_time, end_time, interval="1d"):
        start_time_ms, end_time_ms = self.time_convert(f"{start_time} 00:00:00", f"{end_time} 00:00:00")

        params = {
            "symbol" : f"{ids}USDT",
            "interval" : interval,
            "startTime" : start_time_ms,
            "endTime" : end_time_ms
            }

        response = requests.get(self.base_URL + self.endpoint, params= params)
        response.raise_for_status()
        data = response.json()

        columns = [
            "Open Time", "Open", "High", "Low", "Close", "Volume",
            "Close Time", "Quote Asset Volume", "Number of Trades",
            "Taker Buy Base Volume", "Taker Buy Quote Volume", "Ignore"
        ]
        df_complet = pd.DataFrame(data, columns= columns)
        df = df_complet[["Close Time", "Close"]]
        df.set_index('Close Time', inplace=True)
        self.df_list.append(df)
        return self.df_list
    
    def convert_to_milliseconds(self, start_time, end_time):
        start_time_utc = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        end_time_utc = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        
        start_time_ms = int(start_time_utc.timestamp() * 1000)
        end_time_ms = int(end_time_utc.timestamp() * 1000)

        return start_time_ms, end_time_ms
    
    def all_is_one(self,):
        result = pd.concat(self.df_list)
        deal = result.csv("E:\TJ_Capital")

        return deal
        


start_time = '2024-01-01'
end_time = '2024-02-02'

binance = Binance()
download = binance.binance_download('BTC','ETH', start_time, end_time)
print(download)
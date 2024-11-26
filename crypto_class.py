import requests
import datetime
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

class Crypto:

    def __init__(self):
        """
        Initializes the Crypto class with essential attributes.

        Attributes:
            df (DataFrame): DataFrame for storing coin data.
            merged_df (DataFrame): DataFrame for storing merged market and crypto data.
            API_key (str): The API key retrieved from environment variables for CoinGecko.

        Raises:
            ValueError: If the API_KEY environment variable is not set.
        """

        self.df = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.API_key = os.getenv('API_KEY')
        if not self.API_key:
            raise ValueError("API_KEY environment variable is not set")

    def yahoo_download(self, start_date, end_date):
        """
        Fetches NASDAQ data from Yahoo Finance within the specified date range.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.

        Returns:
            DataFrame: A DataFrame containing the historical NASDAQ data.
        """
        nasdaq = yf.Ticker("^IXIC")
        nasdaq_data = nasdaq.history(start=start_date, end=end_date)
        nasdaq_data.index = nasdaq_data.index.tz_localize(None)
        return nasdaq_data

    def coingecko_download(self, id=None, days=None, currency='usd', interval='daily', precision='2'):
        """
        Downloads historical market chart data for a specific cryptocurrency from CoinGecko.

        Args:
            id (str): The CoinGecko ID of the cryptocurrency (e.g., 'bitcoin').
            days (str): The number of days for which to fetch data.
            currency (str): The currency in which to report the data (default is 'usd').
            interval (str): The interval for the data points (e.g., 'daily').
            precision (str): The precision of the data (default is '2').

        Returns:
            dict: A dictionary containing the historical data from CoinGecko.
        """
        url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/market_chart?vs_currency={currency}&days={days}&interval={interval}&precision={precision}"
        headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": self.API_key}
        response = requests.get(url, headers=headers)
        response.json()
        return response.json()

    def data_transform(self, ids, days=None):
        """
        Transforms and prepares data for analysis. Downloads historical coin data and calculates metrics.

        Args:
            ids (list): A list of cryptocurrency IDs for which to download and process data.

        Returns:
            DataFrame: A DataFrame containing transformed data with calculated weights and index values.
        """
        rows = []
        ### Download historical data for each coin
        for id in ids:
            coin_data = self.coingecko_download(id=id, days=days)
            prices = coin_data['prices']
            market_caps = coin_data['market_caps']
            for price, market_cap in zip(prices, market_caps): ### json transformation
                date = convert_timestamp(price[0])
                price_value = price[1]
                market_cap_value = market_cap[1]
                rows.append([date, price_value, market_cap_value, id])

        df = pd.DataFrame(rows, columns=['date', 'price', 'market_cap', 'id'])

        ### Computation weight & index
        for date in df['date'].unique():
            total_market_cap = df[df['date'] == date]['market_cap'].sum()
            for index, row in df[df['date'] == date].iterrows():
                weight = row['market_cap'] / total_market_cap
                index_value = weight * row['price']
                df.loc[index, 'weight'] = weight
                df.loc[index, 'index'] = index_value
        self.df = df
        return self.df

    def nasdaq_data_transform(self, start_date, end_date):
        """
        Downloads NASDAQ data from Yahoo Finance and merges it with the cryptocurrency data.
        It calculates price percentage change and metric based on the weight of the market cap.

        Args:
            start_date (str): The start date for NASDAQ data download.
            end_date (str): The end date for NASDAQ data download.

        Returns:
            DataFrame: A DataFrame containing merged data from NASDAQ and the cryptocurrencies with calculated metrics.
        """
        ### Nasdaq data download and merge
        nasdaq_data = self.yahoo_download(start_date, end_date)
        nasdaq_data = nasdaq_data.reset_index()
        self.df['date'] = pd.to_datetime(self.df['date'])
        nasdaq_data['Date'] = pd.to_datetime(nasdaq_data['Date'])
        self.merged_df = pd.merge(self.df, nasdaq_data, left_on='date', right_on='Date', how='inner')

        #### Price % change and metric
        self.merged_df = self.merged_df.sort_values(by=['id', 'date'])
        self.merged_df['price_pct_change'] = self.merged_df.groupby('id')['price'].pct_change() * 100
        self.merged_df['metric'] = self.merged_df['price_pct_change'] * self.merged_df['weight']

        return self.merged_df

    def comparison_graph(self, id, left_metric='metric', right_metric='Close'):
        """
        Generates a comparison graph between two metrics for a given cryptocurrency ID.

        Args:
            id (str): The cryptocurrency ID for the comparison.
            left_metric (str): The column to be plotted on the left y-axis (default is 'metric').
            right_metric (str): The column to be plotted on the right y-axis (default is 'Close').

        Returns:
            None: Displays the plot.
        """
        if self.merged_df.empty:
            print("No merged data available. Please run data_transform first.")
            return

        # Filter the merged DataFrame to only include rows for the given id
        filter_df = self.merged_df[self.merged_df['id'] == id].copy()

        ### Date mess
        filter_df['date'] = pd.to_datetime(filter_df['date']).dt.date
        filter_df = filter_df.sort_values(by=['date'])
        filter_df = filter_df.groupby('date', as_index=False).agg({
            left_metric: 'sum',
            right_metric: 'sum'
        }).reset_index()
        filter_df = filter_df.drop_duplicates(subset='date')
        fig, ax1 = plt.subplots(figsize=(12, 6))

        ### Left graph
        ax1.plot(filter_df['date'], filter_df[left_metric], color='blue', label=f"{left_metric} ({id})", linestyle='-',
                 marker='o')
        ax1.set_xlabel('date')
        ax1.set_ylabel(left_metric, color='blue')
        ax1.tick_params(axis='y', colors='blue')
        unique_dates = filter_df['date'].unique()
        ax1.set_xticks(unique_dates)
        ax1.set_xticklabels(unique_dates, rotation=45)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        ### Right graph
        ax2 = ax1.twinx()
        ax2.plot(filter_df['date'], filter_df[right_metric], color='red', label=right_metric, linestyle='-', marker='o')
        ax2.set_ylabel(right_metric, color='red')
        ax2.tick_params(axis='y', colors='red')
        fig.autofmt_xdate()

        ### General stuff
        plt.title(f'Comparison of {left_metric} vs {right_metric} for {id}')
        fig.tight_layout()

        return plt.show()

    def run_script(self, ids=[], days=None):
        """
        Executes the entire workflow of downloading, transforming, and visualizing cryptocurrency data.

        Args:
            ids (list): A list of cryptocurrency IDs to process.

        Returns:
            None: Runs the entire script.
        """

        self.data_transform(ids, days=days)
        start_date = self.df['date'].min()
        end_date = self.df['date'].max()
        self.nasdaq_data_transform(start_date, end_date)
        self.merged_df.to_csv('merged_data.csv')

        for id in ids:
            self.comparison_graph(id=id, left_metric='metric', right_metric='Close')

def convert_timestamp(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

### Run through Commandline
def main():
    ### Run in terminal with "python3 crypto_class.py --ids bitcoin ethereum solana chainlink dogecoin polkadot cardano avalanche-2 binancecoin --days 14"
    parser = argparse.ArgumentParser(description="Crypto Data Analysis")
    parser.add_argument('-ids', '--ids', nargs='+', help='List of cryptocurrency IDs (e.g. bitcoin ethereum)', required=True)
    parser.add_argument('-days', '--days', help='No of days of history to fetch', required=True)
    args = parser.parse_args()

    crypto = Crypto()
    crypto.run_script(ids=args.ids, days=args.days)

if __name__ == "__main__":
    main()

### Run through IDE
#crypto = Crypto()
#crypto.run_script(ids=['bitcoin', 'ethereum', 'solana', 'chainlink', 'dogecoin', 'polkadot', 'cardano', 'avalanche-2', 'binancecoin'], days='1200')

# File: crypto_class.py (index.py)
"""
Tools for downloading and analyzing cryptocurrency and market data.
"""
import os
import requests
import datetime
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse
from dotenv import load_dotenv

load_dotenv()


def convert_timestamp(timestamp: int) -> str:
    """
    Convert a UNIX timestamp in milliseconds to a 'YYYY-MM-DD' string.

    Args:
        timestamp (int): UNIX timestamp in milliseconds.

    Returns:
        str: Date string in 'YYYY-MM-DD' format.
    """
    # Convert ms to seconds, then to UTC datetime, format as date
    return datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')


class CryptoNasdaqIndex:
    """
    Download and analyze cryptocurrency market data alongside NASDAQ metrics.

    This class fetches historical data from CoinGecko and Yahoo Finance, merges
    datasets, computes weighted indices, and generates comparison plots.
    """
    def __init__(self):
        """
        Initialize Crypto with empty DataFrames and API key.

        Raises:
            ValueError: If the CoinGecko API key (API_KEY) is not set in env.
        """
        # Prepare storage and API credentials
        self.df = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.API_key = os.getenv('API_KEY')
        if not self.API_key:
            raise ValueError("API_KEY environment variable is not set.")

    def yahoo_download(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch NASDAQ historical data from Yahoo Finance.

        Args:
            start_date (str): Start date in 'YYYY-MM-DD'.
            end_date (str): End date in 'YYYY-MM-DD'.

        Returns:
            pd.DataFrame: Historical NASDAQ data with timezone localized to None.
        """
        # Retrieve data for ^IXIC ticker
        ticker = yf.Ticker("^IXIC")
        data = ticker.history(start=start_date, end=end_date)
        # Remove timezone info for merging
        data.index = data.index.tz_localize(None)
        return data

    def coingecko_download(self, id: str, days: str, currency: str = 'usd', interval: str = 'daily', precision: str = '2') -> dict:
        """
        Download historical market chart data for a cryptocurrency from CoinGecko.

        Args:
            id (str): CoinGecko ID of the cryptocurrency.
            days (str): Number of days of history to fetch.
            currency (str): Reporting currency (default 'usd').
            interval (str): Data interval, e.g., 'daily'.
            precision (str): Decimal precision (default '2').

        Returns:
            dict: JSON response containing prices and market caps.
        """
        # Construct API URL with parameters
        url = (
            f"https://pro-api.coingecko.com/api/v3/coins/{id}/market_chart"
            f"?vs_currency={currency}&days={days}&interval={interval}&precision={precision}"
        )
        headers = {"accept": "application/json", "x-cg-pro-api-key": self.API_key}
        # Execute GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def data_transform(self, ids: list, days: str = None) -> pd.DataFrame:
        """
        Download and transform cryptocurrency data into a weighted index.

        Args:
            ids (list): Cryptocurrency IDs to process.
            days (str, optional): Number of days of history. Defaults to None.

        Returns:
            pd.DataFrame: Daily total market cap and weighted average price.
        """
        records = []  # Collect rows of [date, price, market_cap, id]
        for coin_id in ids:
            data = self.coingecko_download(id=coin_id, days=days)
            # Iterate through price and market cap pairs
            for price, mcap in zip(data['prices'], data['market_caps']):
                date = convert_timestamp(price[0])
                records.append([date, price[1], mcap[1], coin_id])

        # Build DataFrame from records
        df = pd.DataFrame(records, columns=['date', 'price', 'market_cap', 'id'])
        df['date'] = pd.to_datetime(df['date'])

        # Group by date to compute totals and weighted index
        result = (
            df.groupby('date')
              .apply(lambda x: pd.Series({
                  'total_market_cap': x['market_cap'].sum(),
                  'crypto_index': (x['price'] * x['market_cap']).sum() / x['market_cap'].sum()
              }))
              .reset_index()
        )
        self.df = result
        return self.df

    def nasdaq_data_transform(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Merge NASDAQ data with cryptocurrency index and compute metrics.

        Args:
            start_date (str): Start date for NASDAQ data.
            end_date (str): End date for NASDAQ data.

        Returns:
            pd.DataFrame: Merged DataFrame with price change and weighted metric.
        """
        # Download NASDAQ data and reset index
        nasdaq = self.yahoo_download(start_date, end_date).reset_index()
        # Ensure date columns are datetime
        self.df['date'] = pd.to_datetime(self.df['date'])
        nasdaq['Date'] = pd.to_datetime(nasdaq['Date'])
        # Merge on matching dates
        merged = pd.merge(self.df, nasdaq, left_on='date', right_on='Date', how='inner')

        # Sort and compute daily percent change per coin
        merged = merged.sort_values(['id', 'date'])
        merged['price_pct_change'] = merged.groupby('id')['price'].pct_change() * 100
        # Compute weight of each coin by market cap share
        merged['weight'] = merged['market_cap'] / merged['market_cap'].groupby(merged['date']).transform('sum')
        # Weighted metric across coins
        merged['metric'] = merged['price_pct_change'] * merged['weight']

        self.merged_df = merged
        return self.merged_df

    def comparison_graph(self, id: str, left_metric: str = 'metric', right_metric: str = 'Close') -> None:
        """
        Plot a comparison of two metrics for a given cryptocurrency ID.

        Args:
            id (str): Cryptocurrency ID to plot.
            left_metric (str): Column for left y-axis (default 'metric').
            right_metric (str): Column for right y-axis (default 'Close').
        """
        # Ensure merged data exists
        if self.merged_df.empty:
            print("No data available. Run data_transform() first.")
            return

        # Filter data for given coin ID
        df = self.merged_df[self.merged_df['id'] == id].copy()
        df['date'] = pd.to_datetime(df['date']).dt.date
        # Aggregate by date
        df = df.groupby('date', as_index=False).agg({left_metric: 'sum', right_metric: 'sum'})
        df = df.drop_duplicates('date')

        # Plot on primary axis
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax1.plot(df['date'], df[left_metric], linestyle='-', marker='o')
        ax1.set_xlabel('Date')  # X-axis label
        ax1.set_ylabel(left_metric)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        # Plot on secondary axis
        ax2 = ax1.twinx()
        ax2.plot(df['date'], df[right_metric], linestyle='-', marker='o')
        ax2.set_ylabel(right_metric)

        plt.title(f"Comparison of {left_metric} vs {right_metric} for {id}")
        fig.autofmt_xdate()  # Rotate labels
        plt.tight_layout()
        plt.show()

    def run_script(self, ids: list, days: str = None) -> None:
        """
        Execute full analysis pipeline: download, transform, save, and plot data.

        Args:
            ids (list): Cryptocurrency IDs to process.
            days (str, optional): Number of days of history to fetch.
        """
        # Perform data transformation for coins
        self.data_transform(ids, days)
        # Determine date range
        start = self.df['date'].min()
        end = self.df['date'].max()
        # Merge with NASDAQ and compute metrics
        self.nasdaq_data_transform(start, end)
        # Save merged results
        self.merged_df.to_csv('merged_data.csv')

        # Generate plots for each coin
        for coin_id in ids:
            self.comparison_graph(id=coin_id)


def main() -> None:
    """
    Command-line entry point for the Crypto class.

    Usage:
        python crypto_class.py --ids bitcoin ethereum --days 30
    """
    parser = argparse.ArgumentParser(description='Crypto Data Analysis')
    parser.add_argument('-ids', '--ids', nargs='+', required=True, help='Cryptocurrency IDs')
    parser.add_argument('-days', '--days', required=True, help='Number of days of history')
    args = parser.parse_args()

    crypto = CryptoNasdaqIndex()
    crypto.run_script(ids=args.ids, days=args.days)


if __name__ == '__main__':
    main()
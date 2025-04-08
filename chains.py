import pandas as pd
import requests
from datetime import datetime

class ChainsDataFetcher:
    def __init__(self):
        """
        Initializes the ChainsDataFetcher class with essential attributes.
        Downloads TVL (Total Value Locked) of each available chain.

        Attributes:
            chains_db (str): Path to the original database.
            url (str): Source page URL for downloading the data.
        """

        self.chains_db = "/home/jakub/lama/chains.csv"
        self.url = 'https://api.llama.fi/v2/chains'


    def defillama_downloader(self):
        """
        Fetches Chain data from Defillama API for the previous day.

        Returns:
            DataFrame: A DataFrame containing the fetched Defillama data.
        """
        response = requests.get(self.url)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data) 

    def add_row(self):
        """
        Initializes Deffillama_downloader, transform the data, and add it to the original database.

        Returns:
            DataFrame: Updated database.
        """
        # Load the existing database
        database_df = pd.read_csv(self.chains_db, parse_dates=['Date'], index_col='Date')
        

        # Download the latest chain data
        chains_api_df = self.defillama_downloader()
        chains_api_df = chains_api_df[['name', 'tvl']]  

        # Prepare today's data
        today_date = datetime.today().strftime('%Y-%m-%d')
        total_tvl = chains_api_df['tvl'].sum()

        # Prepare a new row
        new_row = {'Total': total_tvl}
        for chain in chains_api_df['name']:
            tvl_value = chains_api_df.loc[chains_api_df['name'] == chain, 'tvl'].values[0].round(2)
            new_row[chain] = tvl_value

        # Ensure all chain columns exist in the database
        all_chains = set(database_df.columns).union(set(new_row.keys()))
        database_df = database_df.reindex(columns=all_chains)

        # Add today's data as a new row
        new_row_df = pd.DataFrame([new_row], index=[today_date])
        updated_df = pd.concat([database_df, new_row_df], axis=0)

        # Save the updated database with `Date` as a column
        updated_df.index = pd.to_datetime(updated_df.index).strftime('%Y-%m-%d')
        updated_df.reset_index(names='Date').to_csv(self.chains_db, index=False)

        print("New row added and saved to the database.")

        return updated_df



defillama_chains = ChainsDataFetcher()
updated_df = defillama_chains.add_row()




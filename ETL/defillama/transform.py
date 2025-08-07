from __future__ import annotations
import pandas as pd
import datetime
from datetime import datetime
import os
from pathlib import Path
from typing import Callable
from collections import Counter
# ────────────────  custom functions  ─────────────────
from utils.universal_functions import MrProper
from .extract import get_defillama_data

cleaner = MrProper()

# ________________  transform functions  ______________________
class Transform():
    """
    Clean raw CSVs so they can ascend to PostgreSQL as a new whole table containing historical data.
    """    
    def transform_csv_standart(self, df) -> pd.DataFrame:
        df = cleaner.clean_name(df)
        df = cleaner.clean_date(df)


        # Drop unwanted columns
        drop_cols = ['timestamp', 'totalcirculating']
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        df.interpolate(method='linear', axis=0, limit_direction='forward', inplace=True)
        num_cols = df.select_dtypes(include='number').columns
        df[num_cols] = df[num_cols].round(0)
        df['total'] = df.select_dtypes(include='number').sum(axis=1)
        return df
    
    def transform_csv_protocols(self, df) -> pd.DataFrame:
        df = df.T
        # First row contains headers
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        # Clean protocol names
        df = cleaner.clean_name(df)
        # Reset index to column and normalize date
        df = df.rename_axis('date').reset_index()
        df = cleaner.clean_date(df)
        df = df.ffill()
        
        # Filter protocols with TVL < 5_000_000 on last date
        protocols = [col for col in df.columns if col != 'date']
        last = df.iloc[-1]
        allowed_protocols = [col for col in protocols if last[col] > 5_000_000]

        # Round up
        num_cols = df.select_dtypes(include='number').columns
        df[num_cols] = df[num_cols].round(0)
        
        # Keep 'date' + allowed protocols
        df = df[['date'] + allowed_protocols]
        
        return df

    def row_chains(self) -> dict:
        df = get_defillama_data('chains')
        df['tvl'] = df['tvl'].round(0)
        today = pd.Timestamp.today().normalize()
        total = df['tvl'].sum()
        vals = {
            'date': today,
            'total': total
        }
        df['name'] = cleaner.clean_name(df['name'])
        chains_tvl = df.set_index('name')['tvl'].to_dict()
        vals.update(chains_tvl)
        return vals

    def row_protocols(self) -> dict:
        # Pull in raw API data
        df = get_defillama_data('protocols')

        today = pd.Timestamp.today().normalize()

        # Ensure numeric TVL, drop any non-numeric
        df['tvl'] = pd.to_numeric(df['tvl'], errors='coerce').fillna(0).round(0)

        # **Filter out all protocols with TVL below 1 000 000**
        df = df[df['tvl'] > 5_000_000]

        # Compute total TVL across the filtered set
        total = df['tvl'].sum()

        # Clean up names to match your static CSV column names
        df['name'] = cleaner.clean_name(df['name'])

        # Build the dict: { protocol_name: tvl, … }
        proto_tvl = df.set_index('name')['tvl'].to_dict()

        # Return exactly the same shape as your transform_export CSV,
        # so no new columns ever get added at load time
        vals = {'date': today, 'total': total}
        vals.update(proto_tvl)
        return vals
    
    def row_stablecoins(self) -> dict:
        df = get_defillama_data('stablecoins')

        today = pd.Timestamp.today().normalize()
        df = df.replace('TotalCirculating', 'total')
        df = df.sort_values('tvl', ascending=False).head(1596)
        # build a mask for the total row
        mask = df['name'] == 'total'

        # pull the peggedUSD number out of the nested dict and assign it
        df.loc[mask, 'tvl'] = (
            df.loc[mask, 'totalCirculatingUSD']
            .apply(lambda d: d.get('peggedUSD', 0))
        )
        vals = {'date': today}
        df['name'] = cleaner.clean_name(df['name'])
        df['tvl'] = df['tvl'].round(0)
        chain_tvl = df.set_index('name')['tvl']
        vals.update(chain_tvl)
        return vals


    def row_dexs_chains(self) -> dict:
        df = get_defillama_data('dexschains')
        

        # breakdown24h 
        df['breakdown24h'] = df['breakdown24h'].apply(lambda x: x if isinstance(x, dict) else {})
        flat_per_row = df['breakdown24h'].apply(
            lambda d: {chain: round(sum(dex_volume.values()), 0)
                       for chain, dex_volume in d.items()}
        )

        total_counter = Counter()
        for row_dict in flat_per_row:
            total_counter.update(row_dict)
        chain_tvl = dict(total_counter)
        chain_tvl = {cleaner.clean_name(k): v for k, v in total_counter.items()}
        today = pd.Timestamp.today().normalize()
        total = sum(total_counter.values())
        vals = {
            'date': today,
            'total': total
        }
        vals.update(chain_tvl)
        return vals
    
    def row_dexs(self) -> dict:
        df = get_defillama_data('dexschains')
        df['name'] = cleaner.clean_name(df['name'])
        df['tvl'] = df['total24h'].round(0)
        today = pd.Timestamp.today().normalize()
        total = df['total24h'].sum()
        chain_tvl = df.set_index('name')['total24h'].to_dict()
        vals = {
            'date': today,
            'total': total
        }
        vals.update(chain_tvl) 
        return vals
    
    def extract_chains_protocols_symbol(self) -> dict[str, str]:

        defillama_list = ['chains', 'protocols']
        all_dfs = []

        for data in defillama_list:
            df = get_defillama_data(data)
            if data == 'chains':
                df = df[['name', 'tokenSymbol']].copy()
                df.rename(columns={'tokenSymbol': 'symbol'}, inplace=True)
            else:
                df = df[['name', 'symbol']].copy()

            # Normalize the 'name' column
            df['name'] = cleaner.clean_name(df['name'])

            # Remove " v<number>" suffix and trim whitespace
            df['symbol'] = (
                df['symbol']
                .str.replace(r"\s+v\d+$", "", regex=True)
                .str.strip()
            )

            # Snake-case clean the 'symbol'
            df['symbol'] = cleaner.clean_name(df['symbol'])

            # Append cleaned DataFrame
            all_dfs.append(df)

        # Combine both cleaned dataframes
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Drop duplicates by 'symbol' and then 'name'
        combined_df = combined_df.drop_duplicates(subset='symbol', keep='first')
        combined_df = combined_df.drop_duplicates(subset='name', keep='first')

        # Return final mapping as a dictionary
        return print(dict(zip(combined_df['name'], combined_df['symbol'])))
        
tf = Transform()
TRANSFORMS: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    # Standart approach
    'chains': tf.transform_csv_standart,
    'dexs' : tf.transform_csv_standart,
    'dexs_chains': tf.transform_csv_standart,
    # 'perps': tf.transform_csv_standart,
    'stablecoins': tf.transform_csv_standart,

    # Special approach
    'protocols': tf.transform_csv_protocols,
}
    
tf.extract_chains_protocols_symbol()
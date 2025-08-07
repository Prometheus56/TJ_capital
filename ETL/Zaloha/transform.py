from __future__ import annotations
import pandas as pd
import datetime
from datetime import datetime
import os
from pathlib import Path
from typing import Callable
from collections import Counter
# ────────────────  custom functions  ─────────────────
from universal_functions import MrProper
from extract import get_data

cleaner = MrProper()

# ________________  transform functions  ______________________
class Transform():
    """
    Clean raw CSVs so they can ascend to PostgreSQL as a new whole table containg historical data.
    """    
    def transform_standart(self, df) -> pd.DataFrame:
        df = cleaner.clean_name(df)
        df = cleaner.clean_date(df)


        # Drop unwanted columns
        drop_cols = ['timestamp', 'totalcirculating']
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        df.interpolate(method='linear', axis=0, limit_direction='forward', inplace=True)
        df['total'] = df.select_dtypes(include='number').sum(axis=1)
        return df
    
    def transform_protocols(self, df) -> pd.DataFrame:
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
        # Filter protocols with TVL < 1,000,000 on last date
        protocols = [col for col in df.columns if col != 'date']
        
        # Filter protocols with TVL < 5_000_000 on last date
        last = df.iloc[-1]
        allowed_protocols = [col for col in protocols if last[col] > 5_000_000]
        
        # Keep 'date' + allowed protocols
        df = df[['date'] + allowed_protocols]
        
        return df

    def row_chains(self) -> dict:
        df = get_data('chains')
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
        df = get_data('protocols')

        today = pd.Timestamp.today().normalize()

        # Ensure numeric TVL, drop any non-numeric
        df['tvl'] = pd.to_numeric(df['tvl'], errors='coerce').fillna(0)

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
        df = get_data('stablecoins')

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
        chain_tvl = df.set_index('name')['tvl']
        vals.update(chain_tvl)
        return vals


    def row_dexs_chains(self) -> dict:
        df = get_data('dexschains')
        

        # breakdown24h 
        df['breakdown24h'] = df['breakdown24h'].apply(lambda x: x if isinstance(x, dict) else {})
        flat_per_row = df['breakdown24h'].apply(
            lambda d: {chain: sum(dex_volume.values())
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
        df = get_data('dexschains')
        df['name'] = cleaner.clean_name(df['name'])
        today = pd.Timestamp.today().normalize()
        total = df['total24h'].sum()
        chain_tvl = df.set_index('name')['total24h'].to_dict()
        vals = {
            'date': today,
            'total': total
        }
        vals.update(chain_tvl) 
        return vals
    
    
tf = Transform()
TRANSFORMS: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    # Standart approach
    'chains': tf.transform_standart,
    'dexs' : tf.transform_standart,
    'dexs_chains': tf.transform_standart,
    # 'perps': tf.transform_standart,
    'stablecoins': tf.transform_standart,

    # Special approach
    'protocols': tf.transform_protocols,
}
    
# df = pd.read_csv(r'Z:\TJ_Capital\TJ_Capital_database\perps.csv')
tf = Transform()
tf.row_protocols()
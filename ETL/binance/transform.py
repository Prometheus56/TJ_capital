import pandas as pd
# ────────────────  custom functions  ─────────────────
from utils.universal_functions import MrProper
cleaner = MrProper()

def transform_binance_data(df):
    """
    Transforms raw Binance data DataFrame:
    - Selects relevant columns
    - Converts 'Open Time' and 'Close Time' from epoch milliseconds to date objects
    - Renames columns to snake_case
    """
    # 1. Select relevant columns and make a copy
    columns = ['Open Time', 'Close Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = df[columns].copy()

    # 2. Convert epoch-ms to dates on original column names
    df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms').dt.date
    df['Close Time'] = pd.to_datetime(df['Close Time'], unit='ms').dt.date

    # 3. Clean and rename columns to snake_case
    cleaned_columns = cleaner.clean_name(columns)
    df.columns = cleaned_columns

    return df


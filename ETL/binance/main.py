import pandas as pd
import typer
from typing import List

from .extract import get_binance_data
from .transform import transform_binance_data
from .load import create_binance_postgres_table

app = typer.Typer()

@app.command()
def create_table(
    ids: List[str] = typer.Option(
        ..., "--ids", "-i", 
        help="List of Binance symbols (e.g. BTC ETH …)"
    ),
    start_time: str = typer.Option(
        ..., "--start-time", 
        help="Start date (YYYY-MM-DD)"
    ),
    end_time: str = typer.Option(
        ..., "--end-time", 
        help="End date (YYYY-MM-DD)"
    ),
    ):
    """
    Creates Binance PostgreSQL tables for the given list of symbol IDs.
    """

    print("Creating binance postgres tables for chosen ids... ")
    for id in ids:
        table_name = f"{id.lower()}"
        print(f"📥 Loading table: {table_name}")
        extracted_data = get_binance_data(id, start_time, end_time)
        transformed_data = transform_binance_data(extracted_data)
        create_binance_postgres_table(transformed_data, table_name)

if __name__ == "__main__":
    app()
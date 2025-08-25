#!/usr/bin/env python3
"""
Main CLI for the TVL pipeline

Commands:
  setup   - Parse raw CSVs, create cleaned CSVs and recreate DB tables
  update  - Download daily data, transform and upsert rows into existing tables
"""

import typer
from pathlib import Path
import sys
from dotenv import load_dotenv
import pandas as pd
from typing import List

from .extract import get_defillama_data
from .transform import TRANSFORMS
from .load import TVLDataLoader
from .transform import Transform
transform = Transform()

app = typer.Typer()
load_dotenv()

RAW_DIR = Path(r"/home/vpnadmin/TJ_Capital/TJ_Capital_database")
CLEAN_DIR = RAW_DIR / "updated"
CLEAN_DIR.mkdir(exist_ok=True)

def transform_export_csv(table: str):
    """
    Read raw CSV, apply transform, write cleaned CSV.
    """
    raw_path = RAW_DIR / f"{table}.csv"
    clean_path = CLEAN_DIR / "updated"
    if not raw_path.exists():
        typer.secho(f"Input file not found: {raw_path}", err=True, fg=typer.colors.RED)
        raise typer.Exit(1)
    df_raw = pd.read_csv(raw_path)
    df_clean = TRANSFORMS[table](df_raw)
    df_clean.to_csv(clean_path, index=False)
    typer.secho(f"✅ {table}: wrote {clean_path}", fg=typer.colors.GREEN)

def run_transform_export_csv(tables: List[str]):
    """
    Run transforms for specified tables or all if None.
    """
    for tbl in tables or TRANSFORMS.keys():
        transform_export_csv(tbl)

@app.command()
def setup(
    raw_dir: Path = typer.Option(   
        RAW_DIR,
        "--raw_dir", "-rd",
        help= "Path to directory where raw csv are stored"
    ),
    updated_dir: Path = typer.Option(
        None, "--upd_dir", "-ud", 
        help="Path to directory where to save transformed csvs"
    ),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-tab",
        help=""
    )
    ) -> None:
    
    global RAW_DIR, CLEAN_DIR
    RAW_DIR = raw_dir
    CLEAN_DIR = updated_dir or (RAW_DIR / "updated")
    CLEAN_DIR.mkdir(exist_ok=True)
    
    typer.secho(f"🔄 Creating tables from cleaned CSVs in {CLEAN_DIR}...", fg=typer.colors.BLUE)
    loader = TVLDataLoader()
    loader.create_defillama_postgresql_table_from_csv(CLEAN_DIR)
    typer.secho("✅ Setup complete.",fg=typer.colors.GREEN)

@app.command()
def update() -> None:
    """
    Download daily data and update DB tables.
    """
    typer.secho("🔄 Fetching today's data and updating tables...", fg=typer.colors.BLUE)
    loader = TVLDataLoader()
    loader.upsert_defillama_daily_rows()
    typer.secho("✅ Daily update complete.", fg=typer.colors.GREEN)

@app.command()
def create_reference_table():
    """
    Create a reference table mapping coin names to their symbols/tickers.
    """
    typer.secho(
    f"🔄 Building reference table reference_table from chains_table and protocols_table...",
    fg=typer.colors.BLUE,
    )
    transformed_data = transform.extract_chains_protocols_symbol()
    loader = TVLDataLoader()
    loader.create_tickers_table(transformed_data)
    typer.secho(f"✅ Reference table reference_table created successfully.",fg=typer.colors.GREEN,)

if __name__ == "__main__":
    app()


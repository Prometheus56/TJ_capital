# File: upper_cli.py
"""
CLI tool for CSV cleaning, PostgreSQL table management, and DefiLlama TVL operations.
"""
import argparse
import os
import tempfile
import json
from datetime import date

import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
from dotenv import load_dotenv

load_dotenv()


class Upper:
    """
    Encapsulates operations for:
      - Cleaning and interpolating CSV data
      - Creating and bulk-loading Postgres tables
      - Downloading DefiLlama TVL data
      - Upserting daily TVL into Postgres
    """
    def __init__(self, db_config: dict = None):
        """
        Initialize Upper with database configuration.

        Args:   
            db_config (dict, optional): psycopg2 connection parameters.
                If not provided, values are loaded from environment variables.

        Raises:
            ValueError: If the database password is not set.
        """
        # Determine DB config from arguments or environment
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = {
                'dbname': os.getenv('DB_NAME', 'defillama'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD'),
                'host': os.getenv('DB_HOST', '185.8.166.8'),
                'port': os.getenv('DB_PORT', '5432'),
            }
            # Ensure password is provided
            if not self.db_config['password']:
                raise ValueError("Database password not set. Define DB_PASSWORD in environment.")
        # API endpoint for DefiLlama chains
        self.url = "https://api.llama.fi/v2/chains"

    def clean_raw_csv(self, csv_path: str) -> None:
        """
        Clean and interpolate a raw CSV file with chain TVL data.

        Args:
            csv_path (str): Path to the input CSV file.
        """
        
        df = pd.read_csv(csv_path, parse_dates=["Date"], index_col="Date")
        
        if 'Timestamp' in df.columns:
            df.drop(columns=['Timestamp'], inplace=True)
        df.interpolate(method='linear', axis=0, limit_direction='forward', inplace=True)
        df = df.apply(pd.to_numeric)
        df['total'] = df.sum(axis=1)
        df.to_csv(csv_path)
        print(f"✅ Cleaned CSV: {csv_path}")

    def create_table(self, csv_path: str, table_name: str) -> None:
        """
        Create a Postgres table matching a CSV's schema and bulk-load its data.

        Args:
            csv_path (str): Path to the CSV file.
            table_name (str): Name of the table to create.
        """
        # Load CSV into DataFrame and normalize column names
        df = pd.read_csv(csv_path)
        df.columns = [col.strip().lower()
                      .replace(' ', '_')
                      .replace('.', '_')
                      .replace('+', '_') for col in df.columns]
        
        # Convert 'date' column to datetime if present
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # Build SQL for dropping and creating table
        cols_defs = []
        for col in df.columns:
            ident = sql.Identifier(col)
            if col == 'date':
                cols_defs.append(sql.SQL("{} DATE PRIMARY KEY").format(ident))
            else:
                cols_defs.append(sql.SQL("{} DOUBLE PRECISION").format(ident))

        create_sql = sql.SQL(
            """
            DROP TABLE IF EXISTS {table} CASCADE;
            CREATE TABLE {table} ({cols});
            """
        ).format(table=sql.Identifier(table_name), cols=sql.SQL(', ').join(cols_defs))

        # Execute SQL and load data via COPY
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                conn.commit()
                # Write to temporary CSV for COPY
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
                    df.to_csv(tmp.name, index=False)
                    temp_csv = tmp.name
                with open(temp_csv, 'r') as f:
                    next(f)  # skip header
                    copy_sql = sql.SQL("COPY {table} ({cols}) FROM STDIN WITH CSV").format(
                        table=sql.Identifier(table_name),
                        cols=sql.SQL(', ').join(map(sql.Identifier, df.columns))
                    )
                    cur.copy_expert(copy_sql, f)
                    conn.commit()
                # Clean up temp file
                os.remove(temp_csv)
        print(f"✅ Table '{table_name}' created and data loaded.")

    def defillama_downloader(self) -> pd.DataFrame:
        """
        Download raw chain TVL data from DefiLlama API.

        Returns:
            pd.DataFrame: Raw chain TVL data.
        """
        # Fetch JSON from API
        response = requests.get(self.url)
        response.raise_for_status()
        data = response.json()
        # Convert to DataFrame
        return pd.DataFrame(data)

    def add_row(self) -> None:
        """
        Fetch today's TVL per chain and upsert into the 'chains' Postgres table.

        Maintains individual DOUBLE PRECISION columns for each chain, adding new
        columns automatically when new chains appear.
        """
        # Download current chain TVL data
        df = self.defillama_downloader()[['name', 'tvl']]
        # Normalize chain names for SQL-safe identifiers
        df['name'] = (
            df['name']
              .str.strip()
              .str.lower()
              .str.replace(' ', '_')
              .str.replace('.', '_')
              .str.replace('+', '_')
        )

        # Prepare date, total TVL, and per-chain dict
        today = date.today()
        total = float(df['tvl'].sum())
        chain_data = df.set_index('name')['tvl'].to_dict()

        # Build insert values dict: date, total, and one entry per chain
        vals = {'date': today, 'total': total}
        vals.update(chain_data)

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                # Fetch existing non-key columns from 'chains'
                cur.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_name = %s AND column_name NOT IN ('date', 'total')",
                    ('chains',)
                )
                existing = {row[0] for row in cur.fetchall()}

                # Add new DOUBLE PRECISION columns for any chains not yet in table
                new_cols = set(chain_data) - existing
                for col in new_cols:
                    cur.execute(
                        sql.SQL("ALTER TABLE chains ADD COLUMN {col} DOUBLE PRECISION").format(
                            col=sql.Identifier(col)
                        )
                    )

                # Construct upsert clause: insert or update on date conflict
                cols = list(vals.keys())
                idents = [sql.Identifier(c) for c in cols]
                placeholders = [sql.Placeholder(c) for c in cols]
                updates = [
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                    for c in cols if c != 'date'
                ]

                upsert_sql = sql.SQL(
                    "INSERT INTO chains ({cols}) VALUES ({vals})"
                    " ON CONFLICT (date) DO UPDATE SET {updates};"
                ).format(
                    cols=sql.SQL(', ').join(idents),
                    vals=sql.SQL(', ').join(placeholders),
                    updates=sql.SQL(', ').join(updates)
                )

                # Execute the upsert with our vals mapping
                cur.execute(upsert_sql, vals)
                conn.commit()

        print("✅ New row upserted into 'chains' table.")


def main() -> None:
    """
    CLI entry point. Parses arguments and dispatches commands:
      - clean: CSV cleaning and interpolation
      - create_table: table creation and bulk load
      - download: fetch raw chain TVL data
      - add_row: upsert today's TVL with dynamic chain columns
    """
    parser = argparse.ArgumentParser(
        description='upper_cli: CSV cleaning, table management, and dynamic TVL upserts'
    )
    parser.add_argument('--dbname', help='Postgres database name')
    parser.add_argument('--user', help='Postgres username')
    parser.add_argument('--password', help='Postgres password')
    parser.add_argument('--host', help='Postgres host')
    parser.add_argument('--port', help='Postgres port')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Subcommand: clean CSV
    clean_p = subparsers.add_parser(
        'clean',
        help='Clean CSV: drop Timestamp, interpolate missing, add total TVL column'
    )
    clean_p.add_argument('csv_path', help='Path to raw CSV')

    # Subcommand: create_table
    create_p = subparsers.add_parser(
        'create_table',
        help='Create Postgres table from CSV and bulk-load data'
    )
    create_p.add_argument('csv_path', help='Path to CSV file')
    create_p.add_argument('table_name', help='Name of the table to create')

    # Subcommand: download
    download_p = subparsers.add_parser(
        'download',
        help='Download DefiLlama chain TVL data (print or save to CSV)'
    )
    download_p.add_argument('-o', '--output_csv', help='Optional CSV output path')

    # Subcommand: add_row
    subparsers.add_parser(
        'add_row',
        help='Fetch today\'s chain TVL, add new chain columns if needed, and upsert into "chains" table'
    )

    args = parser.parse_args()
    # Build DB config from CLI args or environment
    db_conf = {
        'dbname': args.dbname or os.getenv('DB_NAME', 'defillama'),
        'user': args.user or os.getenv('DB_USER', 'postgres'),
        'password': args.password or os.getenv('DB_PASSWORD'),
        'host': args.host or os.getenv('DB_HOST', '185.8.166.8'),
        'port': args.port or os.getenv('DB_PORT', '5432'),
    }
    cli = Upper(db_config=db_conf)

    # Command dispatch
    if args.command == 'clean':
        cli.clean_raw_csv(args.csv_path)
    elif args.command == 'create_table':
        cli.create_table(args.csv_path, args.table_name)
    elif args.command == 'download':
        df = cli.defillama_downloader()
        if args.output_csv:
            df.to_csv(args.output_csv, index=False)
            print(f"✅ Data saved to {args.output_csv}")
        else:
            print(df)
    elif args.command == 'add_row':
        cli.add_row()

if __name__ == "__main__":
    main()

import os
from datetime import date
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from universal_functions import MrProper
import io
from transform import Transform
from pathlib import Path
load_dotenv()

trans = Transform()

class Chains:
    def __init__(self):
        # Determine DB config from arguments or environment
        self.db_config = {
            'dbname': 'tj_capital',
            'user': 'postgres',
            'password': os.getenv("DB_PASSWORD"),
            'host': '185.8.166.8',
            'port': '5432'
        }
        # Ensure password is present
        if not self.db_config['password']:
            raise ValueError(
                "Database password not set. Please define DB_PASSWORD env var or in .env file."
            )
        # API endpoint for DefiLlama chains

        self.defillama = MrProper()

    

    def create_table(self, csv_path: str, table_name: str):
        """
        Drop any existing table, create a new schema matching the CSV columns,
        and bulk-load data via PostgreSQL COPY.

        Args:
            csv_path (str): Path to the CSV file.
            table_name (str): Name of the table to create.
        """
        # Load CSV into DataFrame
        df = pd.read_csv(csv_path, parse_dates=['date'])

        # Build CREATE TABLE SQL definitions
        column_defs = []
        for col in df.columns:
            ident = sql.Identifier(col)
            if col == 'date':
                column_defs.append(sql.SQL("{} DATE PRIMARY KEY").format(ident))
            else:
                column_defs.append(sql.SQL("{} REAL").format(ident))

        create_sql = sql.SQL(
            """
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table} ({cols});
            """
        ).format(
            table=sql.Identifier(table_name),
            cols=sql.SQL(', ').join(column_defs)
        )

        # Execute SQL and bulk load via COPY using an in-memory buffer
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                # Drop & create the table
                cur.execute(create_sql)
                conn.commit()

                # Prepare COPY statement
                copy_sql = sql.SQL(
                    "COPY {table} ({cols}) FROM STDIN WITH CSV"
                ).format(
                    table=sql.Identifier(table_name),
                    cols=sql.SQL(", ").join(map(sql.Identifier, df.columns)),
                )

                # Write DataFrame to an in-memory buffer
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)

                # Skip header row and stream into COPY
                next(csv_buffer)
                cur.copy_expert(copy_sql, csv_buffer)
                conn.commit()
                print(f"✅ Data loaded into table '{table_name}'")
    
    def run_create_table(self, folder: str | Path):
        folder_path = Path(folder)
        csv_files = folder_path.glob('*_upd.csv')
        for csv_path in csv_files:
            table_name = csv_path.stem.replace("_upd","")
            self.create_table(str(csv_path), table_name)

    def add_row(self) -> None:
        """
        Fetch today's TVL per chain and upsert into the 'chains' Postgres table.

        Maintains individual REAL columns for each chain, adding new
        columns automatically when new chains appear.
        """
        row_funcs = {
            'chains': trans.row_chains,
            'protocols': trans.row_protocols,
            'stablecoins': trans.row_stablecoins,
            'dexs': trans.row_dexs,
            'dexs_chains': trans.row_dexs_chains,
        }
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                for table_name, row_func in row_funcs.items():
                    vals = row_func()

                    cur.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = %s AND column_name NOT IN ('date','total')",
                        (table_name, )
                    )
                    existing = {row[0] for row in cur.fetchall()}

                    new_cols = set(vals) - existing - {'date','total'}
                    for col in new_cols:
                        cur.execute(
                            sql.SQL("ALTER TABLE {table} ADD COLUMN {col} REAL").format(
                                table = sql.Identifier(table_name),
                                col = sql.Identifier(col)
                            )
                        )
                    
                    # Prepare upsert
                    cols = list(vals.keys())
                    idents = [sql.Identifier(c) for c in cols]
                    placeholders = [sql.Placeholder(c) for c in cols]
                    updates = [
                        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                        for c in cols if c != 'date'
                    ]
                    upsert_sql = sql.SQL(
                        """
                        INSERT INTO {table} ({cols}) VALUES ({vals})
                        ON CONFLICT (date) DO UPDATE SET {updates};
                        """
                    ).format(
                        table = sql.Identifier(table_name),
                        cols = sql.SQL(', ').join(idents),
                        vals = sql.SQL(', ').join(placeholders),
                        updates = sql.SQL(', ').join(updates)
                    )
                    # Normalize types
                    vals = {k: (float(v) if isinstance(v, (np.float32, np.float64)) else
                                int(v) if isinstance(v, (np.int32, np.int64)) else
                                v)
                            for k, v in vals.items()}

                    cur.execute(upsert_sql, vals)
                    conn.commit()

                    print(f"✅ New row upserted into '{table_name}' table.")


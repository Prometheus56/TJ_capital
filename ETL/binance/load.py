import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import io

from utils.universal_functions import MrProper
cleaner = MrProper()
load_dotenv()

def create_binance_postgres_table(df: pd.DataFrame, table_name: str):
    db_config = {
        'dbname': 'tj_capital',
        'user': 'postgres',
        'password': os.getenv('DB_PASSWORD'),
        'host': '185.8.166.8',
        'port': '5432'
    }

    if not db_config['password']:
        raise ValueError(
            "Database password not set. Please define DB_PASSWORD env var or in .env file."
        )

    column_defs = []
    for col in df.columns:
        ident = sql.Identifier(col)
        if col == 'close_time':
            column_defs.append(sql.SQL("{} DATE PRIMARY KEY").format(ident))
        elif col == 'open_time':
            column_defs.append(sql.SQL("{} DATE").format(ident))
        else:
            column_defs.append(sql.SQL("{} REAL").format(ident))

    create_sql = sql.SQL(
        """
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} ({cols});
        """
    ).format(
        table = sql.Identifier(table_name),
        cols = sql.SQL(', ').join(column_defs)
    )
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
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

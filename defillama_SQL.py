import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import requests
import datetime
import tempfile

class Defillama:
    def __init__(self, database_name):
        self.db_config = {
            'dbname': 'defillama',
            'user': 'postgres',
            'password': '',
            'host': '185.8.166.8',
            'port': '5432'
        }
        self.database_name = database_name
        self.database_specifier(database_name)

    def database_specifier(self, database_name):
        if database_name == "chains":
            self.csv_path = "/run/media/jakub/USB DRIVE/lama/chains.csv"
            self.url = "https://api.llama.fi/v2/chains"
            self.table_name = "chains"
        elif database_name == "protocols":
            self.csv_path = "/run/media/jakub/USB DRIVE/lama/protocols.csv"
            self.url = "https://api.llama.fi/protocols"
            self.table_name = "protocols"
        else:
            raise ValueError(f"Unsupported database specified: {database_name}")

    def create_table(self):
        self.database_specifier(self.database_name)
        df = pd.read_csv(self.csv_path)
        df.columns = [col.strip().lower().replace(" ", "_").replace(".", "_").replace("+", "_").replace("3f","") for col in df.columns]

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        column_defs = []
        for col in df.columns:
            if col == 'date':
                column_defs.append(f"{col} DATE PRIMARY KEY")
            else:
                column_defs.append(f"{col} DOUBLE PRECISION")

        create_table_sql = (
            f"DROP TABLE IF EXISTS {self.table_name} CASCADE;\n"
            f"CREATE TABLE {self.table_name} (\n"
            + ",\n ".join(column_defs) +
            "\n);"
        )

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()

                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', dir='/tmp') as tmp_file:
                    df.to_csv(tmp_file.name, index=False)
                    temp_csv = tmp_file.name

                with open(temp_csv, "r") as f:
                    next(f)
                    cur.copy_expert(sql.SQL(
                        f"COPY {self.table_name} ({', '.join(df.columns)}) FROM STDIN WITH CSV"
                    ), f)

                conn.commit()
        os.remove(temp_csv)
        print(f"✅ Data loaded successfully into '{self.table_name}'.")

    def defillama_downloader(self):
        response = requests.get(self.url)
        response.raise_for_status()
        return pd.DataFrame(response.json())

    def add_row(self):
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}'
                """)
                valid_columns = [row[0] for row in cursor.fetchall()]

                chains_api_df = self.defillama_downloader()[['name', 'tvl']]
                chains_api_df.columns = chains_api_df.columns.str.lower()

                today_date = datetime.date.today().strftime('%Y-%m-%d')

                raw_tvl_dict = dict(chains_api_df.values)
                tvl_dict = {
                    k.strip().lower().replace(" ", "_").replace(".", "_"): float(v)
                    for k, v in raw_tvl_dict.items()
                    if k.strip().lower().replace(" ", "_").replace(".", "_") in valid_columns
                }

                tvl_dict["total"] = float(chains_api_df["tvl"].sum())
                tvl_dict["date"] = today_date

                columns = list(tvl_dict.keys())
                values = list(tvl_dict.values())

                insert_query = sql.SQL("""
                    INSERT INTO {table} ({fields})
                    VALUES ({placeholders})
                    ON CONFLICT (date) DO UPDATE SET
                    {updates};
                """).format(
                    table=sql.Identifier(self.table_name),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)),
                    updates=sql.SQL(', ').join(
                        sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(col))
                        for col in columns if col != "date"
                    )
                )

                cursor.execute(insert_query, values)
                conn.commit()

        print(f"✅ Full row for {today_date} added/updated in '{self.table_name}'.")


# === MAIN EXECUTION ===
if __name__ == "__main__":
    fetcher = Defillama("chains")
    fetcher.create_table()

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import requests
import datetime
import tempfile

class Defillama:
    """
    Class to manage downloading data from DefiLlama API, saving it to CSV, and uploading it to a PostgreSQL database.
    """

    def __init__(self, database_name):
        """
        Initialize Defillama class with a target database ('chains' or 'protocols').

        Parameters:
        - database_name: str — either 'chains' or 'protocols'
        """
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
        """
        Configure file path, API URL, and table name based on the selected database.

        Parameters:
        - database_name: str — name of the dataset ('chains' or 'protocols')
        """
        # Chains
        if database_name == "chains":
            self.csv_path = "/run/media/jakub/USB DRIVE/lama/chains.csv"
            self.url = "https://api.llama.fi/v2/chains"
            self.table_name = "chains"
        # Protocols
        elif database_name == "protocols":
            self.csv_path = "/run/media/jakub/USB DRIVE/lama/protocols.csv"
            self.url = "https://api.llama.fi/protocols"
            self.table_name = "protocols"
        else:
            raise ValueError(f"Unsupported database specified: {database_name}")

    def create_table(self):
        """
        Creates a PostgreSQL table based on the CSV structure and populates it with CSV data.
        """

        # Load and clean CSV
        self.database_specifier(self.database_name)
        df = pd.read_csv(self.csv_path)
        
        # Normalize column names
        df.columns = [col.strip().lower().replace(" ", "_").replace(".", "_").replace("+", "_") for col in df.columns]

        # Convert 'date' column to datetime if present
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # Create SQL column definitions based on dataframe structure
        column_defs = []
        for col in df.columns:
            if col == 'date':
                column_defs.append(f"{col} DATE PRIMARY KEY")
            else:
                column_defs.append(f"{col} DOUBLE PRECISION")

        # SQL: Drop old table and create a new one
        create_table_sql = (
            f"DROP TABLE IF EXISTS {self.table_name} CASCADE;\n"
            f"CREATE TABLE {self.table_name} (\n"
            + ",\n ".join(column_defs) +
            "\n);"
        )

        # Connect to the database and execute the table creation and import
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                conn.commit()

                # Save dataframe temporarily to CSV
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', dir='/tmp') as tmp_file:
                    df.to_csv(tmp_file.name, index=False)
                    temp_csv = tmp_file.name

                # Bulk load CSV into the PostgreSQL table
                with open(temp_csv, "r") as f:
                    next(f)  # Skip header row
                    cur.copy_expert(sql.SQL(
                        f"COPY {self.table_name} ({', '.join(df.columns)}) FROM STDIN WITH CSV"
                    ), f)

                conn.commit()

        os.remove(temp_csv)
        print(f"✅ Data loaded successfully into '{self.table_name}'.")

    def defillama_downloader(self):
        """
        Download data from the DefiLlama API.

        Returns:
        - DataFrame: the downloaded data from API
        """
        response = requests.get(self.url)
        response.raise_for_status()
        return pd.DataFrame(response.json())

    def add_row(self):
        """
        Fetch today's data from API and insert/update a row in the PostgreSQL table using the current date as key.
        """

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cursor:
                # Get list of valid column names in the table
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}'
                """)
                valid_columns = [row[0] for row in cursor.fetchall()]

                # Download and format data from DefiLlama API
                chains_api_df = self.defillama_downloader()[['name', 'tvl']]
                chains_api_df.columns = chains_api_df.columns.str.lower()

                # Format today's date
                today_date = datetime.date.today().strftime('%Y-%m-%d')

                # Prepare dictionary of values to insert
                raw_tvl_dict = dict(chains_api_df.values)
                tvl_dict = {
                    k.strip().lower().replace(" ", "_").replace(".", "_"): float(v)
                    for k, v in raw_tvl_dict.items()
                    if k.strip().lower().replace(" ", "_").replace(".", "_") in valid_columns
                }

                # Add total TVL and date
                tvl_dict["total"] = float(chains_api_df["tvl"].sum())
                tvl_dict["date"] = today_date

                # Prepare insert query
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

                # Execute the query
                cursor.execute(insert_query, values)
                conn.commit()

        print(f"✅ Full row for {today_date} added/updated in '{self.table_name}'.")


# === MAIN EXECUTION ===
if __name__ == "__main__":
    fetcher = Defillama("chains")
    fetcher.create_table()

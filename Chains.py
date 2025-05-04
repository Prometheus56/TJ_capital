"""
Module for analyzing Total Value Locked (TVL) data of various chains over a specified date range.
"""
import argparse
import os
from datetime import datetime, date
from typing import List, Optional, Union, Dict, Any

import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()


class ChainsAnalytics:
    """
    Analyze chain TVL data over a specified date range using a PostgreSQL database.

    Attributes:
        db_config (dict): Configuration for PostgreSQL connection.
        df (pd.DataFrame): DataFrame containing loaded TVL data, indexed by date.
    """
    def __init__(self, db_config: dict = None):
        """
        Initialize with database configuration.

        Args:
            db_config (dict, optional): psycopg2 connection parameters.
                If not provided, values are loaded from environment variables.

        Raises:
            ValueError: If the database password is not set.
        """
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
            if not self.db_config['password']:
                raise ValueError("Database password not set. Define DB_PASSWORD in environment.")
        self.df: pd.DataFrame = pd.DataFrame()

    def load_data(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        extra_where: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Pull data from Postgres into a DataFrame within the date range.

        Args:
            table (str): table name to query
            columns (List[str], optional): specific columns to select
            start_date (str or date, optional): inclusive start date
            end_date (str or date, optional): inclusive end date
            extra_where (str, optional): additional SQL WHERE clause

        Returns:
            pd.DataFrame: indexed by Date
        """
        # Parse start/end if strings
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Build SELECT fields
        if columns:
            fields_sql = sql.SQL(", ").join([sql.Identifier(col) for col in columns])
        else:
            fields_sql = sql.SQL("*")

        # Build WHERE clauses and parameters
        where_clauses = []
        params: List = []
        if start_date:
            where_clauses.append(sql.SQL("\"Date\" >= %s"))
            params.append(start_date)
        if end_date:
            where_clauses.append(sql.SQL("\"Date\" <= %s"))
            params.append(end_date)
        if extra_where:
            where_clauses.append(sql.SQL(extra_where))

        if where_clauses:
            where_sql = sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses)
        else:
            where_sql = sql.SQL("")

        # Compose query
        query = sql.SQL(
            """
            SELECT {fields}
            FROM {table}
            {where}
            ORDER BY \"Date\"
            """
        ).format(
            fields=fields_sql,
            table=sql.Identifier(table),
            where=where_sql,
        )

        # Execute via pandas
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                sql_str = query.as_string(conn)
                df = pd.read_sql_query(sql_str, conn, params=params, parse_dates=["Date"])

        df.set_index("Date", inplace=True)
        self.df = df
        return df

    def pct_change(
        self,
        chains: Union[str, List[str], Dict[Any, str]]
    ) -> pd.Series:
        """
        Calculate percentage change in TVL for specified chains from the first to the last date in the dataset.

        Args:
            chains (str or list or dict): Chain name(s) or dictionary with any keys and chain names as values.

        Returns:
            pd.Series: Series with chain names as index and percent change as values.
        """
        # Normalize chain list
        if isinstance(chains, str):
            chain_list = [chains]
        elif isinstance(chains, dict):
            chain_list = list(chains.values())
        else:
            chain_list = chains

        df_slice = self.df[chain_list]
        start_vals = df_slice.iloc[0]
        end_vals = df_slice.iloc[-1]
        return ((end_vals - start_vals) / start_vals) * 100

    def tvl_groups(self, top_gainers: int = None):
        """
        Group chains into TVL size buckets and optionally return top gainers in each group.

        Args:
            top_gainers (int, optional): If set, returns the top N chains with the highest percentage gain
                in each group. If None, returns the full groups and their counts.

        Returns:
            Union[
                Tuple[Dict[str, List[str]], Dict[str, int]],
                Dict[str, pd.Series]
                If `top_gainers` is None, returns a tuple of group name to chain lists and a count per group.
                If `top_gainers` is set, returns a dictionary of group name to top N gainers by percent change.
        """
        end_vals = self.df.iloc[-1]
        groups = {
            'imba':   end_vals[end_vals > 1_000_000_000].index.tolist(),
            'large': end_vals[(end_vals > 500_000_000) & (end_vals <= 1_000_000_000)].index.tolist(),
            'big': end_vals[(end_vals > 100_000_000) & (end_vals <= 500_000_000)].index.tolist(),
            'medium': end_vals[(end_vals <=  100_000_000) & (end_vals >  50_000_000)].index.tolist(),
            'small':  end_vals[(end_vals <=   50_000_000) & (end_vals >  20_000_000)].index.tolist(),
            'micro':  end_vals[end_vals <= 20_000_000].index.tolist(),
        }
        counts = {grp: len(lst) for grp, lst in groups.items()}

        if top_gainers is None:
            return groups, counts

        top_per_group: Dict[str, pd.Series] = {}
        for grp, names in groups.items():
            if not names:
                top_per_group[grp] = pd.Series(dtype=float)
            else:
                pct = self.pct_change(names)
                top_per_group[grp] = pct.nlargest(top_gainers)
        return top_per_group


def main():
    """
    Command-line interface entry point for ChainsAnalytics.

    Allows users to query TVL changes and groupings over a date range using the CLI.

    Commands:
        pct_change -- Compute percentage change in TVL for specified chains.
        tvl_groups -- Show TVL groups and optionally top N gainers per group.
    """
    parser = argparse.ArgumentParser(description="ChainsAnalytics CLI")
    parser.add_argument('--dbname', help='Database name')
    parser.add_argument('--user', help='Database user')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--host', help='Database host')
    parser.add_argument('--port', help='Database port')
    parser.add_argument('--start_date', required=True, help='Start date YYYY-MM-DD')
    parser.add_argument('--end_date', required=True, help='End date YYYY-MM-DD')
    sub = parser.add_subparsers(dest='command', required=True)

    pc = sub.add_parser('pct_change', help='Compute percentage change')
    pc.add_argument('chains', nargs='+', help='Chain name(s)')

    tg = sub.add_parser('tvl_groups', help='Show TVL groups')
    tg.add_argument('--top_gainers', type=int, help='Top N gainers per group')

    args = parser.parse_args()
    db_conf = {
        'dbname': args.dbname or os.getenv('DB_NAME', 'defillama'),
        'user': args.user or os.getenv('DB_USER', 'postgres'),
        'password': args.password or os.getenv('DB_PASSWORD'),
        'host': args.host or os.getenv('DB_HOST', '185.8.166.8'),
        'port': args.port or os.getenv('DB_PORT', '5432'),
    }

    analytics = ChainsAnalytics(db_config=db_conf)
    analytics.load_data(
        table='chains',
        start_date=args.start_date,
        end_date=args.end_date
    )

    if args.command == 'pct_change':
        chains = args.chains[0] if len(args.chains) == 1 else args.chains
        result = analytics.pct_change(chains)
        print(result)
    elif args.command == 'tvl_groups':
        if args.top_gainers:
            result = analytics.tvl_groups(top_gainers=args.top_gainers)
            print(result)
        else:
            groups, counts = analytics.tvl_groups()
            print("Groups:", groups)
            print("Counts:", counts)


if __name__ == '__main__':
    main()

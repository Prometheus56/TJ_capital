from typing import Generator, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text, URL
from sqlalchemy.engine import Engine
import os

class LoaderForAnalytics:
    def __init__(self):
        self.db_url = URL.create(
            "postgresql+psycopg2",
            username="postgres",
            password= os.getenv("DB_PASSWORD"), 
            host="185.8.166.8",
            database="tj_capital",
        )
    def _make_engine(self, conn_str: str):
        return create_engine(conn_str)

    def load_whole_table(self, table: str):
        engine = self._make_engine(self.db_url)
        
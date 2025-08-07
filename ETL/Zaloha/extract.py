import argparse
import os
import tempfile
import json
from datetime import date
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
import requests


from functools import singledispatch
import pandas as pd
from name_cleaner import clean

def get_data(end_point):
    end_points = {
    'chains':    'https://api.llama.fi/v2/chains',
    'protocols': 'https://api.llama.fi/v2/protocols',
    'dexschains':'https://api.llama.fi/overview/dexs?…',
    'stablecoins':'https://stablecoins.llama.fi/stablecoins?includePrices=true'
    }

    special_cases = {
        'stablecoins' : 'chains',
        'dexschains' : 'protocols',
    }
    end_point = clean(end_point)

    if end_point not in end_points:
        raise ValueError(f"Unknown endpoint: {end_point!r}")

    response = requests.get(end_points.get(end_point))
    response.raise_for_status()
    jsson = response.json()

    if end_point in special_cases:
        data = jsson.get(special_cases[end_point])
        return pd.DataFrame(data)
    else:
        return pd.DataFrame(jsson)
    


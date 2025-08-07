import pandas as pd
import requests
import pandas as pd

# ────────────────  custom functions  ─────────────────
from utils.universal_functions import MrProper

cleaner = MrProper()

def get_defillama_data(end_point) -> pd.DataFrame:
    """
    Download data from defillama API. 
    Special cases must be extracted from nested JSON first.
    """

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
    end_point = cleaner.clean_name(end_point)

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
    

import re
import pandas as pd
from functools import singledispatch

def clean(name: str) -> str:
    if name is None:
        return None
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    return name.strip("_")

@singledispatch
def clean_name(obj):
    raise TypeError(f"Cannot clean names on {type(obj)}")

@clean_name.register
def _(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=clean)

@clean_name.register
def _(ser: pd.Series) -> pd.Series:
    return ser.map(clean)

@clean_name.register
def _(s: str) -> str:
    return clean(s)

@clean_name.register
def _(tpl: tuple) -> tuple:
    return tuple(clean(x) for x in tpl)

@clean_name.register
def _(lst: list) -> list:
    return [clean(x) for x in lst]

@clean_name.register
def _(dic: dict) -> dict:
    return {clean(key) : value for key, value in dic.items()}
    
    








from functools import singledispatch
import numpy as np
import pandas as pd

@singledispatch
def normalize(obj):
    raise TypeError(f"Cannot normalize type: {type(obj)}")

@normalize.register
def _(v: np.float32):
    return float(v)

@normalize.register
def _(v: np.float64):
    return float(v)

@normalize.register
def _(v: np.int32):
    return int(v)

@normalize.register
def _(v: np.int64):
    return int(v)

@normalize.register
def _(d: dict):
    return {k: normalize(v) for k, v in d.items()}

@normalize.register
def _(lst: list):
    return [normalize(x) for x in lst]

for t in (float, int, str, bool):
    @normalize.register(t)
    def _(v: t):  
        return v

@normalize.register(pd.Timestamp)
def _(ts: pd.Timestamp):
    # Convert single Timestamp to ISO-format string
    return ts.strftime('%Y-%m-%d')

# You can also register the Index explicitly (annotation works, but decorator is clearer):

@normalize.register(pd.DatetimeIndex)
def _(idx: pd.DatetimeIndex):
    # Convert the entire index to a list of ISO strings
    return [normalize(ts) for ts in idx]


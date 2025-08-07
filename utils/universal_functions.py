
from .name_cleaner import clean_name
from .dtypes_cleaner import normalize
import pandas as pd

class MrProper:


    def clean_name(self, obj):
        return clean_name(obj)
    
    def clean_date(self, df):
        df = df.copy()
        df['date'] = pd.to_datetime(df['date']).dt.normalize()  
        return df

    def normalize(self, obj):
        return normalize(obj)
        

defillama = MrProper()

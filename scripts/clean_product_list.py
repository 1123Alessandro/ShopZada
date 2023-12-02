import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

pl = pd.read_excel(f'{dir}/Project Dataset/Business Department/product_list.xlsx')

# drop unnamed column
pl = pl.drop(columns=['Unnamed: 0'])

# drop duplicates
pl = pl.drop_duplicates(['product_id'])

# conform product_type
import re

def conform(x):
    return re.sub('[_]', ' ', x)

pt = pl.product_type
pt = pt.mask(~pt.isna(), pt[~pt.isna()].apply(conform))
pl.product_type = pt

pl.to_parquet(f'{dir}/exports/clean_product_list.py')

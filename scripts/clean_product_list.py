import pandas as pd

pl = pd.read_excel('./Project Dataset/Business Department/product_list.xlsx')

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

pl.to_parquet('./exports/clean_product_list.parquet')

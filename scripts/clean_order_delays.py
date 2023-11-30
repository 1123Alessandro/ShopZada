import pandas as pd

df_delays = pd.read_html('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Operations Department/order_delays.html')

#Removing useless Unnamed: 0 column
df_delays = df_delays.drop('Unnamed: 0', axis=1)

df_delays.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/exports/clean_order_delays.parquet')
import pandas as pd

df_delays = pd.read_html('./Project Dataset/Operations Department/order_delays.html')

#Removing useless Unnamed: 0 column
df_delays = df_delays.drop('Unnamed: 0', axis=1)

df_delays.to_parquet('./exports/clean_order_delays.parquet')

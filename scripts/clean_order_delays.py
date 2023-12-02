import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

df_delays = pd.read_html(f'{dir}/Project Dataset/Operations Department/order_delays.html')[0]

#Removing useless Unnamed: 0 column
df_delays = df_delays.drop('Unnamed: 0', axis=1)

df_delays.to_parquet(f'{dir}/exports/clean_order_delays.parquet')

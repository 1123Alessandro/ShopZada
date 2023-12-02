import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

df_item_prices1 = pd.read_csv(f'{dir}/Project Dataset/Operations Department/line_item_data_prices1.csv')
df_item_prices2 = pd.read_csv(f'{dir}/Project Dataset/Operations Department/line_item_data_prices2.csv')
df_item_prices3 = pd.read_parquet(f'{dir}/Project Dataset/Operations Department/line_item_data_prices3.parquet')

df_item_prices = pd.concat([df_item_prices1, df_item_prices2, df_item_prices3])

#Remove Unnamed: 0 column
df_item_prices = df_item_prices.drop('Unnamed: 0', axis=1)

#Standardize quantity
df_item_prices['quantity'] = df_item_prices['quantity'].str.replace('\D', '', regex=True)
df_item_prices['quantity'] = df_item_prices['quantity'].astype(int)

#Dropping duplicates based on order_id, keep no empty values
df_item_prices = df_item_prices.drop_duplicates(subset=['order_id'])

df_item_prices.to_parquet(f'{dir}/exports/clean_line_item_data_prices.parquet')

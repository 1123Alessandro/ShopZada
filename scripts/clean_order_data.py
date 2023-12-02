import pandas as pd
from datetime import date
import os
dir = os.environ.get('AIRFLOW_HOME')

df_order1 = pd.read_parquet(f'{dir}/Project Dataset/Operations Department/order_data_20200101-20200701.parquet')
df_order2 = pd.read_pickle(f'{dir}/Project Dataset/Operations Department/order_data_20200701-20211001.pickle')
df_order3 = pd.read_csv(f'{dir}/Project Dataset/Operations Department/order_data_20211001-20220101.csv')
df_order4 = pd.read_excel(f'{dir}/Project Dataset/Operations Department/order_data_20220101-20221201.xlsx')
df_order5 = pd.read_json(f'{dir}/Project Dataset/Operations Department/order_data_20221201-20230601.json')
df_order6 = pd.read_html(f'{dir}/Project Dataset/Operations Department/order_data_20230601-20240101.html')

#Removing useless Unnamed: 0 column
df_order3 = df_order3.drop('Unnamed: 0', axis=1)
df_order4 = df_order4.drop('Unnamed: 0', axis=1)
df_order6 = df_order6.drop('Unnamed: 0', axis=1)

#Combining all order data
df_orders = pd.concat([df_order1, df_order2, df_order3, df_order4, df_order5, df_order6])

#renaming estimated arrival as estimated_arrival
df_orders.rename(columns={'estimated arrival': 'estimated_arrival'}, inplace=True)

#estimated arrival column made to int data type
df_orders['estimated_arrival'] = df_orders['estimated_arrival'].str.replace('\D', '', regex=True)
df_orders['estimated_arrival'] = df_orders['estimated_arrival'].astype(int)

#Removing transactions that happened in the future
today = str(date.today())
df_orders = df_orders[df_orders['transaction_date'] <= today]

df_orders.to_parquet(f'{dir}/exports/clean_order_data.parquet')

import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

df_order1 = pd.read_parquet(f'{dir}/Project Dataset/Enterprise Department/order_with_merchant_data1.parquet')
df_order2 = pd.read_parquet(f'{dir}/Project Dataset/Enterprise Department/order_with_merchant_data2.parquet')
df_order3 = pd.read_csv(f'{dir}/Project Dataset/Enterprise Department/order_with_merchant_data3.csv')

#Drop unnecessary indexing
df_order3 = df_order3.drop(columns=['Unnamed: 0'])

#Combine all order files
df_orders = pd.concat([df_order1, df_order2, df_order3], ignore_index=True)

df_orders.to_parquet(f'{dir}/exports/clean_order_with_merchant_data.parquet')

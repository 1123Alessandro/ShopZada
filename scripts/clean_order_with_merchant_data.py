import pandas as pd

df_order1 = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Enterprise Department/order_with_merchant_data1.parquet')
df_order2 = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Enterprise Department/order_with_merchant_data2.parquet')
df_order3 = pd.read_csv('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Enterprise Department/order_with_merchant_data3.csv')

#Drop unnecessary indexing
df_order3 = df_order3.drop(columns=['Unnamed: 0'])

#Combine all order files
df_orders = pd.concat([df_order1, df_order2, df_order3], ignore_index=True)

df_orders.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/exports/clean_order_with_merchant_data.parquet')
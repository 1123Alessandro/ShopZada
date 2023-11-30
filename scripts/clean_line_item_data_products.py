import pandas as pd

df_item_products1 = pd.read_csv('./Project Dataset/Operations Department/line_item_data_products1.csv')
df_item_products2 = pd.read_csv('./Project Dataset/Operations Department/line_item_data_products2.csv')
df_item_products3 = pd.read_parquet('./Project Dataset/Operations Department/line_item_data_products3.parquet')

#Combining line item product files
df_item_products = pd.concat([df_item_products1, df_item_products2, df_item_products3])

#Remove Unnamed: 0 column
df_item_products = df_item_products.drop('Unnamed: 0', axis=1)

#Dropping duplicates based on order_id, keep no empty values
df_item_products = df_item_products.drop_duplicates(subset=['order_id'])

df_item_products.to_parquet('./exports/clean_line_item_data_products.parquet')

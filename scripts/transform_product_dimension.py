import pandas as pd

clean_product_list = pd.read_parquet("./exports/clean_product_list.parquet")

product_dimension = clean_product_list

#Renaming Columns
product_dimension = product_dimension.rename(columns={'product_id': 'PRODUCT_ID', 'product_name': 'PRODUCT_NAME', 'price': 'PRODUCT_PRICE', 'product_type': 'PRODUCT_TYPE'})

product_dimension.to_parquet('./exports/product_dimension.parquet')
import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

clean_product_list = pd.read_parquet(f"{dir}/exports/clean_product_list.parquet")

product_dimension = clean_product_list

#Renaming Columns
product_dimension = product_dimension.rename(columns={'product_id': 'PRODUCT_ID', 'product_name': 'PRODUCT_NAME', 'price': 'PRODUCT_PRICE', 'product_type': 'PRODUCT_TYPE'})

product_dimension.to_parquet(f'{dir}/exports/product_dimension.parquet')

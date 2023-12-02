import pandas as pd

clean_product_list = pd.read_parquet("./exports/clean_product_list.parquet")

clean_product_list.to_parquet('./exports/product_dimension.parquet')
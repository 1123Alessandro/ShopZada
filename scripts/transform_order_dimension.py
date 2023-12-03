import pandas as pd

clean_order_data = pd.read_parquet("./exports/clean_order_data.parquet")

order_dimension = clean_order_data

#Remove user_id
order_dimension = order_dimension.drop(columns=['user_id'])

#Renaming Columns
order_dimension = order_dimension.rename(columns={'order_id': 'ORDER_ID', 'estimated_arrival': 'ORDER_ESTIMATED_ARRIVAL', 'transaction_date': 'ORDER_TRANSACTION_DATE'})

order_dimension.to_parquet('./exports/order_dimension.parquet')
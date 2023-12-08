import pandas as pd

clean_order_data = pd.read_parquet("./exports/clean_order_data.parquet")
line_item_data_prices = pd.read_parquet('./exports/clean_line_item_data_prices.parquet')

order_dimension = clean_order_data

#Remove user_id and transaction_date
order_dimension = order_dimension.drop(columns=['user_id', 'transaction_date'])

# merge quantity from line_item_data_prices
order_dimension = order_dimension.merge(line_item_data_prices[['order_id', 'quantity']], on='order_id')

#Renaming Columns
order_dimension = order_dimension.rename(columns={'order_id': 'ORDER_ID', 'estimated_arrival': 'ORDER_ESTIMATED_ARRIVAL', 'quantity': 'ORDER_QUANTITY'})

order_dimension.to_parquet('./exports/order_dimension.parquet')

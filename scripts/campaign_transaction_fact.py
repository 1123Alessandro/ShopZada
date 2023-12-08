import pandas as pd

a = pd.read_parquet('./exports/clean_transactional_campaign_data.parquet')[['campaign_id', 'order_id']]
order_data = pd.read_parquet('./exports/clean_order_data.parquet')
campaign_dimension = pd.read_parquet('./exports/campaign_dimension.parquet')
order_dimension = pd.read_parquet('./exports/order_dimension.parquet')
date_dimension = pd.read_parquet('./exports/date_dimension.parquet')

# merge order_data transaction_date
campaign_transaction_fact = a.merge(order_data[['order_id', 'transaction_date']], on='order_id')

# merge CAMPAIGN dimension
campaign_transaction_fact = campaign_transaction_fact.merge(campaign_dimension.CAMPAIGN_ID, left_on='campaign_id', right_on='CAMPAIGN_ID')

# merge ORDER dimension
campaign_transaction_fact = campaign_transaction_fact.merge(order_dimension.ORDER_ID, left_on='order_id', right_on='ORDER_ID')

# merge DATE dimension
campaign_transaction_fact.transaction_date = pd.to_datetime(campaign_transaction_fact.transaction_date)
campaign_transaction_fact = campaign_transaction_fact.merge(date_dimension.DATE_ID, left_on='transaction_date', right_on='DATE_ID')

# drop extra columns
campaign_transaction_fact = campaign_transaction_fact.drop(columns=['campaign_id', 'order_id', 'transaction_date'])

# create CAMPAIGN_TRANSACTION_FACT_ID
campaign_transaction_fact = campaign_transaction_fact.reset_index().rename(columns={'index': 'CAMPAIGN_TRANSACTION_FACT_ID'})

# export
campaign_transaction_fact.to_parquet('./exports/campaign_transaction_fact.parquet')

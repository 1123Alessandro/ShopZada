import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

a = pd.read_parquet(f'{dir}/exports/clean_order_with_merchant_data.parquet')[['merchant_id', 'order_id', 'staff_id']]
order_data = pd.read_parquet(f'{dir}/exports/clean_order_data.parquet')[['order_id', 'transaction_date']]

merchant_dimension = pd.read_parquet(f'{dir}/exports/merchant_dimension.parquet')
order_dimension = pd.read_parquet(f'{dir}/exports/order_dimension.parquet')
staff_dimension = pd.read_parquet(f'{dir}/exports/staff_dimension.parquet')
date_dimension = pd.read_parquet(f'{dir}/exports/date_dimension.parquet')

# merge order_data transaction_date
merchant_performance_fact = a.merge(order_data, on='order_id')

# merge MERCHANT dimension
merchant_performance_fact = merchant_performance_fact.merge(merchant_dimension.MERCHANT_ID, left_on='merchant_id', right_on='MERCHANT_ID')

# merge ORDER dimension
merchant_performance_fact = merchant_performance_fact.merge(order_dimension.ORDER_ID, left_on='order_id', right_on='ORDER_ID')

# merge STAFF dimension
merchant_performance_fact = merchant_performance_fact.merge(staff_dimension.STAFF_ID, left_on='staff_id', right_on='STAFF_ID')

# merge DATE dimension
merchant_performance_fact.transaction_date = pd.to_datetime(merchant_performance_fact.transaction_date)
merchant_performance_fact = merchant_performance_fact.merge(date_dimension.DATE_ID, left_on='transaction_date', right_on='DATE_ID')

# drop extra columns
merchant_performance_fact = merchant_performance_fact.drop(columns=['merchant_id', 'order_id', 'staff_id', 'transaction_date'])

# create MERCHANT_PERFORMANCE_FACT_ID
merchant_performance_fact = merchant_performance_fact.reset_index().rename(columns={'index': 'MERCHANT_PERFORMANCE_FACT_ID'})

merchant_performance_fact.to_parquet(f'{dir}/exports/merchant_performance_fact.parquet')

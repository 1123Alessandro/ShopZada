import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

a = pd.read_parquet(f'{dir}/exports/clean_order_data.parquet')
line_item_data_products = pd.read_parquet(f'{dir}/exports/clean_line_item_data_products.parquet')
order_with_merchant = pd.read_parquet(f'{dir}/exports/clean_order_with_merchant_data.parquet')
order_delays = pd.read_parquet(f'{dir}/exports/clean_order_delays.parquet')
order_dimension = pd.read_parquet(f'{dir}/exports/order_dimension.parquet')
customer_dimension = pd.read_parquet(f'{dir}/exports/customer_dimension.parquet')
date_dimension = pd.read_parquet(f'{dir}/exports/date_dimension.parquet')
product_dimension = pd.read_parquet(f'{dir}/exports/product_dimension.parquet')
merchant_dimension = pd.read_parquet(f'{dir}/exports/merchant_dimension.parquet')

a.transaction_date = pd.to_datetime(a.transaction_date)

# gather base values and merge with ORDER dimension
sales_fact = pd.merge(a[['order_id', 'user_id', 'transaction_date']], order_dimension.ORDER_ID, left_on='order_id', right_on='ORDER_ID')

# merge with CUSTOMER dimension
sales_fact = sales_fact.merge(customer_dimension.CUSTOMER_ID, left_on='user_id', right_on='CUSTOMER_ID')

# merge with DATE dimension
sales_fact = sales_fact.merge(date_dimension.DATE_ID, left_on='transaction_date', right_on='DATE_ID').drop(columns=['order_id', 'user_id', 'transaction_date'])

# merge with line_item_data_products and drop extra order_id column
sales_fact = sales_fact.merge(line_item_data_products[['order_id', 'product_id']], left_on='ORDER_ID', right_on='order_id').drop(columns=['order_id'])

# merge with PRODUCT dimension
sales_fact = sales_fact.merge(product_dimension.PRODUCT_ID, left_on='product_id', right_on='PRODUCT_ID').drop(columns=['product_id'])

# merge with order_with_merchant and drop extra order_id column
sales_fact = sales_fact.merge(order_with_merchant[['order_id', 'merchant_id']], left_on='ORDER_ID', right_on='order_id').drop(columns=['order_id'])

# merge with MERCHANT dimension
sales_fact = sales_fact.merge(merchant_dimension.MERCHANT_ID, left_on='merchant_id', right_on='MERCHANT_ID').drop(columns=['merchant_id'])

# adding the TOTAL_AMOUNT
sales_fact = sales_fact.merge(order_dimension[['ORDER_ID', 'ORDER_QUANTITY']], on='ORDER_ID')
sales_fact = sales_fact.merge(product_dimension[['PRODUCT_ID', 'PRODUCT_PRICE']], on='PRODUCT_ID')

def getTotal(x):
    return x.ORDER_QUANTITY * x.PRODUCT_PRICE

sales_fact = sales_fact.assign(TOTAL_AMOUNT=sales_fact.apply(getTotal, axis=1))
sales_fact = sales_fact.drop(columns=['ORDER_QUANTITY', 'PRODUCT_PRICE'])

# adding the DELAY_IN_DAYS
sales_fact = sales_fact.merge(order_delays, how='left', left_on='ORDER_ID', right_on='order_id')
sales_fact = sales_fact.drop(columns=['order_id'])
sales_fact = sales_fact.rename(columns={'delay in days': 'DELAY_IN_DAYS'})

# create index column and rename as SALES_FACT_ID
sales_fact = sales_fact.reset_index().rename(columns={'index': 'SALES_FACT_ID'})

sales_fact.to_parquet(f'{dir}/exports/sales_fact.parquet')

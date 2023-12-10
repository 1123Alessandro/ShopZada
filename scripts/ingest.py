import pandas as pd
from sqlalchemy import create_engine
import psycopg2
engine = create_engine('postgresql+psycopg2://shopzada:pass@localhost/shopzada')
import os
dir = os.environ.get('AIRFLOW_HOME')

# ingest dimensions
pd.read_parquet(f'{dir}/exports/order_dimension.parquet').to_sql('order', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/staff_dimension.parquet').to_sql('staff', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/product_dimension.parquet').to_sql('product', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/campaign_dimension.parquet').to_sql('campaign', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/customer_dimension.parquet').to_sql('customer', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/merchant_dimension.parquet').to_sql('merchant', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/date_dimension.parquet').to_sql('date', engine, if_exists='replace', index=False)

# ingest facts
pd.read_parquet(f'{dir}/exports/sales_fact.parquet').to_sql('sales', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/campaign_transaction_fact.parquet').to_sql('campaign transaction', engine, if_exists='replace', index=False)
pd.read_parquet(f'{dir}/exports/merchant_performance_fact.parquet').to_sql('merchant performance', engine, if_exists='replace', index=False)

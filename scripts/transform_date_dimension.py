import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

a = pd.read_parquet(f'{dir}/exports/clean_transactional_campaign_data.parquet')
b = pd.read_parquet(f'{dir}/exports/clean_order_data.parquet')


ab = pd.DataFrame(pd.concat([a.transaction_date, b.transaction_date]))

# clean duplicates
ab = ab.drop_duplicates()

# convert to datetime
ab.transaction_date = pd.to_datetime(ab.transaction_date)

# add columns
ab = ab.assign(DAY_OF_WEEK=ab.transaction_date.dt.dayofweek)
ab = ab.assign(MONTH=ab.transaction_date.dt.month)
ab = ab.assign(YEAR=ab.transaction_date.dt.year)

# rename column/s
ab = ab.rename(columns={'transaction_date': 'DATE_ID'})

# export
ab.to_parquet(f'{dir}/exports/date_dimension.parquet')

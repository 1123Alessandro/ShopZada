import pandas as pd
from datetime import date
import os
dir = os.environ.get('AIRFLOW_HOME')

df_transactional_campaign = pd.read_csv(f'{dir}/Project Dataset/Marketing Department/transactional_campaign_data.csv')

#Remove useless Unnamed: 0 column
df_transactional_campaign = df_transactional_campaign.drop('Unnamed: 0', axis=1)

#Removing transactions that happened in the future
today = str(date.today())
df_transactional_campaign = df_transactional_campaign[df_transactional_campaign['transaction_date'] <= today]

#renaming estimated arrival as estimated_arrival
df_transactional_campaign.rename(columns={'estimated arrival': 'estimated_arrival'}, inplace=True)

#estimated arrival column made to int data type
df_transactional_campaign['estimated_arrival'] = df_transactional_campaign['estimated_arrival'].str.replace('\D', '', regex=True)
df_transactional_campaign['estimated_arrival'] = df_transactional_campaign['estimated_arrival'].astype(int)

df_transactional_campaign.to_parquet(f'{dir}/exports/clean_transactional_campaign_data.parquet')

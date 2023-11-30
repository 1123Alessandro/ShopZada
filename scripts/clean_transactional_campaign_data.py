import pandas as pd
from datetime import date

df_transactional_campaign = pd.read_csv('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Marketing Department/transactional_campaign_data.csv')

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

df_transactional_campaign.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/exports/clean_transactional_campaign_data.parquet')
import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

#separate columns
df_campaign = pd.read_csv(f'{dir}/Project Dataset/Marketing Department/campaign_data.csv', sep='\t')

#Standardize discount
df_campaign['discount'] = df_campaign['discount'].str.replace('\D', '', regex=True)
df_campaign['discount'] = df_campaign['discount']+ '%'

#Remove useless Unnamed: 0 column
df_campaign = df_campaign.drop('Unnamed: 0', axis=1)

df_campaign.to_parquet(f'{dir}/exports/clean_campaign_data.parquet')

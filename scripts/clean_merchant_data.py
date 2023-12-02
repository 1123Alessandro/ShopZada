import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

df_merchant_data = pd.read_html(f'{dir}/Project Dataset/Enterprise Department/merchant_data.html')[0]

#Remove Unnamed: 0 column
df_merchant_data = df_merchant_data.drop('Unnamed: 0', axis=1)

#Remove duplicates based on merchant_id, keep first instance based on creation_date
df_merchant_data = df_merchant_data.sort_values(by=['merchant_id', 'creation_date']).drop_duplicates(subset=['merchant_id'], keep='first')

#Standardize contact_number
df_merchant_data['contact_number'] = df_merchant_data['contact_number'].str.replace('\.', '-', regex=True)
df_merchant_data['contact_number'] = df_merchant_data['contact_number'].str.replace('[^0-9+()-]', '', regex=True)

#Title case for address except for country
df_merchant_data['street'] = df_merchant_data['street'].str.title()
df_merchant_data['state'] = df_merchant_data['state'].str.title()
df_merchant_data['city'] = df_merchant_data['city'].str.title()

df_merchant_data.to_parquet(f'{dir}/exports/clean_merchant_data.parquet')

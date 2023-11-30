import pandas as pd

df_staff_data = pd.read_html('./Project Dataset/Enterprise Department/staff_data.html')

#Remove Unnamed: 0 column
df_staff_data = df_staff_data.drop('Unnamed: 0', axis=1)

#Remove duplicates based on staff_id, keep first instance based on creation_date
df_staff_data = df_staff_data.sort_values(by=['staff_id', 'creation_date']).drop_duplicates(subset=['staff_id'], keep='first')

#Standardize contact_number
df_staff_data['contact_number'] = df_staff_data['contact_number'].str.replace('\.', '-', regex=True)
df_staff_data['contact_number'] = df_staff_data['contact_number'].str.replace('[^0-9+()-]', '', regex=True)

#Standardize strings, use title case except for country
df_staff_data['name'] = df_staff_data['name'].str.title()
df_staff_data['job_level'] = df_staff_data['job_level'].str.title()
df_staff_data['street'] = df_staff_data['street'].str.title()
df_staff_data['state'] = df_staff_data['state'].str.title()
df_staff_data['city'] = df_staff_data['city'].str.title()

df_staff_data.to_parquet('./exports/clean_staff_data.parquet')

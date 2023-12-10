import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')


clean_user_credit_card = pd.read_parquet(f'{dir}/exports/clean_user_credit_card.parquet')
clean_user_data = pd.read_parquet(f'{dir}/exports/clean_user_data.parquet')
clean_user_job = pd.read_parquet(f'{dir}/exports/clean_user_job.parquet')

#Merging DataFrames
customer_dimension = pd.merge(clean_user_credit_card, clean_user_data, on=['user_id', 'name'])
customer_dimension = pd.merge(customer_dimension, clean_user_job, on=['user_id', 'name'])

#Renaming Columns
customer_dimension = customer_dimension.rename(columns={'user_id': 'CUSTOMER_ID', 'name': 'CUSTOMER_NAME', 'street': 'CUSTOMER_STREET', 'state': 'CUSTOMER_STATE', 'city': 'CUSTOMER_CITY', 
                                                        'country': 'CUSTOMER_COUNTRY', 'birthdate': 'CUSTOMER_BIRTH_DATE', 'gender': 'CUSTOMER_GENDER', 'job_title': 'CUSTOMER_JOB_TITLE', 
                                                        'job_level': 'CUSTOMER_JOB_LEVEL', 'creation_date': 'CUSTOMER_REGISTRATION_DATE', 'credit_card_number': 'CUSTOMER_CREDIT_CARD_NUM', 
                                                        'issuing_bank': 'CUSTOMER_CREDIT_CARD_BANK', 'user_type': 'CUSTOMER_TYPE', 'device_address': 'CUSTOMER_DEVICE_ADDRESS'})

customer_dimension.to_parquet(f'{dir}/exports/customer_dimension.parquet')

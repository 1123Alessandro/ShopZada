import pandas as pd

clean_staff_data = pd.read_parquet("./exports/clean_staff_data.parquet")

staff_dimension = clean_staff_data

#Renaming Columns order_dimension = order_dimension.rename(columns={'order_id': 'ORDER_ID', 'estimated_arrival': 'ORDER_ESTIMATED_ARRIVAL', 'transaction_date': 'ORDER_TRANSACTION_DATE'})
staff_dimension = staff_dimension.rename(columns={'staff_id': 'STAFF_ID', 'name': 'STAFF_NAME', 'job_level': 'STAFF_JOB_LEVEL', 'street': 'STAFF_STREET', 'state': 'STAFF_STATE', 
                                                  'city': 'STAFF_CITY', 'country': 'STAFF_COUNTRY', 'contact_number': 'STAFF_CONTACT_NUMBER', 'creation_date': 'STAFF_CREATION_DATE'})

staff_dimension.to_parquet('./exports/staff_dimension.parquet')

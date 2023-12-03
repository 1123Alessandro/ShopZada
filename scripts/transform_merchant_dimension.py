import pandas as pd

clean_merchant_data = pd.read_parquet('./exports/clean_merchant_data.parquet')

merchant_dimension = clean_merchant_data

#Renaming Columns
merchant_dimension = merchant_dimension.rename(columns={'merchant_id': 'MERCHANT_ID', 'name': 'MERCHANT_NAME', 'creation_date': 'MERCHANT_CREATION_DATE', 
                                                        'contact_number': 'MERCHANT_CONTACT_NUMBER', 'street': 'MERCHANT_STREET', 'state': 'MERCHANT_STATE', 'city': 'MERCHANT_CITY', 
                                                        'country': 'MERCHANT_COUNTRY'})

merchant_dimension.to_parquet('./exports/merchant_dimension.parquet')
import pandas as pd

clean_campaign_data = pd.read_parquet("./exports/clean_campaign_data.parquet")

campaign_dimension = clean_campaign_data

#Renaming Columns
campaign_dimension = campaign_dimension.rename(columns={'campaign_id': 'CAMPAIGN_ID', 'campaign_name': 'CAMPAIGN_NAME', 'campaign_description': 'CAMPAIGN_DESCRIPTION',
                                                        'discount': 'DISCOUNT_PERCENTAGE'})

campaign_dimension.to_parquet('./exports/campaign_dimension.parquet')
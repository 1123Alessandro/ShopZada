import pandas as pd

uj = pd.read_csv('./Project Dataset/Customer Management Department/user_job.csv')

# drop unnamed column
uj = uj.drop(columns=['Unnamed: 0'])

# conform names
uj.name = uj.name.str.title()

# drop duplicates
uj = uj.drop_duplicates(['user_id'])

uj.to_parquet('./exports/clean_user_job.parquet')

import pandas as pd

uj = pd.read_csv('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Customer Management Department/user_job.csv')

# drop unnamed column
uj = uj.drop(columns=['Unnamed: 0'])

# conform names
uj.name = uj.name.str.title()

# drop duplicates
uj = uj.drop_duplicates(['user_id'])

uj.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/exports/clean_user_job.parquet')

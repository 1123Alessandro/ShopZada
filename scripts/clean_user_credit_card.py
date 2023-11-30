import pandas as pd

ucc = pd.read_pickle('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/Project Dataset/Customer Management Department/user_credit_card.pickle')

# Conform names
ucc.name = ucc.name.str.title()

# Drop invalid credit card numbers
ucc = ucc.drop(ucc[ucc.credit_card_number < 10000000].index)

# Drop duplicates
ucc = ucc.drop_duplicates(['user_id'])

ucc.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/ShopZada/exports/clean_user_credit_card.parquet')

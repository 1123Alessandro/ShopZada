import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

ucc = pd.read_pickle(f'{dir}/Project Dataset/Customer Management Department/user_credit_card.pickle')

# Conform names
ucc.name = ucc.name.str.title()

# Drop invalid credit card numbers
ucc = ucc.drop(ucc[ucc.credit_card_number < 10000000].index)

# Drop duplicates
ucc = ucc.drop_duplicates(['user_id'])

ucc.to_parquet(f'{dir}/exports/clean_user_credit_card.parquet')

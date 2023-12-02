import pandas as pd
import os
dir = os.environ.get('AIRFLOW_HOME')

ud = pd.read_json(f'{dir}/Project Dataset/Customer Management Department/user_data.json')

# parse to datetime
ud.creation_date = pd.to_datetime(ud.creation_date)

# conform names
ud.name = ud.name.str.title()

import re

def conform(x):
    a = re.search('(?P<country>[a-zA-Z ]+)[,][a-zA-Z ]+', x)
    b = re.fullmatch('(?P<country>[a-zA-Z ]+)[(][a-zA-Z ]+[)]', x)
    if a:
        return a.group('country')
    elif b:
        return b.group('country')
    else: 
        return x

# conform countries
ud.country = ud.country.apply(conform)

# parse to datetime
ud.birthdate = pd.to_datetime(ud.birthdate)

# drop duplicates
ud = ud.drop_duplicates(['user_id'])

ud.to_parquet(f'{dir}/exports/clean_user_data.parquet')

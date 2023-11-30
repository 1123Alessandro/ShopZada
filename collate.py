import pandas as pd

# FOR FUTURE USE:
# clean_campaign_data = pd.read_parquet("./scripts/clean_campaign_data.py")
# clean_line_item_data_prices = pd.read_parquet("./scripts/clean_line_item_data_prices.py")
# clean_line_item_data_products = pd.read_parquet("./scripts/clean_line_item_data_products.py")
# clean_merchant_data = pd.read_parquet("./scripts/clean_merchant_data.py")
# clean_order_data = pd.read_parquet("./scripts/clean_order_data.py")
# clean_order_delays = pd.read_parquet("./scripts/clean_order_delays.py")
# clean_order_with_merchant_data = pd.read_parquet("./scripts/clean_order_with_merchant_data.py")
# clean_product_list = pd.read_parquet("./scripts/clean_product_list.py")
# clean_staff_data = pd.read_parquet("./scripts/clean_staff_data.py")
# clean_transactional_campaign_data = pd.read_parquet("./scripts/clean_transactional_campaign_data.py")
# clean_user_credit_card = pd.read_parquet("./scripts/clean_user_credit_card.py")
# clean_user_data = pd.read_parquet("./scripts/clean_user_data.py")
# clean_user_job = pd.read_parquet("./scripts/clean_user_job.py")

import os
import re

def df(file):
    print(f'Adding {file} ...')

all = {}
for dir, subdirs, files, in os.walk('./exports/'):
    for file in files:
        if re.search('clean', file):
            pp = os.path.join(dir, file).replace('\\', '/')
            df(file)
            all[file] = pd.read_parquet(pp)

print('\n\n\n--------------------------------------------------')
print('Read Complete\nAccess the DataFrame of each file using the `all` dictionary')
print('e.g. all["filename"]')
print('\nbelow are the available keys for access:')

def ls():
    return '\n'.join([f'{i+1}:\t{x}' for i, x in enumerate(all.keys())])

print(ls())

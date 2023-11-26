import os
import re
import pandas as pd
import pyperclip as pc

# with open('test.json', 'w') as file:
#     file.write(s)

def df(file):
    print(f'Adding {file} ...')

def compile():
    a = {}
    for dir, subdirs, files in os.walk('./Project Dataset'):
        for file in files:
            if re.search('.*[.]csv', file):
                df(file)
                a[file] = pd.read_csv(os.path.join(dir, file))
            elif re.search('.*[.]pickle', file):
                df(file)
                a[file] = pd.read_pickle(os.path.join(dir, file))
            elif re.search('.*[.]xlsx', file):
                df(file)
                a[file] = pd.read_excel(os.path.join(dir, file))
            elif re.search('.*[.]json', file):
                df(file)
                a[file] = pd.read_json(os.path.join(dir, file))
            elif re.search('.*[.]html', file):
                df(file)
                a[file] = pd.read_html(os.path.join(dir, file))
            elif re.search('.*[.]parquet', file):
                df(file)
                a[file] = pd.read_parquet(os.path.join(dir, file))
    return a

all = compile()

print('\n\n\n--------------------------------------------------')
print('Read Complete\nAccess the DataFrame of each file using the `all` dictionary')
print('e.g. all["filename"]')
print('\nbelow are the available keys for access:')

def ls():
    return '\n'.join([f'{i+1}:\t{x}' for i, x in enumerate(all.keys())])

print(ls())

import pandas as pd
import os

n = pd.DataFrame([x for x in range(100)])

dir = os.environ.get('AIRFLOW_HOME')

n.to_parquet(f'{dir}/exports/TESTINGGGG.parquet')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'ShopZada',
    default_arts = {
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
    },
    start_date = datetime(2023, 12, 1),
    schedule = timedelta(minutes=10),
    catchup = False,
)



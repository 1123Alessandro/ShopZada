from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os
dir = os.environ.get('AIRFLOW_HOME')


dag = DAG(
    'testing_postgre',
    default_args = {
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
    },
    start_date = datetime(2023, 12, 1),
    schedule = timedelta(minutes=1),
    catchup = False,
)

s1 = PostgresOperator(
    task_id = 'test1',
    postgres_conn_id = 'shopzada',
    sql = f'/test.sql',
    dag=dag
)

# s2 = PostgresOperator()

s1

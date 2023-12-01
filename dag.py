from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'ShopZada',
    default_args = {
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
    },
    start_date = datetime(2023, 12, 1),
    schedule = timedelta(minutes=1),
    catchup = False,
)

t1 = BashOperator (
    task_id = 'testing',
    bash_command = 'python3 "${AIRFLOW_HOME}/scripts/test.py"',
    dag=dag,
)

t2 = BashOperator (
    task_id = 'halo',
    bash_command = 'echo "Hello world"',
    dag=dag,
)

t1 >> t2

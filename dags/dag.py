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

c1 = BashOperator (
	task_id = "clean_campaign_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_campaign_data.py"',
	dag=dag,
)
c2 = BashOperator (
	task_id = "clean_line_item_data_prices",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_line_item_data_prices.py"',
	dag=dag,
)
c3 = BashOperator (
	task_id = "clean_line_item_data_products",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_line_item_data_products.py"',
	dag=dag,
)
c4 = BashOperator (
	task_id = "clean_merchant_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_merchant_data.py"',
	dag=dag,
)
c5 = BashOperator (
	task_id = "clean_order_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_order_data.py"',
	dag=dag,
)
c6 = BashOperator (
	task_id = "clean_order_delays",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_order_delays.py"',
	dag=dag,
)
c7 = BashOperator (
	task_id = "clean_order_with_merchant_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_order_with_merchant_data.py"',
	dag=dag,
)
c8 = BashOperator (
	task_id = "clean_product_list",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_product_list.py"',
	dag=dag,
)
c9 = BashOperator (
	task_id = "clean_staff_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_staff_data.py"',
	dag=dag,
)
c10 = BashOperator (
	task_id = "clean_transactional_campaign_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_transactional_campaign_data.py"',
	dag=dag,
)
c11 = BashOperator (
	task_id = "clean_user_credit_card",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_user_credit_card.py"',
	dag=dag,
)
c12 = BashOperator (
	task_id = "clean_user_data",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_user_data.py"',
	dag=dag,
)
c13 = BashOperator (
	task_id = "clean_user_job",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/clean_user_job.py"',
	dag=dag,
)

[c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13] >> t2

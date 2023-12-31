from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os
dir = os.environ.get('AIRFLOW_HOME')

dag = DAG(
    'ShopZada',
    default_args = {
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
    },
    start_date = datetime(2023, 12, 1),
    schedule = timedelta(minutes=90),
    catchup = False,
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

d1 = BashOperator (
	task_id = "transform_campaign_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_campaign_dimension.py"',
	dag=dag,
)
d2 = BashOperator (
	task_id = "transform_customer_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_customer_dimension.py"',
	dag=dag,
)
d3 = BashOperator (
	task_id = "transform_date_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_date_dimension.py"',
	dag=dag,
)
d4 = BashOperator (
	task_id = "transform_merchant_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_merchant_dimension.py"',
	dag=dag,
)
d5 = BashOperator (
	task_id = "transform_order_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_order_dimension.py"',
	dag=dag,
)
d6 = BashOperator (
	task_id = "transform_product_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_product_dimension.py"',
	dag=dag,
)
d7 = BashOperator (
	task_id = "transform_staff_dimension",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/transform_staff_dimension.py"',
	dag=dag,
)

f1 = BashOperator (
	task_id = "campaign_transaction_fact",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/campaign_transaction_fact.py"',
	dag=dag,
)

f2 = BashOperator (
	task_id = "merchant_perfomance_fact",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/merchant_perfomance_fact.py"',
	dag=dag,
)

f3 = BashOperator (
	task_id = "sales_fact",
	bash_command = 'python3 "${AIRFLOW_HOME}/scripts/sales_fact.py"',
	dag=dag,
)

in1 = BashOperator(
    task_id = 'ingest',
    bash_command = 'python3 "${AIRFLOW_HOME}/scripts/ingest.py"',
    dag=dag,
)

s1 = PostgresOperator(
    task_id = 'association_analysis',
    postgres_conn_id = 'shopzada',
    sql = '/association-analysis.sql',
    dag=dag
)

s2 = PostgresOperator(
    task_id = 'customer_profiling',
    postgres_conn_id = 'shopzada',
    sql = '/customer_profiling.sql',
    dag=dag
)

s3 = PostgresOperator(
    task_id = 'customer_segmentation',
    postgres_conn_id = 'shopzada',
    sql = '/customer_segmentation.sql',
    dag=dag
)

s4 = PostgresOperator(
    task_id = 'merchant_performance',
    postgres_conn_id = 'shopzada',
    sql = '/merchant-performance.sql',
    dag=dag
)

s5 = PostgresOperator(
    task_id = 'successful_campaigns',
    postgres_conn_id = 'shopzada',
    sql = '/successful_campaigns.sql',
    dag=dag
)

s6 = PostgresOperator(
    task_id = 'preferred_banks',
    postgres_conn_id = 'shopzada',
    sql = '/preferred_banks.sql',
    dag=dag
)

s7 = PostgresOperator(
    task_id = 'products_top_performers',
    postgres_conn_id = 'shopzada',
    sql = '/products_top_performers.sql',
    dag=dag
)

d1 << c1

d2 << c11
d2 << c12
d2 << c13

d3 << c10
d3 << c5

d4 << c4

d5 << c5
d5 << c2

d6 << c8

d7 << c9

f1 << c10
f1 << c5
f1 << d1
f1 << d5
f1 << d3

f2 << c7
f2 << c5
f2 << d4
f2 << d5
f2 << d7
f2 << d3

f3 << c5
f3 << c3
f3 << c7
f3 << d5
f3 << d2
f3 << d3
f3 << d6
f3 << d4
f3 << c6

[d1, d2, d3, d4, d5, d6, d7, f1, f2, f3] >> in1

in1 >> [s1, s2, s3, s4, s5, s6, s7]

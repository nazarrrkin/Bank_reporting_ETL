from datetime import datetime, timedelta
import time

from insert_dag import insert_data
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'daniil',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'f101_v2_insert',
    default_args = default_args,
    description = 'Loading edit f101 to DM',
    catchup = False,
    schedule = '0 0 * * *',
    template_searchpath = '/'
 ) as dag:

    ft_balance_f_load = PythonOperator(
        task_id = 'dm_f101_round_f_v2_load',
        python_callable = insert_data,
        op_kwargs = {'schema' : 'DM', 'table_name' : 'dm_f101_round_f_v2'}
    )

    (
        ft_balance_f_load
    )

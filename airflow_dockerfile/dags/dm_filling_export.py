from datetime import datetime, timedelta
import time
import pandas as pd

from insert_dag import logging
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def export_data(table_name):
    logging(f'Starting data export from "DM".{table_name} to /opt/airflow/plugins/project_csv/{table_name}.csv')
    time.sleep(5)

    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
        engine = postgres_hook.get_sqlalchemy_engine()

        query = f'select * from "DM".{table_name}'
        df = pd.read_sql_query(query, engine)
        df.columns = [col.upper() for col in df.columns]

        df.to_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', sep=';', encoding='utf-8', index=False)
        logging(f'Data successfully exported to /opt/airflow/plugins/project_csv/{table_name}.csv')
    except Exception as e:
        logging(f'Error exporting data from "DM".{table_name}: {e}')

default_args = {
    'owner' : 'daniil',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'dm_filling_export',
    default_args = default_args,
    description = 'Filling tables in DM layer, then export to csv',
    catchup = False,
    schedule = '0 0 * * *',
    template_searchpath = '/'
 ) as dag:

    filling_dm_tables = SQLExecuteQueryOperator(
        task_id='filling_dm_tables',
        conn_id='postgres_db',
        sql='/opt/airflow/scripts/dm_filling.sql',
        autocommit=True
    )

    dm_f101_round_f_export = PythonOperator(
        task_id = 'dm_f101_round_f_export',
        python_callable = export_data,
        op_kwargs = {'table_name' : 'dm_f101_round_f'}
    )

    (
        filling_dm_tables >> dm_f101_round_f_export
     )

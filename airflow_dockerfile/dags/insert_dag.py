from datetime import datetime
import pandas

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.configuration import conf

path = Variable.get('create_schema', default_var='/opt/airflow/scripts/')

def insert_data(table_name):
    df = pandas.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';')
    postgres_hook = PostgresHook(postgres_conn_id = 'postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema = 'DS', if_exists = 'append', index = False)

default_args = {
    'owner' : 'daniil',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 2
}

with DAG(
    'insert_data',
    default_args = default_args,
    description = 'Loading data to DS',
    catchup = False,
    schedule = '0 0 * * *',
    template_searchpath = '/'
 ) as dag:

    start = EmptyOperator(
        task_id = 'start'
    )

    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='postgres_db',
        sql='/opt/airflow/scripts/create_schema.sql',
        autocommit=True
    )

    ft_balance_f_load = PythonOperator(
        task_id = 'ft_balance_f_load',
        python_callable = insert_data,
        op_kwargs = {'table_name' : 'ft_balance_f'}
    )

    ft_posting_f_load = PythonOperator(
        task_id = 'ft_posting_f_load',
        python_callable = insert_data,
        op_kwargs = {'table_name' : 'ft_posting_f'}
    )

    md_account_d_load = PythonOperator(
        task_id = 'md_account_d_load',
        python_callable = insert_data,
        op_kwargs = {'table_name' : 'md_account_d'}
    )

    md_currency_d_load = PythonOperator(
        task_id = 'md_currency_d_load',
        python_callable = insert_data,
        op_kwargs = {'table_name' : 'md_currency_d'}
    )

    md_exchange_rate_d_load = PythonOperator(
        task_id = 'md_exchange_rate_d_load',
        python_callable = insert_data,
        op_kwargs = {'table_name' : 'md_exchange_rate_d'}
    )

    md_ledger_account_s_load = PythonOperator(
        task_id = 'md_ledger_account_s_load',
        python_callable = insert_data,
        op_kwargs = {'table_name' : 'md_ledger_account_s'}
    )

    end = EmptyOperator(
        task_id = 'end'
    )

    (
        start >>
        create_schema >>
        [ft_balance_f_load, ft_posting_f_load, md_account_d_load, md_currency_d_load, md_exchange_rate_d_load, md_ledger_account_s_load] >>
        end
    )

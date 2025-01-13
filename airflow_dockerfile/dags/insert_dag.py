from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.configuration import conf

path = Variable.get('create_schema', default_var='/opt/airflow/scripts/')

def insert_data(table_name):
    try:
        df = pd.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';', encoding='cp1252')

    existing_columns = df.columns
    if table_name == 'md_currency_d' and 'CURRENCY_CODE' in existing_columns:
        df = pd.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';', encoding='cp1252', dtype={'CURRENCY_CODE': str})

    df.columns = [col.upper() for col in df.columns]

    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')

    date_columns = postgres_hook.get_records(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE data_type = 'date' AND table_schema = 'DS' AND table_name = '{table_name}';
    """)


    for column in date_columns:
        column_name = column[0].upper()
        df[column_name] = pd.to_datetime(df[column_name], format='%d.%m.%Y', errors='coerce').fillna(
            pd.to_datetime(df[column_name], format='%d-%m-%Y', errors='coerce')
        ).fillna(
            pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='coerce')
        ).dt.strftime('%Y-%m-%d')

    engine = postgres_hook.get_sqlalchemy_engine()

    for index, row in df.iterrows():
        row_dict = row.to_dict()
        try:
            df_row = {col: row_dict[col] for col in df.columns}
            
            df.iloc[[index]].to_sql(table_name, engine, schema='DS', if_exists='append', index=False)
        except Exception as e:
            print(f"Unexpected error for row {index}: {e}")
            continue

    print(f"Data from {table_name} inserted successfully.")

default_args = {
    'owner' : 'daniil',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 2,
    'retry_delay': timedelta(seconds=5)
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

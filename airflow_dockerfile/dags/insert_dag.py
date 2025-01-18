from datetime import datetime, timedelta
import time
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import execute_values

def logging(status):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
    log_query = """
        INSERT INTO "LOGS".logs("STATUS", "TIME")
        VALUES(%s, NOW())"""
    postgres_hook.run(log_query, parameters=(status,))


def insert_data(table_name):
    logging(f'Starting data load to {table_name}')
    time.sleep(5)
    try:
        df = pd.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';', encoding='utf-8')
        logging(f'File {table_name}.csv reading succesfully with encoding UTF-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';', encoding='cp1252')
            logging(f'File {table_name}.csv reading succesfully with encoding CP-1252')
        except Exception as e:
            logging(f"Error reading file {table_name}.csv: {e}")

    existing_columns = df.columns
    if table_name == 'md_currency_d' and 'CURRENCY_CODE' in existing_columns:
        df = pd.read_csv(f'/opt/airflow/plugins/project_csv/{table_name}.csv', delimiter=';', encoding='cp1252', dtype={'CURRENCY_CODE': str})

    df.columns = [col.upper() for col in df.columns]

    postgres_hook = PostgresHook(postgres_conn_id='postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()

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


    primary_key_query = f"""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'DS'
            AND tc.table_name = '{table_name}';
    """
    primary_keys = postgres_hook.get_records(primary_key_query)
    key_columns = [key[0].upper() for key in primary_keys]

    if not key_columns:
        logging(f'There are no unique constraint columns in table {table_name}. Full data will be writen')
        insert_query = f"""
            INSERT INTO "DS"."{table_name}" ({', '.join(f'"{col}"' for col in df.columns)})
            VALUES %s;
        """
    else:
        logging(f'There are several constraint columns in table {table_name}. Rows with similar keys will be overwritten')
        insert_query = f"""
            INSERT INTO "DS"."{table_name}" ({', '.join(f'"{col}"' for col in df.columns)})
            VALUES %s
            ON CONFLICT ({', '.join(f'"{col}"' for col in key_columns)}) DO UPDATE
            SET {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in key_columns)};
        """

    conn = engine.raw_connection()

    try:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False, name=None):
                execute_values(cur, insert_query, [row])
        conn.commit()
        logging(f'Data load succesfully in table {table_name}')
    except Exception as e:
        logging(f'Failed load data in table {table_name} due to error: {e}')
    finally:
        conn.close()

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

    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id='postgres_db',
        sql='/opt/airflow/scripts/create_schema.sql',
        autocommit=True
    )

    start_load = PythonOperator(
        task_id = 'start_load',
        python_callable = logging,
        op_kwargs = {'status' : 'Getting started'}
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

    end_load = PythonOperator(
        task_id = 'end_load',
        python_callable = logging,
        op_kwargs = {'status' : 'End of work'}
    )

    (
        create_schema >>
        start_load >>
        [ft_balance_f_load, ft_posting_f_load, md_account_d_load, md_currency_d_load, md_exchange_rate_d_load, md_ledger_account_s_load] >>
        end_load
    )

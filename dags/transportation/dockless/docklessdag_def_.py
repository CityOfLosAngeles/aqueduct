"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import time, pytz
# Import configuration file
# from config import config as cfg
# Import functions
import data_import
import data_load
import make_tables
import clear_data

pg_conn = BaseHook.get_connection('postgres_default') 
aws_conn = BaseHook.get_connection('aws_default').extra_dejson 
provider_conn = BaseHook.get_connection('provider').extra_dejson

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 9), 
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id = 'dockless-elt',
    default_args=default_args,
    schedule_interval='@daily'
    )

# Task 1: Create tables if not exists
t1 = PythonOperator(
    task_id = 'make_provider_tables',
    python_callable = make_tables.make_tables,
    dag = dag
    )

# TODO: Add clear data task for idempotent constraint

# Create task for each provider / feed
# for provider in cfg.provider:
#     for feed in ['trips', 'status_changes']:
provider = 'lemon'
feed = 'trips'

        # Task 2: Get provider data
        t2 = PythonOperator(
            task_id = 'e_{}_{}'.format(provider, feed),
            provide_context = True, 
            python_callable = data_import.get_provider_data,
            op_kwargs = {
                'provider_name': provider,
                'feed': feed},
            dag = dag
            )

        # Task 3: Clear provider data for time period
        t3 = PythonOperator(
            task_id = 'clear_{}_{}'.format(provider, feed),
            provide_context = True, 
            python_callable = clear_data.clear_data,
            op_kwargs = {
                'provider_name': provider,
                'feed': feed},
            dag = dag
            )

        # Task 3: Upload provider data to db
        t4 = PythonOperator(
            task_id = 'tl_{}_{}'.format(provider, feed),
            provide_context = True,
            python_callable = data_load.load_json,
            op_kwargs = {
                'provider_name': provider,
                'feed': feed},
            dag = dag)

        t1 >> t2 >> t3 >> t4

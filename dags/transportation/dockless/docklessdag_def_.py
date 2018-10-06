"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import datetime, time, pytz
# Import configuration file
from config import config as cfg
# Import functions
import data_import
import data_load
import make_tables


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 12), # First full day of Waze Data
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
    default_args=default_args)

# Testing Time Range: Sept 15 @ 1pm - 3pm
tz = pytz.timezone("US/Pacific")
start_time = tz.localize(datetime.datetime(2018, 9, 15, 13))
end_time = tz.localize(datetime.datetime(2018, 9, 15, 15))

# Task 1: Create tables if not exists
t1 = PythonOperator(
    task_id = 'make_provider_tables',
    provide_context = True,
    python_callable = make_tables.make_tables,
    dag = dag
    )

# Create task for each provider / feed
for provider in cfg.provider:
    for feed in ['trips', 'status_changes']:

        # Task 2: Validate


        # Task 3: Get provider data
        t2 = PythonOperator(
            task_id = 'e_{}_{}'.format(provider, feed),
            provide_context = True, 
            python_callable = data_import.get_provider_data,
            op_kwargs = {
                'provider': provider,
                'feed': feed,
                'start_time': start_time,
                'end_time': end_time},
            dag = dag
            )

        # Task 4: Upload provider data to db
        t3 = PythonOperator(
            task_id = 'tl_{}_{}'.format(provider, feed),
            provide_context = True,
            python_callable = data_load.load_json,
            op_kwargs = {
                'provider': provider,
                'feed': feed,
                'start_time': start_time,
                'end_time': end_time},
            dag = dag)

        t1 >> t2 >> t3

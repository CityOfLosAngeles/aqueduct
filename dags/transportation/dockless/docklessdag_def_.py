"""
DAG for ETL Processing of Dockless Mobility Provider Data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
# Import configuration file
from config import config as cfg


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 12), # First full day of Waze Data
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id = 'dockless-elt',
    default_args=default_args)


# Task 1: Get provider data
t1 = PythonOperator(
    task_id='e_provider',
    provide_context=True, # what does this mean?
    python_callable=get_provider_data,
    op_kwargs={
        'provider': provider,
        'feed': feed,
        'start_time': start_time,
        'end_time': end_time},
    dag=dag
    )

# Task 2: Upload provider data to db
t2 = PythonOperator(
    task_id='tl_provider',
    provide_context=True,
    python_callable=load_json,
    op_kwargs={
        'provider': provider,
        'feed': feed,
        'start_time': start_time,
        'end_time': end_time},
    dag=dag)

# TODO: Create fun to loop through providers
# and create task for each


# interesting...
t1 = S3KeySensor(
    task_id='s3_file_test',
    poke_interval=0,
    timeout=10,
    soft_fail=True,
    bucket_key='s3://scripted-waze-data-525978535215-test/',
    bucket_name=None,
    dag=dag)


t1 >> t2



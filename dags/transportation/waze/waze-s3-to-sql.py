"""
DAG for Waze Data to move to SQL Data Warehouse from Amazon S3
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
"""

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

dag = DAG('waze-s3-to-sql', default_args=default_args)

t1 = S3KeySensor(
    task_id='s3_file_test',
    poke_interval=0,
    timeout=10,
    soft_fail=True,
    bucket_key='s3://scripted-waze-data-525978535215-test/',
    bucket_name=None,
dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)


t2.set_upstream(t1)
"""

"""
DAG for Waze Data to move to SQL Data Warehouse from Amazon S3
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 12), # First full day of Waze Data
    'email': ['hunter.owens@lacity.org','chelsea.ursaner@lacity.org','alex.pudlin@lacity.org','brendan.bailey@lacity.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'scheduler_interval': '@daily'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('racer-nightly-to-s3', default_args=default_args)

def download_google_sheet(ds, **kwargs):
    '''This is a function that will run within the DAG execution'''
    print("running google sheet")
    df = pd.read_csv('https://docs.google.com/spreadsheets/d/1vYg2lvDL9Q4ddzUgKVTvTyCUcF2EgnNsxNwffHo28lo/export?gid=0&format=csv')
    execution_date = kwargs['execution_date'].date()
    df.to_csv('~/shelters-' + str(execution_date) + '.csv')
    return df 

run_this = PythonOperator(
    task_id='download_google_sheet',
    provide_context=True,
    python_callable=download_google_sheet,
    dag=dag)

"""
DAG for Waze Data to move to SALUS from REC and Parks FTP Site
"""
from airflow import DAG
import airflow.operators
import ftplib
import pandas as pd
import arcgis
import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2018, 7, 23), # First full day of FTP Data. 
    'email': ['hunter.owens@lacity.org','alex.pudlin@lacity.org','brendan.bailey@lacity.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15),
    'scheduler_interval': '@daily'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('rap-nightly-to-salus', default_args=default_args)

def get_file(ds, **kwargs):
    hook = airflow.operators.FTPHook(ftp_conn_id = 'rec-park-salus')
    hook.retrieve_file('/some-path-to-file/', 'local/path')
    return ret

def correct_file(ds, **kwargs):
    df = pd.read_csv('local_path')
    df.loc[df["GeoLong"] > 0, "GeoLong"] *= -1
    df.to_csv('local_path', index = False)
    return

def update_arcgis(ds, **kwargs):
    site = ' '# airflow.config
    user =  ' ' # airflow.config
    password =  ' ' # airflow.config
    feature_id = ' ' # airflow.config
    gis = arcgis.GIS(site, user, password)
    gis_item = gis.content.get(feature_id)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)
    return

t1 = airflow.operators.PythonOperator(
    task_id='get_file_from_ftp',
    provide_context=True,
    python_callable=get_file,
    dag=dag
)
t2 = airflow.operators.PythonOperator(
    task_id='correct_file',
    provide_context=True,
    python_callable=correct_file,
    dag=dag
)
t3 = airflow.operators.PythonOperator(
    task_id='update_arcgis',
    provide_context=True,
    python_callable=update_arcgis,
    dag=dag
)

t1 << t2 << t3

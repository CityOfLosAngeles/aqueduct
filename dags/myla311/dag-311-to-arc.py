import pandas as pd
import arcgis 
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import geopandas as gpd 
import os 
import shutil
import pathlib

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def overwrite_layer(**kwargs):
    """
    Overwrites the layer 
    on a ESRI GIS
    """
    print('logging into ESRI')

    gis = GIS("http://lahub.maps.arcgis.com/home/index.html", 
            os.environ.get('AIRFLOW_CONN_LAHUB_USERNAME'),
            os.environ.get('AIRFLOW_CONN_LAHUB_PASSWORD'))
    # get the item 
    item=gis_item = gis.content.get('0e7e2be182e54142be89c2488f25296b')
    print(f'item is {item}')
    print("overwriting")
    # overwrite the file 
    feature_layer_collection = FeatureLayerCollection.fromitem(item)
    large_path = '/tmp/service_requests.zip'
    print(feature_layer_collection.manager.overwrite(large_path))
    return True

default_args = {
    'owner': 'hunterowens',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 10),
    'email': ['ITAData@lacity.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('311-sync', default_args=default_args, schedule_interval=timedelta(days=1))

t1 = BashOperator(
    task_id='download_data',
    bash_command="bash download-data.sh",
    dag=dag)

t2 = PythonOperator(
    task_id="upload_to_esri", 
    provide_context=True,
    python_callable=overwrite_layer,
    dag=dag
)

t1 >> t2
## add postgres insert task here 


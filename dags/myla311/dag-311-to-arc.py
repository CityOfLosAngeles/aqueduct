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
            os.environ.get('LAHUB_USERNAME'),
            os.environ.get('LAHUB_PASSWORD'))
    # get the item 

    my_content = gis.content.search(query="311_test owner:" + gis.users.me.username, 
                                item_type="Feature Layer", 
                                max_items=15)
    item=my_content[0]
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
    task_id='download_date',
    bash_command='',
    dag=dag)

t2 = BashOperator(
    task_id='make_directory',
    bash_command="mkdir -p /tmp/service-requests",
    dag=dag)

ogr_command = """ogr2ogr \ 
                 -s_srs EPSG:4326 \
                 -t_srs EPSG:4326  \
                 -f "ESRI Shapefile" \
                 /tmp/service-requests/service-requests.shp DOWNLOADED_FILE.CSV
                 """

t3 = BashOperator(
    task_id='covert_to_shp',
    bash_command=ogr_command,
    dag=dag)
)

t4 = BashOperator(
    task_id='zip_file',
    bash_command='zip -r /tmp/service_requests.zip /tmp/service_requests/*'
)

t5 = PythonOperator(
    task_id="upload_to_esri", 
    provide_context=True,
    python_callable=overwrite_layer,
    dag=dag
)

t1 >> t2 >> t3 >> t4

t4 >> t5

## add postgres insert task here 


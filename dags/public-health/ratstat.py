import arcgis 
import os 
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import base_hook
from datetime import datetime, timedelta
import logging
pg_conn = BaseHook.get_connection('postgres_default') 


RAT_STAT_ID = "9e7caafab26b490e8ad7f02e5518758c"

def ratstat_loader():
    """
    load the ratstat data 
    into a postgres DB
    """ 


    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = arcgis.gis.GIS(url='https://lahub.maps.arcgis.com',
                         username=arcuser,
                         password=arcpassword))
    rat_stat_group = arcgis.gis.Group(gis, 
                                      RAT_STAT_ID) #this is the groupid of the ratstat group       
    logging.info("Logged into hub and accessed rat stat group.")
    content = rat_stat_group.content()
    feature_layers = []
    for item in content:
        try:
            for layer in item.layers:
                feature_layers.append(layer)
        except KeyError: 
            pass
    dfs = {layer.properties['name']: layer.query(as_df=True, as_shapely=True) for layer in feature_layers}
    logging.info(f"Parsed {dfs.keys} into Pandas")
    logging.info("Connecting to DB")
    user = pg_conn.login
    password = pg_conn.get_password()
    host = pg_conn.host
    dbname = pg_conn.schema
    logging.info(f"Logging into postgres://-----:----@{host}:5432/{dbname}")
    conn = create_engine(f'postgres://{user}:{password}@{host}:5432/{dbname}'')
    for k,table in dfs.items(): 
        table['SHAPE'] = str(table['SHAPE'])
        table.to_sql(k,
                     conn, 
                     schema='public-health',
                     if_exists='replace')
    return True

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 30),
    "email": ["hunter.owens@lacity.org", "ian.rose@lacity.org", "anthony.lyons@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=2),
}             

dag = DAG('ratstat-loader', default_args=default_args, schedule_interval="@daily")

t1 = PythonOperator(
    task_id='ratstat-loader',
    provide_content=True,
    python_callable=ratstat_loader, 
    dag=dag
)
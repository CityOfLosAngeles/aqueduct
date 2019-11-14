"""
Pull data from RAP FTP and sync with ArcGIS Online
"""
import pandas as pd
import ftplib
import arcgis

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def get_file(ftphost, ftpuser, ftppassword, filename):
	ftp = ftplib.FTP(host = ftphost)
	ftp.login(user = ftpuser, passwd = ftppassword)
	ftp.cwd("Upload")
	ftp.retrbinary('RETR %s' % filename, open(filename, 'wb').write)
    
def correct_file(filename):
	df = pd.read_csv(filename)
	if "GeoLong" in df.columns:
		df.loc[df["GeoLong"] > 0, "GeoLong"] *= -1
	df["DummyField"] = 1
	df.to_csv(filename, index = False)

def update_arcgis(arcuser, arcpassword, arcfeatureid, filename):
	gis = arcgis.GIS("http://lahub.maps.arcgis.com", arcuser, arcpassword)
	gis_item = gis.content.get(arcfeatureid)
	gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
	gis_layer_collection.manager.overwrite(filename)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 14),
    "email": ["ian.rose@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG("rap-data", default_args=default_args, schedule_interval="@daily")


def update_rap_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    #Collecting data from FTP site
    ftphost = kwargs.get("ftphost")
    ftpuser = kwargs.get("ftpuser")
    ftppassword = kwargs.get("ftppassword")
    filename = kwargs.get("filename")
    get_file(ftphost, ftpuser, ftppassword, filename)
    
    #Correcting issues with file
    correct_file(filename)
    
    #Updating ArcGis
    arcuser = kwargs.get("arcuser")
    arcpassword = kwargs.get("arcpassword")
    arcfeatureid = kwargs.get("arcfeatureid")
    update_arcgis(arcuser, arcpassword, arcfeatureid, filename)

#Sync ServiceRequestData.csv
t1 = PythonOperator(
    task_id="update_RAP_ServiceRequestData",
    provide_context=True,
    python_callable=update_rap_data,
    op_kwargs={
        "ftphost": "replace", #Replace with FTP Host Data
        "ftpuser": "replace", #Replace with FTP User Data
        "ftppassword": "replace", #Replace with FTP Password Data
        "filename": "ServiceRequestData.csv",
        "arcuser":"replace", #Replace with AGOL User Data
        "arcpassword": "replace", #Replace with AGOL Password Data
        "arcfeatureid": '96cd30870acd4c8f8999c734dfc5b480'
    },
    dag=dag,
)

#Sync raphrsrstats.csv
t2 = PythonOperator(
    task_id="update_raphrsrstats",
    provide_context=True,
    python_callable=update_rap_data,
    op_kwargs={
        "ftphost": "replace", #Replace with FTP Host Data
        "ftpuser": "replace", #Replace with FTP User Data
        "ftppassword": "replace", #Replace with FTP Password Data
        "filename": "raphrsrstats.csv",
        "arcuser":"replace", #Replace with AGOL User Data
        "arcpassword": "replace", #Replace with AGOL Password Data
        "arcfeatureid": '7fe1ef026bbe4bf9a07119eb134fb6cf'
    },
    dag=dag,
)

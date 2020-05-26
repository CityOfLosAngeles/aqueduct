"""
Pull data from Bed Availability Google Sheet and upload to ESRI
"""
import datetime
import os
import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

bucket_name = "public-health-dashboard"


def get_data(filename, workbook):
    df = pd.read_csv(workbook)
    df.to_csv(filename, index=False)
    # df.to_parquet(f"s3://{bucket_name}/jhu_covid19/hospital-availability.parquet")


def update_arcgis(arcuser, arcpassword, arcfeatureid, filename):
    gis = arcgis.GIS("http://lahub.maps.arcgis.com", arcuser, arcpassword)
    gis_item = gis.content.get(arcfeatureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)
    os.remove(filename)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 4, 28),
    "email": [
        "ian.rose@lacity.org",
        "hunter.owens@lacity.org",
        "brendan.bailey@lacity.org",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(days=1),
}

dag = DAG(
    "bed-availability-data", default_args=default_args, schedule_interval="@hourly"
)


def update_bed_availability_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    # Getting data from google sheet
    filename = kwargs.get("filename")
    workbook = kwargs.get("workbook")
    get_data(filename, workbook)

    # Updating ArcGis
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    arcfeatureid = kwargs.get("arcfeatureid")
    update_arcgis(arcuser, arcpassword, arcfeatureid, filename)


# Sync ServiceRequestData.csv
t1 = PythonOperator(
    task_id="update_bed_availability_data",
    provide_context=True,
    python_callable=update_bed_availability_data,
    op_kwargs={
        "filename": "/tmp/Bed_Availability_Data.csv",
        "arcfeatureid": "956e105f422a4c1ba9ce5d215b835951",
        "workbook": "https://docs.google.com/spreadsheets/d/"
        "1rS0Vt-kuxwQKoqZBcaOYOOTc5bL1QZqAqqPSyCaMczQ/"
        "export?format=csv&#gid=156644705",
    },
    dag=dag,
)

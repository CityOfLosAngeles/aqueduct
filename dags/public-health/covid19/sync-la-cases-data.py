"""Pull data from DAILY COVID TABLE for
Mayor's Office Daily COVID-19 Report and upload to ESRI"""
import datetime
import os
import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

bucket_name = "public-health-dashboard"


def get_data(filename, workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name)
    df = df.loc[:, ["Date", "City of LA Cases", "City of LA New Cases"]]
    df.dropna(
        subset=["City of LA Cases", "City of LA New Cases"], how="all", inplace=True
    )
    df["City of LA Cases"] = df["City of LA Cases"].astype(int)
    df["City of LA New Cases"] = df["City of LA New Cases"].astype(int)
    df.to_csv(filename, index=False)
    # df.to_parquet(f"s3://{bucket_name}/jhu_covid19/city-of-la-cases.parquet")


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

dag = DAG("la-cases-data", default_args=default_args, schedule_interval="@hourly")


def update_la_cases_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    # Getting data from google sheet
    filename = kwargs.get("filename")
    workbook = kwargs.get("workbook")
    sheet_name = kwargs.get("sheet_name")
    get_data(filename, workbook, sheet_name)

    # Updating ArcGis
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    arcfeatureid = kwargs.get("arcfeatureid")
    update_arcgis(arcuser, arcpassword, arcfeatureid, filename)


# Sync ServiceRequestData.csv
t1 = PythonOperator(
    task_id="update_la_cases_data",
    provide_context=True,
    python_callable=update_la_cases_data,
    op_kwargs={
        "filename": "/tmp/LA_Cases_Table_V2.csv",
        "arcfeatureid": "1d1e4679a94e43e884b97a0488fc04cf",
        "workbook": "https://docs.google.com/spreadsheets/d/"
        "1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/"
        "export?format=xlsx&#gid=0",
        "sheet_name": "CASE_DATA",
    },
    dag=dag,
)

"""
Pull data from MOPS COVID Dashboard and upload to ESRI
"""
import datetime
import pytz
import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator


def get_data(filename, workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name, skiprows=1, index_col=0)
    df = df.T
    df = df.iloc[:, 1:3]
    df.reset_index(level=0, inplace=True)
    df.columns = ["Date", "Performed", "Test Kit Inventory"]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.loc[
        df["Date"].dt.date
        < datetime.datetime.now()
        .astimezone(pytz.timezone("America/Los_Angeles"))
        .date()
    ]
    df.sort_values("Date", inplace=True)
    cumulative = []
    temp_value = 0
    for i in df["Performed"]:
        temp_value += i
        cumulative.append(temp_value)
    df["Cumulative"] = cumulative
    df["Performed"] = df["Performed"].astype(int)
    df["Cumulative"] = df["Cumulative"].astype(int)
    df.to_csv("/tmp/%s" % filename, index=False)


def update_arcgis(arcuser, arcpassword, arcfeatureid, filename):
    filename = "/tmp/%s" % filename
    gis = arcgis.GIS("http://lahub.maps.arcgis.com", arcuser, arcpassword)
    gis_item = gis.content.get(arcfeatureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 4, 22),
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

cron_string = "0 0-6,18-23 * * *"

dag = DAG(
    "covid-testing-data", default_args=default_args, schedule_interval=cron_string
)


def update_covid_testing_data(**kwargs):
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
    task_id="update_COVID_Testing_Data",
    provide_context=True,
    python_callable=update_covid_testing_data,
    op_kwargs={
        "filename": "COVID_testing_data.csv",
        "arcfeatureid": "64b91665fef4471dafb6b2ff98daee6c",
        "workbook": "https://docs.google.com/spreadsheets/d/"
        "1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/"
        "export?format=xlsx&#gid=0",
        "sheet_name": "DUPLICATE OF MOPS",
    },
    dag=dag,
)

"""
Pull data from MOPS COVID Dashboard and upload to ESRI.
"""
import datetime
import os
import pytz
import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

# County totals
LA_COUNTY_TESTS_FEATURE_ID = "64b91665fef4471dafb6b2ff98daee6c"
# City and county totals
LA_CITY_TESTS_FEATURE_ID = "996a863e59f04efdbe33206a6c717afb"

bucket_name = "public-health-dashboard"


def get_county_data(filename, workbook, sheet_name):
    df = pd.read_excel(workbook, sheet_name=sheet_name, skiprows=1, index_col=0)
    df = df.T

    select_rows = [1, 2, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    df = df.iloc[:, select_rows]

    df.reset_index(level=0, inplace=True)

    column_names = [
        "Date",
        "Performed",
        "Test Kit Inventory",
        "Hotkin_Memorial",
        "Hansen_Dam",
        "Crenshaw_Christian",
        "VA_Lot15",
        "Lincoln_Park",
        "West_Valley_Warner",
        "Carbon_Health_Echo_Park",
        "Kedren_Health",
        "Baldwin_Hills_Crenshaw",
        "Northridge_Fashion",
        "Nursing_Home_Outreach",
        "Homeless_Outreach",
    ]

    df.columns = column_names

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[
        df.Date.dt.date
        < datetime.datetime.now()
        .astimezone(pytz.timezone("America/Los_Angeles"))
        .date()
    ].sort_values("Date")

    # Fill in missing values with 0's for city sites
    city_sites = [
        "Hotkin_Memorial",
        "Hansen_Dam",
        "Crenshaw_Christian",
        "VA_Lot15",
        "Lincoln_Park",
        "West_Valley_Warner",
        "Carbon_Health_Echo_Park",
        "Kedren_Health",
        "Baldwin_Hills_Crenshaw",
        "Northridge_Fashion",
        "Nursing_Home_Outreach",
        "Homeless_Outreach",
    ]

    df[city_sites] = df[city_sites].fillna(0).astype(int)

    df = df.assign(City_Performed=df[city_sites].astype(int).sum(axis=1),)

    # Calculate cumulative sums for whole county and city
    keep_cols = ["Date", "Performed", "Cumulative", "City_Performed", "City_Cumulative"]

    df = df.assign(
        Performed=df.Performed.astype(int),
        Cumulative=df.sort_values("Date")["Performed"].cumsum().astype(int),
        City_Cumulative=df.sort_values("Date")["City_Performed"].cumsum().astype(int),
    )[keep_cols].sort_values("Date")

    return df


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

    df = get_county_data(filename, workbook, sheet_name)
    df.drop(columns=["City_Performed", "City_Cumulative"]).to_csv(
        "/tmp/county_cumulative.csv", index=False
    )

    # Updating ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    filename = "/tmp/county_cumulative.csv"
    gis_item = gis.content.get(LA_COUNTY_TESTS_FEATURE_ID)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)

    # Clean up
    os.remove(filename)


def update_covid_testing_city_county_data(**kwargs):
    """
    The actual python callable that Airflow schedules.
    """
    # Getting data from google sheet
    filename = kwargs.get("filename")
    workbook = kwargs.get("workbook")
    sheet_name = kwargs.get("sheet_name")

    df = get_county_data(filename, workbook, sheet_name)
    df.to_csv("/tmp/county_city_cumulative.csv", index=False)

    # Updating ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    filename = "/tmp/county_city_cumulative.csv"
    gis_item = gis.content.get(LA_CITY_TESTS_FEATURE_ID)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)

    # Clean up
    os.remove(filename)


# Sync ServiceRequestData.csv
t1 = PythonOperator(
    task_id="update_COVID_Testing_Data",
    provide_context=True,
    python_callable=update_covid_testing_data,
    op_kwargs={
        "filename": "COVID_testing_data.csv",
        "workbook": "https://docs.google.com/spreadsheets/d/"
        "1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/"
        "export?format=xlsx&#gid=0",
        "sheet_name": "DUPLICATE OF MOPS",
    },
    dag=dag,
)


t2 = PythonOperator(
    task_id="update_COVID_Testing_City_County_Data",
    provide_context=True,
    python_callable=update_covid_testing_city_county_data,
    op_kwargs={
        "filename": "COVID_testing_data.csv",
        "workbook": "https://docs.google.com/spreadsheets/d/"
        "1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/"
        "export?format=xlsx&#gid=0",
        "sheet_name": "DUPLICATE OF MOPS",
    },
    dag=dag,
)

t1 > t2

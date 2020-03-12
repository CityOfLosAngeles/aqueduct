import os
from datetime import datetime, timedelta

import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

DEATHS_URL = (
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/"
    "master/csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_19-covid-Deaths.csv"
)

CASES_URL = (
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/"
    "master/csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_19-covid-Confirmed.csv"
)

RECOVERED_URL = (
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/"
    "master/csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_19-covid-Recovered.csv"
)

time_series_featureid = "20271474d3c3404d9c79bed0dbd48580"
current_featureid = "191df200230642099002039816dc8c59"


def parse_columns(df):
    """
    quick helper function to parse columns into values
    uses for pd.melt
    """

    columns = list(df.columns)

    id_vars, dates = [], []

    for c in columns:
        if c.endswith("20"):
            dates.append(c)
        else:
            id_vars.append(c)
    return id_vars, dates


def load_jhu_to_esri(**kwargs):
    """
    A single python ETL function.

    Loads the JHU data, transforms it so we are happy
    with it, and then creates the appropriate featureLayer

    Ref:
    """
    cases = pd.read_csv(CASES_URL)
    deaths = pd.read_csv(DEATHS_URL)
    recovered = pd.read_csv(RECOVERED_URL)
    # melt cases
    id_vars, dates = parse_columns(cases)
    df = pd.melt(
        cases,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_cases",
        var_name="date",
    )

    # melt deaths
    id_vars, dates = parse_columns(deaths)
    deaths_df = pd.melt(
        deaths, id_vars=id_vars, value_vars=dates, value_name="number_of_deaths"
    )

    # melt recovered

    id_vars, dates = parse_columns(deaths)
    recovered_df = pd.melt(
        recovered, id_vars=id_vars, value_vars=dates, value_name="number_of_recovered"
    )

    # join
    df["number_of_deaths"] = deaths_df.number_of_deaths
    df["number_of_recovered"] = recovered_df.number_of_recovered
    # make some simple asserts to assure that the data structures haven't changed and
    # some old numbers are still correct
    assert (
        df.loc[(df["Province/State"] == "Los Angeles, CA") & (df["date"] == "3/11/20")][
            "number_of_cases"
        ].iloc[0]
        == 27
    )
    assert (
        df.loc[(df["Province/State"] == "Los Angeles, CA") & (df["date"] == "3/11/20")][
            "number_of_deaths"
        ].iloc[0]
        == 1
    )
    assert (
        df.loc[(df["Province/State"] == "Shanghai") & (df["date"] == "3/11/20")][
            "number_of_recovered"
        ].iloc[0]
        == 320
    )

    # Output to CSV
    time_series_filename = "/tmp/jhu_covid19_time_series.csv"
    df.to_csv(time_series_filename, index=False)

    # Also output the most current date as a separate CSV for convenience
    most_recent_date_filename = "/tmp/jhu_covid19_current.csv"
    current_df = df.assign(date=pd.to_datetime(df.date))
    current_df[current_df.date == current_df.date.max()].to_csv(
        most_recent_date_filename, index=False
    )

    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # Overwrite the existing layers
    gis_item = gis.content.get(time_series_featureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(time_series_filename)

    gis_item = gis.content.get(current_featureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(most_recent_date_filename)

    # Clean up
    os.remove(time_series_filename)
    os.remove(most_recent_date_filename)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 1),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

dag = DAG("jhu-to-esri", default_args=default_args, schedule_interval="@daily")


# Sync ServiceRequestData.csv
t1 = PythonOperator(
    task_id="sync-jhu-to-esri",
    provide_context=True,
    python_callable=load_jhu_to_esri,
    op_kwargs={},
    dag=dag,
)

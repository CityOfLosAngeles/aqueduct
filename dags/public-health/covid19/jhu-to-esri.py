"""
ETL for COVID-19 Data.
Pulls from Johns-Hopkins CSSE data as well as local government agencies.
"""
import logging
import os
from datetime import datetime, timedelta

import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

# This ref is to the last commit that JHU had before they
# switched to not providing county-level data. We use it
# below to backfill some case counts in a county-level time series.
# JHU_BRANCH = "35a425a1da92c3a05c9b33341a9ca154c1e47b07"

# URL to JHU confirmed cases time series.
CASES_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_confirmed_global.csv"
)

# URL to JHU deaths time series.
DEATHS_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_deaths_global.csv"
)

# URL to JHU recoveries time series
RECOVERED_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/"
    "time_series_covid19_recovered_global.csv"
)

# Feature IDs for state/province level time series and current status
jhu_time_series_featureid = "20271474d3c3404d9c79bed0dbd48580"
jhu_current_featureid = "191df200230642099002039816dc8c59"

max_record_count = 6_000_000

# The date at the time of execution. We choose midnight in the US/Pacific timezone,
# but then convert to UTC since that is what AGOL expects. When the feature layer
# is viewed in a dashboard it is converted back to local time.
date = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")


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


def coerce_integer(df):
    """
    Coerce nullable columns to integers for CSV export.

    TODO: recent versions of pandas (>=0.25) support nullable integers.
    Once we can safely upgrade, we should use those and remove this function.
    """

    def integrify(x):
        return int(float(x)) if not pd.isna(x) else None

    cols = [
        "cases",
        "deaths",
        "recovered",
    ]
    new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}
    return df.assign(**new_cols)


def load_jhu_global_time_series(branch="master"):
    """
    Loads the JHU global timeseries data, transforms it so we are happy with it.
    """
    cases = pd.read_csv(CASES_URL.format(branch))
    deaths = pd.read_csv(DEATHS_URL.format(branch))
    recovered = pd.read_csv(RECOVERED_URL.format(branch))
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
    deaths_df = pd.melt(deaths, id_vars=id_vars, value_vars=dates, value_name="deaths")

    # melt recovered
    id_vars, dates = parse_columns(recovered)
    recovered_df = pd.melt(
        recovered, id_vars=id_vars, value_vars=dates, value_name="recovered"
    )

    # join
    df = df.assign(
        number_of_deaths=deaths_df.deaths, number_of_recovered=recovered_df.recovered,
    )

    return df.sort_values(["date", "Country/Region", "Province/State"]).reset_index(
        drop=True
    )


def load_global_covid_data():
    """
    Load global COVID-19 data from JHU.
    """
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    df = load_jhu_global_time_series()

    df = df.assign(
        number_of_cases=pd.to_numeric(df.number_of_cases),
        number_of_deaths=pd.to_numeric(df.number_of_deaths),
        number_of_recovered=pd.to_numeric(df.number_of_recovered),
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

    # Overwrite the existing layers
    gis_item = gis.content.get(jhu_time_series_featureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(time_series_filename)
    gis_layer_collection.manager.update_definition({"maxRecordCount": max_record_count})

    gis_item = gis.content.get(jhu_current_featureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(most_recent_date_filename)
    gis_layer_collection.manager.update_definition({"maxRecordCount": max_record_count})

    # Clean up
    os.remove(time_series_filename)
    os.remove(most_recent_date_filename)


def load_data(**kwargs):
    """
    Entry point for the DAG, loading state and county data to ESRI.
    """
    try:
        load_global_covid_data()
    except Exception as e:
        logging.warning("Failed to load global data with error: " + str(e))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 16),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org", "itadata@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

dag = DAG("jhu-to-esri_v3", default_args=default_args, schedule_interval="@hourly")


t1 = PythonOperator(
    task_id="sync-jhu-to-esri",
    provide_context=True,
    python_callable=load_data,
    op_kwargs={},
    dag=dag,
)

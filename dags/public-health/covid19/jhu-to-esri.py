"""
ETL for COVID-19 Data.
Pulls from Johns-Hopkins CSSE data.
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

bucket_name = "public-health-dashboard"

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

# Feature ID for JHU global source data
JHU_GLOBAL_SOURCE_ID = "c0b356e20b30490c8b8b4c7bb9554e7c"

# Feature IDs for state/province level time series and current status
jhu_time_series_featureid = "20271474d3c3404d9c79bed0dbd48580"
jhu_current_featureid = "191df200230642099002039816dc8c59"

max_record_count = 6_000_000

# The date at the time of execution. We choose midnight in the US/Pacific timezone,
# but then convert to UTC since that is what AGOL expects. When the feature layer
# is viewed in a dashboard it is converted back to local time.
# date = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")


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
        "number_of_cases",
        "number_of_deaths",
        "number_of_recovered",
    ]
    new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}
    return df.assign(**new_cols)


sort_cols = ["Country_Region", "Province_State", "date"]


def load_jhu_global_time_series(branch="master"):
    """
    Loads the JHU global timeseries data, transforms it so we are happy with it.
    """
    cases = pd.read_csv(CASES_URL.format(branch))
    deaths = pd.read_csv(DEATHS_URL.format(branch))
    recovered = pd.read_csv(RECOVERED_URL.format(branch))
    # melt cases
    id_vars, dates = parse_columns(cases)
    cases_df = pd.melt(
        cases,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_cases",
        var_name="date",
    )

    # melt deaths
    id_vars, dates = parse_columns(deaths)
    deaths_df = pd.melt(
        deaths,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_deaths",
        var_name="date",
    )

    # melt recovered
    id_vars, dates = parse_columns(recovered)
    recovered_df = pd.melt(
        recovered,
        id_vars=id_vars,
        value_vars=dates,
        value_name="number_of_recovered",
        var_name="date",
    )

    # join
    merge_cols = ["Province/State", "Country/Region", "Lat", "Long", "date"]
    m1 = pd.merge(cases_df, deaths_df, on=merge_cols, how="left")
    df = pd.merge(m1, recovered_df, on=merge_cols, how="left")

    df = df.assign(
        date=pd.to_datetime(df.date)
        .dt.tz_localize("US/Pacific")
        .dt.normalize()
        .dt.tz_convert("UTC"),
    ).rename(
        columns={
            "Country/Region": "Country_Region",
            "Province/State": "Province_State",
        }
    )

    return df.sort_values(sort_cols).reset_index(drop=True)


def load_jhu_global_current(**kwargs):
    """
    Loads the JHU global current data, transforms it so we are happy with it.
    """
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # (1) Load current data from ESRI
    gis_item = gis.content.get(JHU_GLOBAL_SOURCE_ID)
    layer = gis_item.layers[1]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # ESRI dataframes seem to lose their localization.
    sdf = sdf.assign(
        date=sdf.Last_Update.dt.tz_localize("US/Pacific")
        .dt.normalize()
        .dt.tz_convert("UTC")
    )
    # Drop some ESRI faf
    sdf = sdf.drop(columns=["OBJECTID", "SHAPE"])

    # CSVs report province-level totals for every country except US.
    global_df = sdf[sdf.Country_Region != "US"]
    # US should just have 1 observation.
    us_df = sdf[sdf.Country_Region == "US"]
    us_totals = (
        us_df.groupby(["Country_Region", "date"])
        .agg({"Confirmed": "sum", "Recovered": "sum", "Deaths": "sum"})
        .assign(Lat=37.0902, Long_=-95.7129,)
        .reset_index()
    )

    us_df = pd.merge(
        us_df.drop(columns=["Lat", "Long_", "Confirmed", "Recovered", "Deaths"]),
        us_totals,
        on=["Country_Region", "date"],
        how="left",
    )

    df = global_df.append(us_totals, sort=False)

    df = df.assign(
        number_of_cases=pd.to_numeric(df.Confirmed),
        number_of_recovered=pd.to_numeric(df.Recovered),
        number_of_deaths=pd.to_numeric(df.Deaths),
    ).rename(columns={"Long_": "Long"})

    keep_cols = [
        "Province_State",
        "Country_Region",
        "date",
        "Lat",
        "Long",
        "number_of_cases",
        "number_of_recovered",
        "number_of_deaths",
    ]

    df = df[keep_cols]

    return df.sort_values(sort_cols).reset_index(drop=True)


def load_global_covid_data():
    """
    Load global COVID-19 data from JHU.
    """
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    historical_df = load_jhu_global_time_series()

    # Bring in the current date's JHU data
    today_df = load_jhu_global_current()
    coordinates = today_df[
        ["Province_State", "Country_Region", "Lat", "Long"]
    ].drop_duplicates()

    # Append
    df = historical_df.append(today_df, sort=False)

    # Merge in lat/lon coordinates
    # There are differences between GitHub CSV and feature layer. Use feature layer's.
    df = pd.merge(
        df.drop(columns=["Lat", "Long"]),
        coordinates,
        on=["Province_State", "Country_Region"],
        how="left",
        validate="m:1",
    )

    df = (
        df.assign(
            number_of_cases=pd.to_numeric(df.number_of_cases),
            number_of_deaths=pd.to_numeric(df.number_of_deaths),
            number_of_recovered=pd.to_numeric(df.number_of_recovered),
        )
        .pipe(coerce_integer)
        .drop_duplicates(subset=sort_cols, keep="last")
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )

    # Output to CSV
    time_series_filename = "/tmp/jhu_covid19_time_series.csv"
    df.to_csv(time_series_filename, index=False)
    # df.to_parquet(f"s3://{bucket_name}/jhu_covid19/global-time-series.parquet")

    # Also output the most current date as a separate CSV for convenience
    most_recent_date_filename = "/tmp/jhu_covid19_current.csv"
    df[df.date == df.date.max()].to_csv(most_recent_date_filename, index=False)

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

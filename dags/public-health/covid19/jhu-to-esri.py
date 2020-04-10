"""
ETL for COVID-19 Data.
Pulls from Johns-Hopkins CSSE data as well as local government agencies.
"""
import logging
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

import arcgis
import geopandas as gpd
from arcgis.gis import GIS

# This ref is to the last commit that JHU had before they
# switched to not providing county-level data. We use it
# below to backfill some case counts in a county-level time series.
JHU_COUNTY_BRANCH = "a3e83c7bafdb2c3f310e2a0f6651126d9fe0936f"

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

# Feature IDs for county level time series and current status
time_series_featureid = "d61924e1d8344a09a1298707cfff388c"
current_featureid = "523a372d71014bd491064d74e3eba2c7"

# Feature IDs for state/province level time series and current status
jhu_time_series_featureid = "20271474d3c3404d9c79bed0dbd48580"
jhu_current_featureid = "191df200230642099002039816dc8c59"

# The date at the time of execution. We choose midnight in the US/Pacific timezone,
# but then convert to UTC since that is what AGOL expects. When the feature layer
# is viewed in a dashboard it is converted back to local time.
date = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")

# Columns expected for our county level timeseries.
columns = [
    "state",
    "county",
    "date",
    "latitude",
    "longitude",
    "cases",
    "deaths",
    "recovered",
    "travel_based",
    "locally_acquired",
    "ca_total",
    "non_scag_total",
]


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


def rename_geog_cols(df):
    """
    # Rename geography columns to be the same as future schemas
    """
    df.rename(
        columns={
            "Country/Region": "Country_Region",
            "Province/State": "Province_State",
            "Long": "Lon",
        },
        inplace=True,
    )
    return df


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
        "travel_based",
        "locally_acquired",
        "ca_total",
        "non_scag_total",
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


def load_esri_time_series(gis):
    """
    Load the county-level time series dataframe from the ESRI feature server.
    """
    gis_item = gis.content.get(time_series_featureid)
    layer = gis_item.layers[0]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # Drop some ESRI faf
    sdf = sdf.drop(columns=["ObjectId", "SHAPE"]).drop_duplicates(
        subset=["date", "county"], keep="last"
    )
    return sdf.assign(date=sdf.date.dt.tz_localize("UTC"))


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

    gis_item = gis.content.get(jhu_current_featureid)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(most_recent_date_filename)

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


########### T2 and Helper Functions ###################################
def subset_msa(df):
    # 5 MSAs to plot: NYC, SF_SJ, SEA, DET, LA
    df = df[
        df.cbsatitle.str.contains("Los Angeles")
        | df.cbsatitle.str.contains("New York")
        | df.cbsatitle.str.contains("San Francisco")
        | df.cbsatitle.str.contains("San Jose")
        | df.cbsatitle.str.contains("Seattle")
        | df.cbsatitle.str.contains("Detroit")
    ]

    def new_categories(row):
        if ("San Francisco" in row.cbsatitle) or ("San Jose" in row.cbsatitle):
            return "SF/SJ"
        elif "Los Angeles" in row.cbsatitle:
            return "LA/OC"
        elif "New York City" in row.cbsatitle:
            return "NYC"
        elif "Seattle" in row.cbsatitle:
            return "SEA"
        elif "Detroit" in row.cbsatitle:
            return "DET"

    df = df.assign(msa=df.apply(new_categories, axis=1))

    return df


def update_msa_dataset(**kwargs):
    """
    Update MSA dataset
    ref gh/aqueduct#199
    takes the previous step data, aggegrates by MSA
    replaces featurelayer.
    """
    # load and subset data
    url = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/county_time_series_331/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=county%2C+state%2C+fips%2C+date%2C+Lat%2C+Lon%2C+cases%2C+deaths%2C+incident_rate%2C+people_tested%2C+state_cases%2C+state_deaths%2C+new_cases%2C+new_deaths%2C+new_state_cases%2C+new_state_deaths&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    gdf = gpd.read_file(url)
    # Switch pop crosswalk with GitHub URL, once we upload it there
    pop = pd.read_csv(
        "./msa_county_pop_crosswalk.csv",
        dtype={"county_fips": "str", "cbsacode": "str"},
    )

    pop = pop[["cbsacode", "cbsatitle", "population", "county_fips"]]
    pop = subset_msa(pop)

    # merge
    gdf = pd.merge(
        gdf, pop, left_on="fips", right_on="county_fips", how="inner", validate="m:1"
    )

    # Aggregate by MSA
    group_cols = ["cbsacode", "msa", "population", "date"]
    msa = gdf.groupby(group_cols).agg({"cases": "sum", "deaths": "sum"}).reset_index()

    # Calculate rate per 1M
    rate = 1_000_000
    msa = msa.assign(
        cases_per_1M=msa.cases / msa.population * rate,
        deaths_per_1M=msa.deaths / msa.population * rate,
    )
    msa = merge_jhu_with_msa_crosswalk(gdf, pop)


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

t2 = PythonOperator(
    task_id="update-msa-data",
    provide_context=True,
    python_callable=update_msa_dataset,
    op_kwargs={},
    dag=dag,
)

t1 > t2

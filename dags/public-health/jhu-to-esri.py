"""
ETL for COVID-19 Data.
Pulls from Johns-Hopkins CSSE data as well as local government agencies.
"""
import logging
import os
from datetime import datetime, timedelta

import arcgis
import bs4
import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

# This ref is to the last commit that JHU had before they
# switched to not providing county-level data. We use it
# below to backfill some case counts in a county-level time series.
JHU_COUNTY_BRANCH = "a3e83c7bafdb2c3f310e2a0f6651126d9fe0936f"

# URL to JHU confirmed cases time series.
CASES_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
)

# URL to JHU deaths time series.
DEATHS_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"
)

# URL to JHU recoveries time series
RECOVERED_URL = (
    "https://github.com/CSSEGISandData/COVID-19/raw/{}/"
    "csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"
)

# Feature IDs for county level time series and current status
time_series_featureid = "d61924e1d8344a09a1298707cfff388c"
current_featureid = "523a372d71014bd491064d74e3eba2c7"

# Feature IDs for state/province level time series and current status
jhu_time_series_featureid = "20271474d3c3404d9c79bed0dbd48580"
jhu_current_featureid = "191df200230642099002039816dc8c59"

# The date at the time of execution.
date = pd.Timestamp.now(tz="US/Pacific").date()

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


def load_jhu_time_series(branch="master"):
    """
    Loads the JHU data, transforms it so we are happy with it.
    This mostly reshapes and joins the CSVs, any filtering and
    renaming is done downstream.
    """
    cases = pd.read_csv(CASES_URL.format(branch))
    deaths = pd.read_csv(DEATHS_URL.format(branch))
    recovered = pd.read_csv(RECOVERED_URL.format(branch))
    # melt cases
    id_vars, dates = parse_columns(cases)
    df = pd.melt(
        cases, id_vars=id_vars, value_vars=dates, value_name="cases", var_name="date",
    )

    # melt deaths
    id_vars, dates = parse_columns(deaths)
    deaths_df = pd.melt(deaths, id_vars=id_vars, value_vars=dates, value_name="deaths")

    # melt recovered
    id_vars, dates = parse_columns(deaths)
    recovered_df = pd.melt(
        recovered, id_vars=id_vars, value_vars=dates, value_name="recovered"
    )

    # join
    df["deaths"] = deaths_df.deaths
    df["recovered"] = recovered_df.recovered

    return df


def load_jhu_state_time_series():
    """
    Load time series for JHU at the state/province level.
    """
    # Load the overall time series
    df = load_jhu_time_series()

    # Drop county level data
    df = df[~((df["Country/Region"] == "US") & df["Province/State"].str.contains(","))]

    # Rename some columns to conform to a previous schema for the CSV.
    df = df.rename(
        columns={
            "cases": "number_of_cases",
            "deaths": "number_of_deaths",
            "recovered": "number_of_recovered",
        }
    )

    return df.sort_values(["date", "Country/Region", "Province/State"]).reset_index(
        drop=True
    )


def load_jhu_county_time_series():
    """
    Load time series for JHU at the Southern California county level.

    This is mostly a historical dataset, since JHU no longer updates county-level
    data. Instead, we load it once into our feature server, then continually update
    that feature server with county level data.
    """
    # Load the overall time series.
    df = load_jhu_time_series(branch=JHU_COUNTY_BRANCH)

    # filter to SCAG counties
    df = df[
        (df["Province/State"] == "Los Angeles, CA")
        | (df["Province/State"] == "Riverside County, CA")
        | (df["Province/State"] == "Ventura, CA")
        | (df["Province/State"] == "Orange County, CA")
    ]
    # make some simple asserts to assure that the data structures haven't changed and
    # some old numbers are still correct
    assert (
        df.loc[(df["Province/State"] == "Los Angeles, CA") & (df["date"] == "3/11/20")][
            "cases"
        ].iloc[0]
        == 27
    )
    assert (
        df.loc[(df["Province/State"] == "Los Angeles, CA") & (df["date"] == "3/11/20")][
            "deaths"
        ].iloc[0]
        == 1
    )

    # Rename columns and rows to match schema
    df = df.rename(
        columns={"Province/State": "county", "Lat": "latitude", "Long": "longitude"}
    )
    df = df.assign(
        county=df.county.str.rstrip(", CA").str.rstrip("County").str.strip(),
        state="CA",
        travel_based=None,
        locally_acquired=None,
        date=pd.to_datetime(df.date).dt.date,
    ).drop(columns=["Country/Region"])

    return df.sort_values(["date", "county"]).reset_index(drop=True)


def load_esri_time_series(gis):
    """
    Load the county-level time series dataframe from the ESRI feature server.
    """
    gis_item = gis.content.get(time_series_featureid)
    layer = gis_item.layers[0]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # Drop some ESRI faf
    sdf = sdf.drop(columns=["ObjectId", "SHAPE"]).drop_duplicates(
        subset=["date", "county"], keep="last",
    )
    # Convert from timestamp to date for later comparisons
    sdf = sdf.assign(date=sdf.date.dt.date)
    return sdf


def scrape_la_county_public_health_data():
    """
    Scrape data from the Los Angeles County Department of Public Health.
    """
    text = requests.get("http://publichealth.lacounty.gov/media/Coronavirus/").text
    soup = bs4.BeautifulSoup(text, "lxml")
    counter_data = soup.find_all("div", class_="counter-block counter-text")
    counts = [int(c.contents[0]) for c in counter_data]
    cases, deaths = counts
    return {
        "state": "CA",
        "county": "Los Angeles",
        "latitude": 34.05,
        "longitude": -118.25,
        "date": date,
        "cases": cases,
        "deaths": deaths,
        "recovered": None,
        "travel_based": None,
        "locally_acquired": None,
    }


def scrape_imperial_county_public_health_data():
    """
    Scrape data from the Imperial County Department of Public Health.
    """
    df = pd.read_html(
        "http://www.icphd.org/health-information-and-resources/healthy-facts/covid-19/"
    )[0].dropna()
    cases = int(
        df[df.iloc[:, 0].str.lower().str.contains("confirmed")].iloc[:, 1].iloc[0]
    )
    return {
        "state": "CA",
        "county": "Imperial",
        "latitude": 32.8,
        "longitude": -115.57,
        "date": date,
        "cases": cases,
        "deaths": None,
        "recovered": None,
        "travel_based": None,
        "locally_acquired": None,
    }


def scrape_orange_county_public_health_data():
    """
    Scrape data from the Orange County Department of Public Health.
    """
    df = pd.read_html(
        "http://www.ochealthinfo.com"
        "/phs/about/epidasmt/epi/dip/prevention/novel_coronavirus",
        match="Orange County Coronavirus",
    )[0].dropna()
    cases = pd.to_numeric(df[1][6])
    deaths = int(df[1][10])
    travel_based = int(df[1][7])
    locally_acquired = int(df[1][8]) + int(df[1][9])
    return {
        "state": "CA",
        "county": "Orange",
        "latitude": 33.74,
        "longitude": -117.88,
        "date": date,
        "cases": cases,
        "deaths": deaths,
        "recovered": None,
        "travel_based": travel_based,
        "locally_acquired": locally_acquired,
    }


def scrape_county_public_health_data():
    """
    Scrape data from many public health departments.
    """
    df = pd.DataFrame(columns=columns)
    # make robust
    county_dfs = []
    try:
        logging.info("Loading data from LA County")
        county_dfs.append(scrape_la_county_public_health_data())
    except (Exception, ArithmeticError):
        logging.warning("Failed to load data from Orange County")

    try:
        logging.info("Loading data from Imperial County")
        county_dfs.append(scrape_imperial_county_public_health_data())
    except (Exception, ArithmeticError):
        logging.warning("Failed to load data from Orange County")

    try:
        logging.info("Loading data from Orange County")
        county_dfs.append(scrape_orange_county_public_health_data())
    except (Exception, ArithmeticError):
        logging.warning("Failed to load data from Orange County")

    df = df.append(county_dfs, ignore_index=True,)
    return df


def load_state_covid_data():
    """
    Load State/Province level COVID-19 data from JHU.
    """
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    df = load_jhu_state_time_series()

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


def load_county_covid_data():
    """
    Load County level COVID-19 data from JHU/ESRI/Public Health departments.
    """
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # Loaded JHU once, from then on we append to the ESRI layer.
    # prev_data = load_jhu_county_time_series()
    prev_data = load_esri_time_series(gis)

    county_data = scrape_county_public_health_data()
    df = (
        prev_data.append(county_data, sort=False)
        .drop_duplicates(subset=["date", "county"], keep="last")
        .reset_index(drop=True)
    )

    # Add placeholder data for California and non-SCAG totals.
    df = df.assign(ca_total=0, non_scag_total=0)

    # Output to CSV
    time_series_filename = "/tmp/covid19_time_series.csv"
    df.to_csv(time_series_filename, index=False)

    # Also output the most current date as a separate CSV for convenience
    most_recent_date_filename = "/tmp/covid19_current.csv"
    current_df = df.assign(date=pd.to_datetime(df.date))
    current_df[current_df.date == current_df.date.max()].to_csv(
        most_recent_date_filename, index=False
    )

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


def load_data(**kwargs):
    """
    Entry point for the DAG, loading state and county data to ESRI.
    """
    try:
        load_county_covid_data()
    except Exception as e:
        logging.warning("Failed to load county-level data" + str(e))
    try:
        load_state_covid_data()
    except Exception as e:
        logging.warning("Failed to load state-level data" + str(e))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 16),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

dag = DAG("jhu-to-esri_v2", default_args=default_args, schedule_interval="@hourly")


t1 = PythonOperator(
    task_id="sync-jhu-to-esri",
    provide_context=True,
    python_callable=load_data,
    op_kwargs={},
    dag=dag,
)

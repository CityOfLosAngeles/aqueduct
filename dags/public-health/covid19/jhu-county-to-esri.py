"""
Grab the 'static' portion of time-series from NYT
and add JHU DAG to this.
"""
from datetime import datetime, timedelta

import arcgis
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

# General function
TIME_SERIES_FEATURE_ID = "4e0dc873bd794c14b7bd186b4b5e74a2"
JHU_FEATURE_ID = "628578697fb24d8ea4c32fa0c5ae1843"


def append_county_time_series(**kwargs):
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # (1) Load time series data from ESRI
    gis_item = gis.content.get(TIME_SERIES_FEATURE_ID)
    layer = gis_item.layers[0]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # ESRI dataframes seem to lose their localization.
    sdf = sdf.assign(date=sdf.date.dt.tz_localize("UTC"))
    # Drop some ESRI faf
    old_ts = sdf.drop(columns=["ObjectId", "SHAPE"])

    # (2) Bring in NYT US county level data and clean
    NYT_COMMIT = "baeca648aefa9694a3fc8f2b3bd3f797937aa1c5"
    NYT_COUNTY_URL = (
        f"https://raw.githubusercontent.com/nytimes/covid-19-data/{NYT_COMMIT}/"
        "us-counties.csv"
    )
    county = pd.read_csv(NYT_COUNTY_URL)
    county = clean_nyt_county(county)
    nyt_geog = county[county.fips != ""][["fips", "county", "state"]].drop_duplicates()

    # (3) Load the most recent data from the JHU feature layer.
    gis_item = gis.content.get(JHU_FEATURE_ID)
    layer = gis_item.layers[0]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # Drop some ESRI faf
    jhu = sdf.drop(columns=["OBJECTID", "SHAPE"])

    # Create localized then normalized date column
    jhu["date"] = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")
    jhu = clean_jhu_county(jhu, nyt_geog)

    # (4) Append NYT and JHU and fill in missing county lat/lon
    us_county = old_ts.append(jhu, sort=False)

    # Clean up full dataset by filling in missing values and dropping duplicates
    us_county = fill_missing_stuff(us_county)

    # (5) Calculate US State totals
    us_county = us_state_totals(us_county)

    # (6) Calculate change in casesload from the prior day
    us_county = calculate_change(us_county)

    # (7) Fix column types before exporting
    final = fix_column_dtypes(us_county)

    # (8) Write to CSV and overwrite the old feature layer.
    time_series_filename = "/tmp/jhu-county-time-series.csv"
    final.to_csv(time_series_filename)
    gis_item = gis.content.get(TIME_SERIES_FEATURE_ID)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(time_series_filename)

    return True


# Sub-functions to be used
def coerce_fips_integer(df):
    def integrify(x):
        return int(float(x)) if not pd.isna(x) else None

    cols = ["fips"]

    new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}

    return df.assign(**new_cols)


def correct_county_fips(row):
    if len(row.fips) == 5:
        return row.fips
    elif (len(row.fips) == 4) and (row.fips != "None"):
        return "0" + row.fips
    elif row.fips == "None":
        return ""


# (1) Bring in NYT US county level data and clean
def clean_nyt_county(df):
    keep_cols = ["date", "county", "state", "fips", "cases", "deaths"]
    df = df[keep_cols]
    df["date"] = pd.to_datetime(df.date)
    # Create new columns to store what JHU reports
    df["incident_rate"] = np.nan
    df["people_tested"] = np.nan
    # Fix column type
    df = coerce_fips_integer(df)
    df["fips"] = df.fips.astype(str)
    df["fips"] = df.apply(correct_county_fips, axis=1)
    return df


# (2) Add JHU data for 3/30 and clean up geography
def clean_jhu_county(df, nyt_geog):
    # Only keep certain columns and rename them to match NYT schema
    keep_cols = [
        "Province_State",
        "Country_Region",
        "Lat",
        "Long_",
        "Confirmed",
        "Deaths",
        "FIPS",
        "Incident_Rate",
        "People_Tested",
        "date",
    ]

    df = df[keep_cols]

    df.rename(
        columns={
            "Confirmed": "cases",
            "Deaths": "deaths",
            "FIPS": "fips",
            "Long_": "Lon",
            "People_Tested": "people_tested",
            "Incident_Rate": "incident_rate",
        },
        inplace=True,
    )

    # Use FIPS to merge in NYT columns for county and state names
    # There are some values with no FIPS, NYT calls these county = "Unknown"
    df = pd.merge(df, nyt_geog, on="fips", how="left", validate="m:1")

    # Fix when FIPS is unknown, which wouldn't have merged in anything from nyt_geog
    df["county"] = df.apply(
        lambda row: "Unknown" if row.fips is None else row.county, axis=1
    )
    df["state"] = df.apply(
        lambda row: row.Province_State if row.fips is None else row.state, axis=1
    )
    df["fips"] = df.fips.fillna("")

    # Only keep certain columns and rename them to match NYT schema
    drop_cols = ["Province_State", "Country_Region"]

    df = df.drop(columns=drop_cols)

    return df


# (3) Append NYT and JHU and fill in missing county lat/lon
def fill_missing_stuff(df):
    not_missing_coords = df[df.Lat.notna()][
        ["state", "county", "Lat", "Lon"]
    ].drop_duplicates()

    df = pd.merge(
        df.drop(columns=["Lat", "Lon"]),
        not_missing_coords,
        on=["state", "county"],
        how="left",
    )

    # Drop duplicates and keep last observation
    group_cols = ["state", "county", "fips", "date"]
    for col in ["cases", "deaths"]:
        df[col] = df.groupby(group_cols)[col].transform("max")

    df = df.drop_duplicates(subset=group_cols, keep="last")

    return df


# (5) Calculate US State totals
def us_state_totals(df):
    state_grouping_cols = ["state", "date"]

    state_totals = df.groupby(state_grouping_cols).agg(
        {"cases": "sum", "deaths": "sum"}
    )
    state_totals = state_totals.rename(
        columns={"cases": "state_cases", "deaths": "state_deaths"}
    )

    df = pd.merge(
        df.drop(columns=["state_cases", "state_deaths"]),
        state_totals,
        on=state_grouping_cols,
    )

    return df


# (6) Calculate change in casesload from the prior day
def calculate_change(df):
    group_cols = ["state", "county", "fips", "date"]

    for col in ["cases", "deaths"]:
        new_col = f"new_{col}"
        county_group_cols = ["state", "county"]
        df[new_col] = (
            df.sort_values(group_cols)
            .groupby(county_group_cols)[col]
            .apply(lambda row: row - row.shift(1))
        )
        # First obs will be NaN, but the change in caseload is just the # of cases.
        df[new_col] = df[new_col].fillna(df[col])

    for col in ["state_cases", "state_deaths"]:
        new_col = f"new_{col}"
        state_group_cols = ["state"]
        df[new_col] = (
            df.sort_values(group_cols)
            .groupby(state_group_cols)[col]
            .apply(lambda row: row - row.shift(1))
        )
        df[new_col] = df[new_col].fillna(df[col])

    return df


# (7) Fix column types before exporting
def fix_column_dtypes(df):
    def coerce_integer(df):
        def integrify(x):
            return int(float(x)) if not pd.isna(x) else None

        cols = [
            "cases",
            "deaths",
            "state_cases",
            "state_deaths",
            "new_cases",
            "new_deaths",
            "new_state_cases",
            "new_state_deaths",
        ]

        new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}

        return df.assign(**new_cols)

    # Sort columns
    col_order = [
        "county",
        "state",
        "fips",
        "date",
        "Lat",
        "Lon",
        "cases",
        "deaths",
        "incident_rate",
        "people_tested",
        "state_cases",
        "state_deaths",
        "new_cases",
        "new_deaths",
        "new_state_cases",
        "new_state_deaths",
    ]

    df = (
        df.pipe(coerce_integer)
        .reindex(columns=col_order)
        .sort_values(["state", "county", "fips", "date", "cases"])
    )

    return df


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 1),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org", "itadata@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("jhu-county-to-esri", default_args=default_args, schedule_interval="@hourly")


t1 = PythonOperator(
    task_id="append_county_time_series",
    provide_context=True,
    python_callable=append_county_time_series,
    op_kwargs={},
    dag=dag,
)

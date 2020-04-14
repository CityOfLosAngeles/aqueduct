"""
ETL for Shelter Occupancy Data.
From Google Forms to ArcGIS Online.
"""
import os
from datetime import datetime, timedelta

import arcgis
import geopandas as gpd
import numpy as np
import pandas as pd
import pytz
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from arcgis.gis import GIS

RAP_SHELTER_URL = "https://services7.arcgis.com/aFfS9FqkIRSo0Ceu/ArcGIS/rest/services/LARAP%20COVID19%20MPOD%20Public/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="  # noqa: E501

SHELTER_CSV_URL = "https://docs.google.com/spreadsheets/d/1pkg7PVCS4lwVhNA3TkMSWKmzw4vsd4W53_Q_Ou2mXqw/export?format=csv&id=1pkg7PVCS4lwVhNA3TkMSWKmzw4vsd4W53_Q_Ou2mXqw&gid=73455498"  # noqa: E501

EMAIL_LIST_URL = "https://docs.google.com/spreadsheets/u/1/d/1q6u3nqBnyckVOIg-nCzPZuRXRs0b6YDgRHWGg5OAp-8/export?format=csv&id=1q6u3nqBnyckVOIg-nCzPZuRXRs0b6YDgRHWGg5OAp-8&gid=0"  # noqa: E501

SHELTER_ID = "2085cb061b834faf9fa5244b033b41ec"

LATEST_ID = "1b73a44e811549ec8952a1ff24e51cd0"

STATS_ID = "8679b3973d254aca9e247ffa85b012dd"

local_tz = "America/Los_Angeles"


def upload_to_esri(df, layer_id, filename="/tmp/df.csv"):
    """
    A quick helper function to upload a data frame
    to ESRI as a featurelayer backed CSV

    recommend: no geometries, lat/long columns
    remember ESRI is UTC only.
    """
    df.to_csv(filename, index=False)
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    gis_item = gis.content.get(layer_id)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(filename)

    os.remove(filename)
    return True


def load_data(**kwargs):
    """
    Entry point for the DAG, loading shelter to ESRI.
    """
    gdf = gpd.read_file(RAP_SHELTER_URL, driver="GeoJSON")
    gdf = gdf.assign(FacilityName=gdf.FacilityName.str.strip())
    df = pd.read_csv(SHELTER_CSV_URL)
    df = df.rename({"Shelter Site? ": "FacilityName"}, axis=1)
    df = df.assign(FacilityName=df.FacilityName.str.strip())

    # need to string match join here, which is a pain.
    # this is only a tier 1 pass for now.
    df = df.replace(
        to_replace="109th Recreation Center", value="109th Street Recreation Center"
    )
    df = df.replace(
        to_replace=["Echo Park Community Center", "Echo Park Community Ctr"],
        value="Echo Park Community Center - Gym",
    )
    df = df.replace(
        to_replace="Westchester Recreation Center",
        value="Westchester Recreation Center - Gym",
    )

    # change reported time to pacific time
    df["date"] = (
        df["Date "].apply(pd.to_datetime, format="%m/%d/%Y", errors="coerce").dt.date
    )
    df["time"] = (
        df["Time"].apply(pd.to_datetime, format="%I:%M %p", errors="coerce").dt.time
    )
    df["reported_datetime"] = pd.to_datetime(
        df.date.astype(str) + " " + df.time.astype(str), errors="coerce"
    )
    df["reported_datetime"] = df.reported_datetime.dt.tz_localize(tz="US/Pacific")
    df["Date "] = df.reported_datetime.dt.tz_convert("UTC")
    df["Time"] = df.reported_datetime.dt.tz_convert("UTC")

    # timestamp
    df["Timestamp"] = (
        pd.to_datetime(df.Timestamp)
        .dt.tz_localize(tz="US/Pacific")
        .dt.tz_convert("UTC")
    )
    df["string-ts"] = df["Timestamp"].dt.strftime("%B %d, %Y, %r")

    df["string-ti"] = df["Time"].dt.strftime("%B %d, %Y, %r")
    # do something horrifying

    df["string-ts"].iloc[1] = "None"

    # merge on parksname
    # this preserves the number of entries (tested.)
    gdf = gdf.merge(df, on="FacilityName")

    # export to CSV
    gdf["Latitude"] = gdf.geometry.y
    gdf["Longitude"] = gdf.geometry.x
    time_series_filename = "/tmp/rap-shelter-timeseries-v3.csv"

    df = pd.DataFrame(gdf).drop(
        ["geometry", "date", "time", "reported_datetime"], axis=1
    )

    # make latest layer
    latest_df = df[
        df.groupby("FacilityName")["Timestamp"].transform(max) == df["Timestamp"]
    ]

    latest_df["open_beds_computed"] = (
        latest_df["Number of Open Beds - MEN"]
        + latest_df["Number of Open Beds - WOMEN"]
    )
    latest_df["occupied_beds_computed"] = (
        latest_df["Total Women Currently at Site"]
        + latest_df["Total Men Currently at Site"]
    )

    latest_filename = "/tmp/rap-latest-shelters-v3.csv"
    latest_df.to_csv(latest_filename, index=False)

    # Compute a number of open and reporting shelter beds
    utc_now = pytz.utc.localize(datetime.utcnow())
    report_interval = utc_now - pd.Timedelta("24H")

    stats = {
        "Number_of_Reporting_Shelters": df.FacilityName.nunique(),
        "Number_of_Reporting_Shelters_last_24H": df[
            df.Timestamp >= report_interval
        ].FacilityName.nunique(),
    }
    stats_df = pd.DataFrame.from_dict(
        stats, orient="index", columns=["Count"]
    ).transpose()
    # TODO: Write an assert to make sure all rows are in resultant GDF

    # push the tables into kwargs for email
    kwargs["ti"].xcom_push(key="latest_df", value=latest_df)
    kwargs["ti"].xcom_push(key="stats_df", value=stats_df)
    # upload to ESRI
    upload_to_esri(latest_df, LATEST_ID, filename=latest_filename)
    upload_to_esri(df, SHELTER_ID, filename=time_series_filename)
    upload_to_esri(stats_df, STATS_ID, filename="/tmp/rap-shelter-stats.csv")
    return True


def integrify(x):
    return str(int(x)) if not pd.isna(x) else "Error"


def format_table(row):
    """
    returns a nicely formatted HTML
    for each Shelter row
    """
    shelter_name = row["FacilityName"]
    last_report = row["timestamp_local"]
    capacity = integrify(row["ShelterCapacity"])
    occupied_beds = integrify(row["occupied_beds_computed"])
    aval_beds = integrify(row["open_beds_computed"])
    male_tot = integrify(row["Total Men Currently at Site"])
    female_total = integrify(row["Total Women Currently at Site"])
    pets = integrify(row["Number of Pets Currently at Site"])
    shelter = f"""<b>{shelter_name}</b><br>
    <i>Report Time: {last_report}</i><br>
    <p style="margin-top:2px; margin-bottom: 2px">Capacity: {capacity}</p>
    <p style="margin-top:2px; margin-bottom: 2px">Occupied Beds: {occupied_beds}</p>
    <p style="margin-top:2px; margin-bottom: 2px">Avaliable Beds: {aval_beds}</p>
    <p style="margin-top:2px; margin-bottom: 2px">Male: {male_tot}</p>
    <p style="margin-top:2px; margin-bottom: 2px">Female: {female_total}</p>
    <p style="margin-top:2px; margin-bottom: 2px">Pets: {pets}</p><br>
    """
    return shelter.strip()


def email_function(**kwargs):
    """
    Sends a hourly email with the latest updates from each shelter
    Formatted for use
    """
    latest_df = kwargs["ti"].xcom_pull(key="latest_df", task_ids="sync-shelter-to-esri")
    stats_df = kwargs["ti"].xcom_pull(key="stats_df", task_ids="sync-shelter-to-esri")
    exec_time = pd.Timestamp.now(tz="US/Pacific").strftime("%m-%d-%Y %I:%M%p")
    latest_df["timestamp_local"] = latest_df.Timestamp.dt.tz_convert(
        local_tz
    ).dt.strftime("%m-%d-%Y %I:%M%p")
    tbl = np.array2string(
        latest_df.apply(format_table, axis=1).str.replace("\n", "").values
    )
    tbl = tbl.replace("""'\n '""", "").lstrip(""" [' """).rstrip(""" '] """)
    email_template = f"""
    Shelter Report for {exec_time}.

    The Current Number of Reporting Shelters is
    {stats_df['Number_of_Reporting_Shelters'][0]}.

    <br>

    {tbl}

    For issue with this report, please contact itadata@lacity.org
    """

    if pd.Timestamp.now(tz="US/Pacific").hour in [8, 12, 15, 17, 20]:
        email_list = list(pd.read_csv(EMAIL_LIST_URL).email_addr.values)
    else:
        email_list = ["itadata@lacity.org"]

    send_email(
        to=email_list,
        subject=f"""Shelter Stats for {exec_time}""",
        html_content=email_template,
    )
    return True


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 25),
    "email": ["ian.rose@lacity.org", "hunter.owens@lacity.org", "itadata@lacity.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
}

dag = DAG("shelter-to-esri", default_args=default_args, schedule_interval="@hourly")


t1 = PythonOperator(
    task_id="sync-shelter-to-esri",
    provide_context=True,
    python_callable=load_data,
    op_kwargs={},
    dag=dag,
)

t2 = PythonOperator(
    task_id="send_shelter_email",
    provide_context=True,
    python_callable=email_function,
    dag=dag,
)

t1 >> t2

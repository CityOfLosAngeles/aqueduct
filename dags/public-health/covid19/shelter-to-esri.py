"""
ETL for Shelter Occupancy Data.
From Google Forms to ArcGIS Online.
"""
import os
from datetime import datetime, timedelta

import arcgis
import geopandas as gpd
import pandas as pd
import pytz
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

# Shelter locations from Oscar
# on LASAN site

TIER_1_META_URL = "https://services1.arcgis.com/X1hcdGx5Fxqn4d0j/arcgis/rest/services/COVID_19_Shelters_Tier1/FeatureServer/0/query?where=objectid%3Dobjectid&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="  # noqa: E501

TIER_2_META_URL = "https://services1.arcgis.com/X1hcdGx5Fxqn4d0j/ArcGIS/rest/services/COVID_19_Shelters_Tier2/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="  # noqa: E501

SHELTER_CSV_URL = "https://docs.google.com/spreadsheets/d/1pkg7PVCS4lwVhNA3TkMSWKmzw4vsd4W53_Q_Ou2mXqw/export?format=csv&id=1pkg7PVCS4lwVhNA3TkMSWKmzw4vsd4W53_Q_Ou2mXqw&gid=73455498"  # noqa: E501

SHELTER_ID = "22b5b5f4852041f68796b7967d559e0f"

LATEST_ID = "dbf7e62b02244e1a855a1f4b2624de76"

STATS_ID = "8679b3973d254aca9e247ffa85b012dd"


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
    tier_1 = gpd.read_file(TIER_1_META_URL, driver="GeoJSON")
    tier_2 = gpd.read_file(TIER_2_META_URL, driver="GeoJSON")
    gdf = pd.concat([tier_1, tier_2])
    df = pd.read_csv(SHELTER_CSV_URL)
    df = df.rename({"Shelter Site? ": "ParksName"}, axis=1)

    # need to string match join here, which is a pain.
    # this is only a tier 1 pass for now.
    df = df.replace(
        to_replace="109th Recreation Center", value="109th Street Recreation Center"
    )
    df = df.replace(
        to_replace="Granada Hills Recreation Center",
        value="Granada Hills Youth Recreation Center",
    )
    df = df.replace(
        to_replace="Central Recreation Center", value="Central Park Recreation Center"
    )
    df = df.replace(
        to_replace="Cheviot Hills Recreation Center",
        value="Cheviot Hills Park and Recreation Center",
    )
    df = df.replace(
        to_replace="Granada Hills Recreation Center",
        value="Granada Hills Youth Recreation Center",
    )
    df = df.replace(
        to_replace=["Echo Park Community Center", "Echo Park Community Ctr"],
        value="Echo Park Boys & Girls Club",
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
    gdf = gdf.merge(df, on="ParksName")

    # export to CSV
    gdf["Latitude"] = gdf.geometry.y
    gdf["Longitude"] = gdf.geometry.x
    time_series_filename = "/tmp/shelter_timeseries_current_v2.csv"

    df = pd.DataFrame(gdf).drop(
        ["geometry", "date", "time", "reported_datetime"], axis=1
    )

    # make latest layer
    latest_df = df[
        df.groupby("ParksName")["Timestamp"].transform(max) == df["Timestamp"]
    ]

    latest_df["open_beds_computed"] = (
        latest_df["Number of Open Beds - MEN"]
        + latest_df["Number of Open Beds - WOMEN"]
    )
    latest_df["occupied_beds_computed"] = (
        latest_df["Total Men Currently at Site"]
        + latest_df["Total Men Currently at Site"]
    )

    latest_df["total_capacity"] = (
        latest_df.open_beds_computed + latest_df.occupied_beds_computed
    )
    latest_filename = "/tmp/latest-shelters-v2.csv"
    latest_df.to_csv(latest_filename, index=False)

    # Compute a number of open and reporting shelter beds
    utc_now = pytz.utc.localize(datetime.utcnow())
    report_interval = utc_now - pd.Timedelta("24H")

    stats = {
        "Number_of_Reporting_Shelters": df.ParksName.nunique(),
        "Number_of_Reporting_Shelters_last_24H": df[
            df.Timestamp >= report_interval
        ].ParksName.nunique(),
    }
    stats_df = pd.DataFrame.from_dict(
        stats, orient="index", columns=["Count"]
    ).transpose()
    # TODO: Write an assert to make sure all rows are in resultant GDF

    upload_to_esri(latest_df, LATEST_ID, filename=latest_filename)
    upload_to_esri(df, SHELTER_ID, filename=time_series_filename)
    upload_to_esri(stats_df, STATS_ID, filename="/tmp/stats.csv")
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
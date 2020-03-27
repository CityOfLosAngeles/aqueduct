"""
ETL for Shelter Occupancy Data.
From Google Forms to ArcGIS Online.
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

import arcgis
import geopandas as gpd
from arcgis.gis import GIS

# Shelter locations from Oscar
# on LASAN site

TIER_1_META_URL = "https://services1.arcgis.com/X1hcdGx5Fxqn4d0j/arcgis/rest/services/COVID_19_Shelters_Tier1/FeatureServer/0/query?where=objectid%3Dobjectid&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="  # noqa: E501

TIER_2_META_URL = "https://services1.arcgis.com/X1hcdGx5Fxqn4d0j/ArcGIS/rest/services/COVID_19_Shelters_Tier2/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="  # noqa: E501

SHELTER_CSV_URL = "https://docs.google.com/spreadsheets/d/1pkg7PVCS4lwVhNA3TkMSWKmzw4vsd4W53_Q_Ou2mXqw/export?format=csv&id=1pkg7PVCS4lwVhNA3TkMSWKmzw4vsd4W53_Q_Ou2mXqw&gid=73455498"  # noqa: E501

SHELTER_ID = "22b5b5f4852041f68796b7967d559e0f#"


def load_data(**kwargs):
    """
    Entry point for the DAG, loading shelter to ESRI.
    """
    gdf = gpd.read_file(TIER_1_META_URL, driver="GeoJSON")
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
        to_replace="Echo Park Community Center", value="Echo Park Boys & Girls Club"
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

    pd.DataFrame(gdf).drop(
        ["geometry", "date", "time", "reported_datetime"], axis=1
    ).to_csv(time_series_filename, index=False)
    # TODO: Write an assert to make sure all rows are in resultant GDF
    # Login to ArcGIS
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # Overwrite the existing layers
    gis_item = gis.content.get(SHELTER_ID)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(time_series_filename)
    # Clean up
    os.remove(time_series_filename)
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

"""
Test whether we can port Civis jobs over to GCP

Create simple workflow related to 311 data
Bring in GeoHub boundary of sorts
Simple aggregation
Export to S3?

Ex: https://github.com/CityOfLosAngeles/notebook-demos/blob/master/ibis-query-311.py
"""

import geopandas
import intake_civis
import ibis
import pandas

WGS84 = "EPSG:4326"

COUNCIL_DISTRICTS_URL = (
    "https://services1.arcgis.com/tp9wqSVX1AitKgjd/"
    "arcgis/rest/services/MA_IRZ_MAP/FeatureServer/3/"
    "query?where=1%3D1&outFields=*&outSR=4326&f=json"
)


# Read in 311

# Switch it out to wherever 311 is in data lake
catalog = intake_civis.open_redshift_catalog()

# Save the 311 table as an ibis object
expr = catalog.public.import311.to_ibis()


def prep_311_data(expr):

    # Grab last 6 month's worth of data
    filtered_expr = expr[
        (expr.createddate > (ibis.now() - ibis.interval(months=6)))
        & (expr.requesttype != "Homeless Encampment")
    ]

    keep_cols = ["srnumber", "requesttype", "createddate", "longitude", "latitude"]

    filtered_expr = filtered_expr[keep_cols]
    df = filtered_expr.execute()

    return df


def make_gdf_spatial_join_to_geography(df, GEOG_URL):
    # Change data types
    df = df.assign(createddate=pandas.to_datetime(df.createddate))

    # Make a gdf
    gdf = geopandas.GeoDataFrame(
        df, crs=WGS84, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
    )

    # Import CDs dataset
    keep = ["District", "NAME", "geometry"]
    cd = geopandas.read_file(GEOG_URL)[keep]

    # Spatial join
    m1 = geopandas.sjoin(
        gdf.to_crs(WGS84), cd.to_crs(WGS84), how="inner", op="intersects"
    ).drop(columns="index_right")

    m1 = m1.reset_index(drop=True)

    return m1


def aggregate_by_category(df):

    # Groupby CD and 311 request type
    group_cols = ["District", "Name", "requesttype"]

    df = df.groupby(group_cols).agg({"srnumber": "count"}).reset_index()

    return df


df = prep_311_data(expr)
gdf = make_gdf_spatial_join_to_geography(df, COUNCIL_DISTRICTS_URL)
final = aggregate_by_category(gdf)

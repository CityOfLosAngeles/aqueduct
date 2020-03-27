"""
Run this once to set the schema for US county-level data
"""
import geopandas as gpd
import pandas as pd


# Functions to be used
def coerce_fips_integer(df):
    def integrify(x):
        return int(float(x)) if not pd.isna(x) else None

    cols = [
        "fips",
    ]

    new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}

    return df.assign(**new_cols)


def correct_county_fips(row):
    if len(str(row.fips)) == 5:
        return str(row.fips)
    elif row.fips is not None:
        return "0" + str(row.fips)
    elif row.fips is None:
        return ""


# Bring in NYT US county level data
# pin this to https://github.com/nytimes/covid-19-data/commit/80f9cc25057e64750db23460c8c9dcbe3fe4b577
nyt_county_url = (
    "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
)
county = pd.read_csv(nyt_county_url)


def clean_nyt_county(df):
    keep_cols = ["date", "county", "state", "fips", "cases", "deaths"]

    df = df[keep_cols]

    # Coerce fips into integer, then convert to string
    df = coerce_fips_integer(df)

    df["fips"] = df.apply(correct_county_fips, axis=1)

    df["date"] = pd.to_datetime(df.date)

    return df


county = clean_nyt_county(county)

# Add JHU data that we were missing for 3/25 - 3/27
jhu325 = gpd.read_file(
    "s3://public-health-dashboard/jhu_covid19/jhu_feature_layer_3_25_2020.geojson"
)
jhu326 = gpd.read_file(
    "s3://public-health-dashboard/jhu_covid19/jhu_feature_layer_3_26_2020.geojson"
)
jhu327 = gpd.read_file(
    "s3://public-health-dashboard/jhu_covid19/jhu_feature_layer_3_27_2020.geojson"
)

jhu325["date"] = "3/25/2020"
jhu326["date"] = "3/26/2020"
jhu327["date"] = "3/27/2020"

jhu1 = jhu325.append(jhu326).append(jhu327)
jhu1["date"] = pd.to_datetime(jhu1.date)


# Bring in the NYT's way of naming geographies, use FIPS to merge
nyt_geog = county[county.fips.notna()][["fips", "county", "state"]].drop_duplicates()

nyt_geog = coerce_fips_integer(nyt_geog)
nyt_geog["fips"] = nyt_geog.apply(correct_county_fips, axis=1)


# Clean JHU data from 3/25 - 3/27
def clean_jhu_county(df):
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
    # There are some values with no FIPS, which were all state observations.
    # Drop them, use an inner join for merge.
    df = pd.merge(df, nyt_geog, on="fips", how="inner", validate="m:1")

    # Only keep certain columns and rename them to match NYT schema
    drop_cols = ["Province_State", "Country_Region"]

    df = df.drop(columns=drop_cols)

    return df


jhu1 = clean_jhu_county(jhu1)


# Append everything just once
us_county_time_series = county.append(jhu1, sort=False)


def fill_missing_stuff(df):
    for col in ["Lat", "Lon"]:
        df[col] = df.groupby(["fips", "county", "state"])[col].transform("max")

    # There's a FIPS that isn't caught because of a tilde for Dona Ana, New Mexico.
    df["fips"] = df.apply(
        lambda row: "35013" if ("Ana" in row.county) & (row.fips == "") else row.fips,
        axis=1,
    )

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
    ]

    df = df.reindex(columns=col_order).sort_values(["fips", "date"])

    # Set data types for cases and deaths? Seems ok for now....
    for col in ["incident_rate", "people_tested"]:
        df[col] = df[col].astype(float)

    return df


us_county_time_series = fill_missing_stuff(us_county_time_series)

## NEED TO EXPORT THIS SOMEWHERE?
us_county_time_series.to_csv()

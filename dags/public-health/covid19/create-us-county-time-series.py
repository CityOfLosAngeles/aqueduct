"""
Run this once to set the schema for US county-level data
This grabs NYT data up to 3/30 and sample JHU data for 3/30 to append
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
NYT_330_COMMIT = "99b30cbf4181e35bdcc814e2b29671f38d7860a7"
NYT_COUNTY_URL = (
    f"https://raw.githubusercontent.com/nytimes/covid-19-data/{NYT_330_COMMIT}/"
    "us-counties.csv"
)
county = pd.read_csv(NYT_COUNTY_URL)


def clean_nyt_county(df):
    keep_cols = ["date", "county", "state", "fips", "cases", "deaths"]
    df = df[keep_cols]
    df["date"] = pd.to_datetime(df.date)
    # Create new columns to store what JHU reports
    df["incident_rate"] = np.nan
    df["people_tested"] = np.nan
    return df


county = clean_nyt_county(county)


# Add JHU data for 3/30
jhu_dfs = {}
bucket_name = "public-health-dashboard"

for d in range(30, 31):
    key_name = f"jhu3{d}"
    fill_in_date = f"3/{d}/2020"
    data = gpd.read_file(
        f"s3://{bucket_name}/jhu_covid19/jhu_feature_layer_3_{d}_2020.geojson"
    )
    data["date"] = fill_in_date
    jhu_dfs[key_name] = data


jhu1 = jhu_dfs["jhu330"]
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


jhu1 = clean_jhu_county(jhu1)


# Append everything just once
us_county = county.append(jhu1, sort=False)


def fill_missing_stuff(df):
    for col in ["Lat", "Lon"]:
        df[col] = df.groupby(["fips", "county", "state"])[col].transform("max")

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

    df = df.reindex(columns=col_order).sort_values(
        ["state", "county", "fips", "date", "cases"]
    )

    # Set data types for cases and deaths? Seems ok for now....
    for col in ["incident_rate", "people_tested"]:
        df[col] = df[col].astype(float)

    # Drop duplicates
    # Either: (1) values are updated throughout the day, or
    # (2) slight discrepancies between NYT and JHU.
    # Regardless, take the max value for cases and deaths for each date.
    group_cols = ["state", "county", "fips", "date"]
    for col in ["cases", "deaths"]:
        df[col] = df.groupby(group_cols).transform("max")

    df = df.drop_duplicates(subset=group_cols)

    return df


us_county = fill_missing_stuff(us_county)


# Fix values with missing lat/lon (NYT breaks out Kansas City, MO and NYC, NY)
fix_me = us_county.loc[us_county.Lat.isna()]
rest_of_df = us_county.loc[us_county.Lat.notna()]

fix_latitude = {
    "Kansas City": 39.0997,
    "New York    City": 40.7128,
}

fix_longitude = {
    "Kansas City": -94.5786,
    "New York City": -74.0060,
}

fix_me["Lat"] = fix_me.county.map(fix_latitude)
fix_me["Lon"] = fix_me.county.map(fix_longitude)

us_county = rest_of_df.append(fix_me, sort=False).reset_index(drop=True)


# Export as csv
us_county.to_csv(f"s3://{bucket_name}/jhu_covid19/county_time_series_330.csv")

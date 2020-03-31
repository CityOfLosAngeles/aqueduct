"""
Grab the 'static' portion of time-series from NYT
and add JHU DAG to this.
"""
import geopandas as gpd
import numpy as np
import pandas as pd


# General function
"""
OOPS, is df the right thing to put as arg in create_append_county_time_series?
I want to read in 2 different dataframes
Append, pass df into some functions to clean up, then spit out a cleaned df to export
"""
def create_append_county_time_series(df):
    # Import static time-series csv item
    # ITEM IS IN UTC...which displays the dates wrong...
    # should show 3/30 as last date, but shows 3/29 right now
    # http://lahub.maps.arcgis.com/home/item.html?id=705a232fae6f4777b8bce98741fe590e
    # --> Replace this with importing from item ID?
    old_ts = pd.read_csv('s3://public-health-dashboard/jhu_covid19/county_time_series_330.csv')
    # The dates in csv are correct, but not once csv read by ESRI

    # Create crosswalk using NYT geography columns to clean up JHU schema
    NYT_330_COMMIT = ""
    NYT_COUNTY_URL = (
        f"https://raw.githubusercontent.com/nytimes/covid-19-data/{NYT_330_COMMIT}/"
        "us-counties.csv"
    )
    county = pd.read_csv(NYT_COUNTY_URL)
    nyt_geog = county[county.fips.notna()][["fips", "county", "state"]].drop_duplicates()
    nyt_geog = coerce_fips_integer(nyt_geog)
    nyt_geog["fips"] = nyt_geog.apply(correct_county_fips, axis=1)


    # Import JHU feature layer
    # --> Replace with importing JHU data
    # JHU county data: https://www.arcgis.com/home/item.html?id=628578697fb24d8ea4c32fa0c5ae1843
    #jhu =

    # Create localized then normalized date column
    jhu['date'] = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")

    jhu = clean_jhu_county(jhu)

    # Append everything
    us_county = old_ts.append(jhu, sort=False)

    # Clean up full dataset by filling in missing values and dropping duplicates
    us_county = fill_missing_stuff(us_county)

    # Get state totals
    us_county = us_state_totals(us_county)

    # Clean up column types again before exporting?
    us_county = fix_column_dtypes(us_county)

    # Export and overwrite this one (http://lahub.maps.arcgis.com/home/item.html?id=705a232fae6f4777b8bce98741fe590e)
    return us_county


# Sub-functions to be used
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
    elif len(str(row.fips)) == 4:
        return "0" + str(row.fips)
    elif (row.fips is None) or (row.fips==np.nan):
        return ""


# Clean JHU data to match NYT schema
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


def fill_missing_stuff(df):
    for col in ["Lat", "Lon"]:
        df[col] = df.groupby(["fips", "county", "state"])[col].transform("max")

    # Fix values with missing lat/lon (NYT breaks out Kansas City, MO and NYC, NY)
    fix_me = df.loc[df.Lat.isna()]
    no_fix_needed = df.loc[df.Lat.notna()]

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

    df = no_fix_needed.append(fix_me, sort=False).reset_index(drop=True)

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

    # Drop duplicates and keep last observation
    group_cols = ["state", "county", "fips", "date"]
    df = df.drop_duplicates(subset=group_cols, keep="last")

    return df


# Calculate US State totals
def us_state_totals(df):

    state_grouping_cols = ['state', 'date']

    state_totals = df.groupby(state_grouping_cols).agg(
        {'cases':'sum', 'deaths':'sum'})

    state_totals.rename(columns = {'cases': 'state_cases',
                                  'deaths': 'state_deaths'}, inplace = True)

    df = pd.merge(df, state_totals, on = state_grouping_cols)

    return df

# Somehow, column types get all messed up after subsetting, do 1 final pass
def fix_column_dtypes(df):
    def coerce_integer(df):
        def integrify(x):
            return int(float(x)) if not pd.isna(x) else None

        cols = [
            "cases",
            "deaths",
            "state_cases",
            "state_deaths"
        ]

        new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}

        return df.assign(**new_cols)

    df["date"] = pd.to_datetime(df.date)

    return df

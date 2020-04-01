"""
Run this once to set the schema for US county-level data
This grabs NYT data up to 3/31 and sample JHU data for 3/30 to append
"""
import geopandas as gpd
import numpy as np
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
    if len(row.fips) == 5:
        return row.fips
    elif (len(row.fips) == 4) and (row.fips != "None"):
        return "0" + row.fips
    elif row.fips == "None":
        return ""


# (1) Bring in NYT US county level data and clean
NYT_COMMIT = "baeca648aefa9694a3fc8f2b3bd3f797937aa1c5"
NYT_COUNTY_URL = (
    f"https://raw.githubusercontent.com/nytimes/covid-19-data/{NYT_COMMIT}/"
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
    # Fix column type
    df = coerce_fips_integer(df)
    df["fips"] = df.fips.astype(str)
    df["fips"] = df.apply(correct_county_fips, axis=1)
    return df


county = clean_nyt_county(county)


# (2) Add JHU data for 3/30 and clean up geography
bucket_name = "public-health-dashboard"
jhu = gpd.read_file(
    f"s3://{bucket_name}/jhu_covid19/jhu_feature_layer_3_30_2020.geojson"
)
jhu["date"] = "3/30/2020"
jhu["date"] = pd.to_datetime(jhu.date)


# Bring in the NYT's way of naming geographies, use FIPS to merge
nyt_geog = county[county.fips != ""][["fips", "county", "state"]].drop_duplicates()


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


jhu = clean_jhu_county(jhu)


# (3) Append NYT and JHU and fill in missing county lat/lon
us_county = county.append(jhu, sort=False)


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


us_county = fill_missing_stuff(us_county)


# (4) Import crosswalk from JHU to fix missing state lat/lon
JHU_COMMIT = "376119aa4b3dbc37b863ac11d4984e480e81227b"
JHU_LOOKUP_URL = (
    f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/{JHU_COMMIT}/"
    "csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
)

# Fix values with missing lat/lon (NYT breaks out Kansas City, MO and NYC, NY)
jhu_lookup = pd.read_csv(JHU_LOOKUP_URL)
cols_to_keep = ["Province_State", "Lat", "Long_"]
jhu_lookup = jhu_lookup[jhu_lookup.Country_Region == "US"][cols_to_keep]
jhu_lookup.rename(columns={"Province_State": "state", "Long_": "Lon"}, inplace=True)

# Fix the different types of missing lat/lon
cond1 = "us_county.Lat.isna()"
cond2 = "us_county.county.notna()"
fix_county = us_county[(cond1) and (cond2) and (us_county.county != "Unknown")]
fix_state = us_county[(cond1) and (cond2) and (us_county.county == "Unknown")]
rest_of_df = us_county[us_county.Lat.notna()]

fix_county_lat = {
    "Kansas City": 39.0997,
    "New York City": 40.7128,
}
fix_county_lon = {
    "Kansas City": -94.5786,
    "New York City": -74.0060,
}
fix_county["Lat"] = fix_county.county.map(fix_county_lat)
fix_county["Lon"] = fix_county.county.map(fix_county_lon)

fix_state = pd.merge(
    fix_state.drop(columns=["Lat", "Lon"]), jhu_lookup, on="state", how="left"
)


# Append the fixes together
us_county = (
    rest_of_df.append(fix_county, sort=False).append(fix_state).reset_index(drop=True)
)
us_county = us_county.sort_values(
    ["fips", "state", "county", "date", "cases", "Lat", "Lon"],
    ascending=[True, True, True, True, True, True, True],
).drop_duplicates(subset=["fips", "state", "county", "date"], keep="first")


# (5) Calculate US State totals
def us_state_totals(df):

    state_grouping_cols = ["state", "date"]

    state_totals = df.groupby(state_grouping_cols).agg(
        {"cases": "sum", "deaths": "sum"}
    )

    state_totals.rename(
        columns={"cases": "state_cases", "deaths": "state_deaths"}, inplace=True
    )

    df = pd.merge(df, state_totals, on=state_grouping_cols)

    return df


us_county = us_state_totals(us_county)


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


us_county = calculate_change(us_county)


# (7) Fix column types before exporting
def fix_column_dtypes(df):
    df["date"] = pd.to_datetime(df.date)

    # integrify wouldn't work?
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
    df["date"] = pd.to_datetime(df.date)

    return df


us_county = fix_column_dtypes(us_county)

print(us_county.dtypes)

# Export as csv
us_county.to_csv(
    f"s3://{bucket_name}/jhu_covid19/county_time_series_331.csv", index=False
)

"""
Set the schema for US county-level data
using only JHU data.
Grab historical time-series data from JHU GitHub
and append 4/8 JHU feature layer.
"""
import geopandas as gpd
import numpy as np
import pandas as pd


# General functions to be used
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


def rename_geog_cols(df):
    """
    # Rename geography columns to be the same as future schemas
    """
    df.rename(
        columns={"Long_": "Lon", "FIPS": "fips"}, inplace=True,
    )
    return df


def coerce_fips_integer(df):
    def integrify(x):
        return int(float(x)) if not pd.isna(x) else None

    cols = [
        "fips",
    ]

    new_cols = {c: df[c].apply(integrify, convert_dtype=False) for c in cols}

    return df.assign(**new_cols)


def correct_county_fips(row):
    if (len(row.fips) == 4) and (row.fips != "None"):
        return "0" + row.fips
    elif row.fips == "None":
        return ""
    else:
        return row.fips


sort_cols = ["state", "county", "fips", "date"]

# (1) Bring in JHU historical county time-series data and clean
bucket_name = "public-health-dashboard"

JHU_COMMIT = "5acaa8f852178af7f0c9eebfd0d5db746bbb2305"

CASES_URL = (
    f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/{JHU_COMMIT}/"
    "csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
)

DEATHS_URL = (
    f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/{JHU_COMMIT}/"
    "csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
)


def import_historical():

    cases = pd.read_csv(CASES_URL)
    deaths = pd.read_csv(DEATHS_URL)

    # melt cases
    id_vars, dates = parse_columns(cases)
    df = pd.melt(
        cases, id_vars=id_vars, value_vars=dates, value_name="cases", var_name="date",
    )

    # melt deaths
    id_vars, dates = parse_columns(deaths)
    deaths_df = pd.melt(deaths, id_vars=id_vars, value_vars=dates, value_name="deaths")

    # join
    df = (
        df.assign(deaths=deaths_df.deaths, state=df.Province_State, county=df.Admin2,)
        .pipe(rename_geog_cols)
        .pipe(coerce_fips_integer)
    )

    # Fix fips and make it a 5 digit string
    df["fips"] = df.fips.astype(str)
    df["fips"] = df.apply(correct_county_fips, axis=1)

    for col in ["state", "county", "fips"]:
        df[col] = df[col].fillna("")

    # Make sure date is UTC
    df["date"] = pd.to_datetime(df.date)
    df = df.assign(date=df.date.dt.tz_localize("UTC"))

    drop_col = [
        "UID",
        "iso2",
        "iso3",
        "code3",
        "Province_State",
        "Country_Region",
        "Admin2",
    ]

    df = df.drop(columns=drop_col)
    return df.sort_values(sort_cols).reset_index(drop=True)


# (2) Bring in current JHU feature layer and clean
def clean_jhu_county(df):
    # Only keep certain columns and rename them to match NYT schema
    keep_cols = [
        "Province_State",
        "Admin2",
        "Lat",
        "Long_",
        "Confirmed",
        "Deaths",
        "FIPS",
        "Incident_Rate",
        "People_Tested",
        "date",
        "Combined_Key",
    ]

    df = df[keep_cols]

    df.rename(
        columns={
            "Deaths": "deaths",
            "FIPS": "fips",
            "Long_": "Lon",
            "Province_State": "state",
            "Admin2": "county",
            "People_Tested": "people_tested",
            "Incident_Rate": "incident_rate",
        },
        inplace=True,
    )

    # Use floats
    for col in ["people_tested", "incident_rate"]:
        df[col] = df[col].astype(float)

    # Fix fips
    df = df.pipe(coerce_fips_integer)
    df["fips"] = df.fips.astype(str)
    df["fips"] = df.apply(correct_county_fips, axis=1)
    for col in ["state", "county", "fips"]:
        df[col] = df[col].fillna("")

    return df


# (3) Fill in missing stuff after appending
def fill_missing_stuff(df):
    # Standardize how New York City shows up
    df["county"] = df.apply(
        lambda row: "New York City" if row.fips == "36061" else row.county, axis=1
    )

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
    for col in ["cases", "deaths"]:
        df[col] = df.groupby(sort_cols)[col].transform("max")

    df = (
        df.drop_duplicates(subset=sort_cols, keep="last")
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )

    return df


# (4) Calculate US state totals
def us_state_totals(df):
    state_grouping_cols = ["state", "date"]

    state_totals = df.groupby(state_grouping_cols).agg(
        {"cases": "sum", "deaths": "sum"}
    )
    state_totals = state_totals.rename(
        columns={"cases": "state_cases", "deaths": "state_deaths"}
    )

    df = pd.merge(df, state_totals, on=state_grouping_cols,)

    return df.sort_values(sort_cols).reset_index(drop=True)


# (5) Calculate change in caseloads from prior day
def calculate_change(df):
    for col in ["cases", "deaths"]:
        new_col = f"new_{col}"
        county_group_cols = ["state", "county", "fips"]
        df[new_col] = (
            df.sort_values(sort_cols)
            .groupby(county_group_cols)[col]
            .apply(lambda row: row - row.shift(1))
        )
        # First obs will be NaN, but the change in caseload is just the # of cases.
        df[new_col] = df[new_col].fillna(df[col])

    for col in ["state_cases", "state_deaths"]:
        new_col = f"new_{col}"
        state_group_cols = ["state"]
        df[new_col] = (
            df.sort_values(sort_cols)
            .groupby(state_group_cols)[col]
            .apply(lambda row: row - row.shift(1))
        )
        df[new_col] = df[new_col].fillna(df[col])

    return df


# (6) Fix column types before exporting
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

    # Counties with zero cases are included in Jan/Feb/Mar.
    # Makes CSV huge. Drop these.
    df["obs"] = df.groupby(["state", "county", "fips"]).cumcount() + 1
    df["nonzero_case"] = df.apply(
        lambda row: row.obs if row.cases > 0 else np.nan, axis=1
    )
    df["first_case"] = df.groupby(["state", "county", "fips"])[
        "nonzero_case"
    ].transform("min")
    df = df[df.obs >= df.first_case]
    df = df.drop(columns=["obs", "nonzero_case", "first_case"])

    df = (
        df.pipe(coerce_integer)
        .reindex(columns=col_order)
        .sort_values(["state", "county", "fips", "date", "cases"])
    )

    print(df.dtypes)
    return df.sort_values(sort_cols).reset_index(drop=True)


# Create our time-series file
cases = pd.read_csv(CASES_URL)
deaths = pd.read_csv(DEATHS_URL)

# (1) Bring in JHU historical county time-series data and clean
df = import_historical()

# (2) Bring in current JHU feature layer and clean
jhu = gpd.read_file(
    f"s3://{bucket_name}/jhu_covid19/jhu_feature_layer_4_8_2020.geojson"
)
jhu["date"] = "4/8/2020"
jhu["date"] = pd.to_datetime(jhu.date).dt.tz_localize("UTC")

jhu = clean_jhu_county(jhu)

# Append datasets
ts = df.append(jhu, sort=False)

# (3) Fill in missing stuff after appending
us_county = fill_missing_stuff(ts)

# (4) Calculate US state totals
us_county = us_state_totals(us_county)

# (5) Calculate change in caseloads from prior day
us_county = calculate_change(us_county)

# (6) Fix column types before exporting
final = fix_column_dtypes(us_county)

# Export as csv
final.to_csv(f"s3://{bucket_name}/jhu_covid19/county_time_series_408.csv", index=False)

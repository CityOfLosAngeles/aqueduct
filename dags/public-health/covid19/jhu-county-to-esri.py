"""
Grab the 'static' portion of time-series from NYT
and add JHU DAG to this.
"""
import os
from datetime import datetime, timedelta

import arcgis
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from arcgis.gis import GIS

# General function
TIME_SERIES_FEATURE_ID = "4e0dc873bd794c14b7bd186b4b5e74a2"
JHU_FEATURE_ID = "628578697fb24d8ea4c32fa0c5ae1843"
MSA_FEATURE_ID = "b37e229b71dc4c65a479e4b5912ded66"
max_record_count = 6_000_000


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

    # (2) Bring in current JHU feature layer and clean
    gis_item = gis.content.get(JHU_FEATURE_ID)
    layer = gis_item.layers[0]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)
    # Drop some ESRI faf
    jhu = sdf.drop(columns=["OBJECTID", "SHAPE"])

    # Create localized then normalized date column
    jhu["date"] = pd.Timestamp.now(tz="US/Pacific").normalize().tz_convert("UTC")
    jhu = clean_jhu_county(jhu)

    # (3) Fill in missing stuff after appending
    us_county = old_ts.append(jhu, sort=False)
    us_county = fill_missing_stuff(us_county)

    # (4) Calculate US state totals
    us_county = us_state_totals(us_county)

    # (5) Calculate change in caseloads from prior day
    us_county = calculate_change(us_county)

    # (6) Fix column types before exporting
    final = fix_column_dtypes(us_county)

    # (7) Write to CSV and overwrite the old feature layer.
    time_series_filename = "/tmp/jhu-county-time-series.csv"
    final.to_csv(time_series_filename)
    gis_item = gis.content.get(TIME_SERIES_FEATURE_ID)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(time_series_filename)
    gis_layer_collection.manager.update_definition({"maxRecordCount": max_record_count})

    return True


# Sub-functions to be used
def coerce_fips_integer(df):
    def integrify(x):
        return int(float(x)) if not pd.isna(x) else None

    cols = ["fips"]

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
            "Confirmed": "cases",
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

    df = pd.merge(
        df.drop(columns=["state_cases", "state_deaths"]),
        state_totals,
        on=state_grouping_cols,
    )

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

    df = (
        df.pipe(coerce_integer)
        .reindex(columns=col_order)
        .sort_values(["state", "county", "fips", "date", "cases"])
    )

    print(df.dtypes)

    return df


# T2 Sub-functions
def subset_msa(df):
    # 5 MSAs to plot: NYC, SF_SJ, SEA, DET, LA
    df = df[
        df.cbsatitle.str.contains("Los Angeles")
        | df.cbsatitle.str.contains("New York")
        | df.cbsatitle.str.contains("San Francisco")
        | df.cbsatitle.str.contains("San Jose")
        | df.cbsatitle.str.contains("Seattle")
        | df.cbsatitle.str.contains("Detroit")
    ]

    def new_categories(row):
        if ("San Francisco" in row.cbsatitle) or ("San Jose" in row.cbsatitle):
            return "SF/SJ"
        elif "Los Angeles" in row.cbsatitle:
            return "LA/OC"
        elif "New York" in row.cbsatitle:
            return "NYC"
        elif "Seattle" in row.cbsatitle:
            return "SEA"
        elif "Detroit" in row.cbsatitle:
            return "DET"

    df = df.assign(msa=df.apply(new_categories, axis=1))

    return df


def update_msa_dataset(**kwargs):
    """
    Update MSA dataset
    ref gh/aqueduct#199
    takes the previous step data, aggegrates by MSA
    replaces feature layer.
    """
    arcconnection = BaseHook.get_connection("arcgis")
    arcuser = arcconnection.login
    arcpassword = arcconnection.password
    gis = GIS("http://lahub.maps.arcgis.com", username=arcuser, password=arcpassword)

    # (1) Load time series data from ESRI
    gis_item = gis.content.get(TIME_SERIES_FEATURE_ID)
    layer = gis_item.layers[0]
    sdf = arcgis.features.GeoAccessor.from_layer(layer)

    county_df = sdf.drop("SHAPE", axis=1)

    # MSA county - CBSA crosswalk with population crosswalk
    CROSSWALK_URL = (
        f"https://raw.githubusercontent.com/CityOfLosAngeles/aqueduct/master/dags/"
        "public-health/covid19/msa_county_pop_crosswalk.csv"
    )

    pop = pd.read_csv(CROSSWALK_URL, dtype={"county_fips": "str", "cbsacode": "str"},)

    pop = pop[["cbsacode", "cbsatitle", "population", "county_fips"]]
    pop = subset_msa(pop)

    # merge
    final_df = pd.merge(
        county_df,
        pop,
        left_on="fips",
        right_on="county_fips",
        how="inner",
        validate="m:1",
    )

    # Aggregate by MSA
    group_cols = ["msa", "population", "date"]
    msa = (
        final_df.groupby(group_cols)
        .agg({"cases": "sum", "deaths": "sum"})
        .reset_index()
    )

    # Calculate rate per 1M
    rate = 1_000_000
    msa = msa.assign(
        cases_per_1M=msa.cases / msa.population * rate,
        deaths_per_1M=msa.deaths / msa.population * rate,
        # Can't keep CBSA code, since SF/SJ are technically 2 CBSAs.
        # Keep column because feature layer already has it, set it to ""
        cbsacode="",
    )

    MSA_FILENAME = "/tmp/msa_v1.csv"
    msa.to_csv(MSA_FILENAME, index=False)
    gis_item = gis.content.get(MSA_FEATURE_ID)
    gis_layer_collection = arcgis.features.FeatureLayerCollection.fromitem(gis_item)
    gis_layer_collection.manager.overwrite(MSA_FILENAME)
    gis_layer_collection.manager.update_definition({"maxRecordCount": max_record_count})

    os.remove(MSA_FILENAME)
    return


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

t2 = PythonOperator(
    task_id="update-msa-data",
    provide_context=True,
    python_callable=update_msa_dataset,
    op_kwargs={},
    dag=dag,
)

t1 > t2

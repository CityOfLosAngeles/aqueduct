from arcgis import GIS
import pandas as pd
import re
import os
import json
import math
from sodapy import Socrata as sc
import civis
import datetime


USERNAME = os.environ["LAHUB_ACC_USERNAME"]
PASSWORD = os.environ["LAHUB_ACC_PASSWORD"]
SOCRATA_USER = os.environ["SOCRATA_ACC_USERNAME"]
SOCRATA_PASS = os.environ["SOCRATA_ACC_PASSWORD"]
pattern = re.compile("[Rr]efresh [Rr]ate")
pattern2 = re.compile("[Rr]efresh [Rr]ate.*")


def make_connection(USERNAME, PASSWORD):
    """
    Pull down stuff from Geohub
    """
    geohub = GIS("https://lahub.maps.arcgis.com", USERNAME, PASSWORD)
    content = geohub.content.search(query=None, max_items=9999)
    # above is the largest call to max_items before you gotta do it in batches i think,
    # currently under 8k so not aproblem yet, just worth noting
    content_df = pd.DataFrame(content)
    df = content_df[
        [
            "url",
            "access",
            "created",
            "description",
            "id",
            "title",
            "owner",
            "type",
            "modified",
            "numViews",
            "licenseInfo",
        ]
    ]  # not every possible column makes it through
    # without doing another (very slow) query based on user_id, but enough.
    # ADD NEW COLUMNS
    geohub_filter = groups_filter(geohub)
    df1 = df[df["id"].isin(geohub_filter)]
    return df1


def fix_the_columns(df):
    """
    Ensure that column names match up with columns here
    https://data.lacity.org/A-Well-Run-City/All-Public-Datasets-Inventory-and-Metrics/wxaq-cvn5
    """

    df = df.rename(
        columns={
            "url": "URL",
            "access": "Public",
            "type": "Type",
            "numViews": "Visits",
            "title": "Name",
            "id": "U ID",
            "owner": "Owner",
            "created": "Creation Date",
            "cleaned_description": "Description",
            "modified": "Last Updated",
            "licenseInfo": "License",
        }
    )

    df = df.reindex(
        columns=[
            "URL",
            "Public",
            "Derived View",
            "U ID",
            "Department",
            "Domain",
            "Type",
            "Name",
            "Description",
            "Visits",
            "Creation Date",
            "Last Update Date (data)",
            "Category",
            "Owner UID",
            "License",
            "Publication Stage",
            "Data Provided By",
            "Source Link",
            "Routing & Approval",
            "api_endpoint",
            "System",
            "Last Updated",
            "What geographic unit is the data collected?",
            "Refresh rate",
            "Does this data have a Location column? (Yes or No)",
            "geometryType",
            "hasAttachments",
            "Owner",
        ]
    )

    return df


def remove_html_tags(data):
    """
    Description file had lots of residual HTML tags
    """
    p = re.compile(r"<.*?>")
    return p.sub("", data)


def findRefreshRate(d):
    """
    Does description text contain the words refresh rate? 1 for yes, 0 for no
    """
    if re.search(pattern, d):
        return 1
    else:
        return 0


def extract_refresh(x, pattern2):
    """
    if has refresh rate, extracts the rest of the text.
    """
    if x["Has Refresh Rate"] == 1:
        return re.search(pattern2, x["cleaned_description"]).group(0)
    else:
        return "N/A"


def clean_refresh(x):
    for row in x:
        try:
            if "As Needed" in x["Refresh rate_X"]:
                return "As Needed"
            elif "Never" in x["Refresh rate_X"]:
                return "As Needed"
            elif "One-time" in x["Refresh rate_X"]:
                return "As Needed"
            elif "Annual" in x["Refresh rate_X"]:
                return "Annual"
            elif "Semi-Annual" in x["Refresh rate_X"]:
                return "Semi-Annual"
            elif "Quarterly" in x["Refresh rate_X"]:
                return "Quarterly"
            elif "Monthly" in x["Refresh rate_X"]:
                return "Monthly"
            elif "Weekly" in x["Refresh rate_X"]:
                return "Weekly"
            elif "Daily" in x["Refresh rate_X"]:
                return "Daily"
            elif "Hourly" in x["Refresh rate_X"]:
                return "Hourly"
            else:
                return ""
        except:
            return ""


def make_it_la_hub(x):
    """
    To make domain row all the below string
    """
    return "lahub.maps.arcgis.com"


def make_it_TRUE(x):
    """
    To make domain row all the below string
    """
    return "TRUE"


def make_it_FALSE(x):
    """
    To make domain row all the below string
    """
    return "FALSE"


def fix_the_time(x):
    """
    Change time from UNIX to a timestamp
    """
    x = int(x)
    return datetime.datetime.utcfromtimestamp(x / 1000).strftime("%Y-%m-%d %H:%M:%S")


def groups_filter(account):
    geogroups = [
        "1e780996f0d842828ae9495716988369",
        "b46a7f3494754d2499adeef9dfb9a003",
        "e4b84b5bb5244e43b9a4085f5984bef9",
        "9b86ac4e38974d1c88aff2e97724b9b7",
        "9db43b592f4b4a3c864954e5b4b5a6e6",
    ]
    geohub_data = pd.DataFrame()
    for group in geogroups:
        target_group = account.groups.get(group)
        target_content = target_group.content()
        target_content_data = pd.DataFrame(target_content)
        geohub_data = geohub_data.append(target_content_data)
        geohub_ids = geohub_data.id
        return geohub_ids


def generate_geohub_df():
    """
    can call from command line
    """

    df = make_connection(USERNAME, PASSWORD,)
    df["description"] = df["description"].astype(str)
    df["Has Refresh Rate"] = df["description"].apply(findRefreshRate)
    df["cleaned_description"] = df.description.apply(remove_html_tags)
    df = df.drop(columns=["description"])
    access_filter = df.access == "public"  # we only want publically accessable datasets
    df = df[access_filter]
    df["Refresh rate_X"] = df.apply(lambda x: extract_refresh(x, pattern2), axis=1)
    df["Refresh rate"] = clean_refresh(df)
    df = fix_the_columns(df)
    df["Last Updated"] = df["Last Updated"].apply(lambda x: fix_the_time(x))
    df["Creation Date"] = df["Creation Date"].apply(lambda x: fix_the_time(x))
    df["Last Update Date (data)"] = df["Last Updated"]
    df["Domain"] = df["Domain"].apply(lambda x: make_it_la_hub(x))
    df["Public"] = df["Public"].apply(lambda x: make_it_TRUE(x))
    df["Derived View"] = df["Derived View"].apply(lambda x: make_it_FALSE(x))

    civis.io.dataframe_to_civis(
        df, "City of Los Angeles - Redshift", "scratch.data_freshness_geohub"
    )
    return


## Start of Socrata Stuff
def update_socrata_data():
    """
    This updates the dataset on socrata
    """
    query = "https://data.lacity.org/resource/kqy8-vgbp.json?"
    raw_data = pd.read_json(query)
    geohub_raw_data = civis.io.read_civis(
        "scratch.data_freshness_geohub",
        "City of Los Angeles - Redshift",
        use_pandas=True,
    )

    geohub_data = geohub_raw_data[
        [
            "url",
            "u id",
            "department",
            "name",
            "description",
            "visits",
            "creation date",
            "last update date (data)",
            "category",
            "license",
            "api_endpoint",
            "refresh rate",
            "owner",
        ]
    ].copy()
    raw_data.head()

    raw_data.drop(
        [
            "publication_stage",
            "public",
            "source_link",
            "derived_view",
            "dataset_link",
            "keywords",
            "type",
            "what_geographic_unit_is_the_data_collected",
            "does_this_data_have_a_location_column_yes_or_no",
            "domain",
            "data_provided_by",
            "owner_uid",
        ],
        axis=1,
        inplace=True,
    )

    geohub_data.columns = [
        "api_endpoint",
        "u_id",
        "department",
        "name",
        "description",
        "visits",
        "creation_date",
        "last_update_date_data",
        "category",
        "license",
        "api_endpointz",
        "refresh_rate",
        "owner",
    ]
    geohub_data = geohub_data[
        [
            "u_id",
            "department",
            "name",
            "description",
            "visits",
            "creation_date",
            "last_update_date_data",
            "category",
            "license",
            "api_endpoint",
            "refresh_rate",
            "owner",
        ]
    ]

    geohub_data["downloads"] = 0
    geohub_data = geohub_data[
        [
            "u_id",
            "department",
            "name",
            "description",
            "visits",
            "creation_date",
            "last_update_date_data",
            "category",
            "downloads",
            "license",
            "api_endpoint",
            "refresh_rate",
            "owner",
        ]
    ]
    geohub_data.head()

    all_data = raw_data.append(geohub_data)
    all_data.to_csv("All_Public_Data_Inventory.csv", index=False)
    all_data.head()

    all_data["last_update_date_data"] = pd.to_datetime(
        all_data["last_update_date_data"], utc=True
    ).dt.tz_convert("US/Pacific")

    all_data["creation_date"] = pd.to_datetime(
        all_data["creation_date"], utc=True
    ).dt.tz_convert("US/Pacific")

    all_data = all_data[
        [
            "name",
            "description",
            "department",
            "category",
            "license",
            "owner",
            "creation_date",
            "last_update_date_data",
            "visits",
            "downloads",
            "api_endpoint",
            "refresh_rate",
        ]
    ]

    all_data.groupby("refresh_rate").size()
    all_data["is_dataset_up_to_date"] = ""
    all_data["action_needed"] = ""
    now = pd.Timestamp.now(tz="US/Pacific")
    all_data["days_since_last_update"] = (
        pd.Timestamp.now(tz="US/Pacific") - all_data.last_update_date_data
    ).dt.days
    all_data.head()

    def action_needed(df):
        if df["refresh_rate"] == "Annual":
            if df["days_since_last_update"] < 375:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Daily":
            if df["days_since_last_update"] < 3:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Hourly":
            if df["days_since_last_update"] < 2:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Monthly":
            if df["days_since_last_update"] < 34:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Quarterly":
            if df["days_since_last_update"] < 140:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Semi-annual":
            if df["days_since_last_update"] < 190:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Weekly":
            if df["days_since_last_update"] < 10:
                return "No action needed"
            else:
                return "Contact dataset owner to update dataset"
        else:
            return "Contact dataset owner to update Refresh rate"

    def dataset_up_to_date(df):
        if df["refresh_rate"] == "Annual":
            if df["days_since_last_update"] < 375:
                return "Yes"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Daily":
            if df["days_since_last_update"] < 3:
                return "Yes"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Hourly":
            if df["days_since_last_update"] < 2:
                return "Yes"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Monthly":
            if df["days_since_last_update"] < 34:
                return "Yes"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Quarterly":
            if df["days_since_last_update"] < 140:
                return "Yes"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Semi-annual":
            if df["days_since_last_update"] < 190:
                return "Yes"
            else:
                return "Contact dataset owner to update dataset"
        elif df["refresh_rate"] == "Weekly":
            if df["days_since_last_update"] < 10:
                return "Yes"
            else:
                return "No"
        else:
            return "No"

    all_data["action_needed"] = all_data.apply(lambda row: action_needed(row), axis=1)
    all_data["is_dataset_up_to_date"] = all_data.apply(
        lambda row: action_needed(row), axis=1
    )
    all_data.head()

    all_data.drop("days_since_last_update", inplace=True, axis=1)

    all_data["creation_date"] = all_data["creation_date"].astype(str)

    all_data["last_update_date_data"] = all_data["last_update_date_data"].astype(str)

    all_data.rename(
        columns={
            "name": "Dataset Name",
            "description": "Description",
            "department": "Department",
            "license": "License",
            "owner": "Owner",
            "creation_date": "Creation Date",
            "last_update_date_data": "Last Update Date",
            "visits": "Visits",
            "downloads": "Downloads",
            "api_endpoint": "API Endpoint",
            "refresh_rate": "Refresh Rate",
            "is_dataset_up_to_date": "Is Dataset Up to Date",
            "action_needed": "Action Needed",
        },
        inplace=True,
    )

    all_data.to_csv("Data_Freshness_Report.csv", index=False)

    json_str = all_data.to_json(orient="records")

    json_data = json.loads(json_str)

    # convert all None values to ''
    for obs in json_data:
        keys = list(obs.keys())
        vals = list(obs.values())
        for i in range(len(vals)):
            if vals[i] == None:
                obs[keys[i]] = ""

    print(json_data)

    client = sc(
        domain="data.lacity.org",
        app_token="K0lgodxtUCxf7AH5Q3qegOwCJ",
        username=SOCRATA_USER,
        password=SOCRATA_PASS,
    )

    # upsert in small batches to avoid timeout
    batch_size = 1000
    n_batches = math.ceil(len(json_data) / batch_size)

    for i in range(n_batches):
        b1 = i * batch_size
        b2 = (i + 1) * batch_size
        try:
            client.upsert("5h3p-n236", json_data[b1:b2])
            print("upload succeeded for rows " + str(b1) + " to " + str(b2) + ".")
        except Exception as e:
            print("upload failed for rows " + str(b1) + " to " + str(b2) + ".")
            print(e)

    # close the client
    client.close()


if __name__ == "__main__":
    generate_geohub_df()
    update_socrata_data()

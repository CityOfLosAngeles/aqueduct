"""
Helpers functions for the socrata import
"""
import pandas as pd
import os
import civis
from typing import Tuple
from datetime import datetime
from civis import APIClient
import logging
import json
from functools import reduce
import numpy as np
import re
from collections import OrderedDict


from civis.io import file_to_civis

LOG = logging.getLogger(__name__)


def _parse_metadata(metadata: dict, paths: dict):
    out = {}
    for name, path in paths.items():
        out[name] = reduce(
            lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
            path.split("."),
            metadata,
        )
    return out


def write_and_attach_jsonvalue(
    json_value: str, name: str, client: APIClient = None,
) -> None:
    json_obj = client.json_values.post(json.dumps(json_value), name=name)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="JSONValue",
        object_id=json_obj.id,
    )


def _store_and_attach_dataset_csv(
    client: civis.APIClient, csv_path: str, filename: str
) -> int:
    """
    Given an APIClient object, a csv path, and a filename, write the csv
    to a file, attach the file as a script output, and return the file_id.

    Parameters
    ----------
    client: APIClient
        An instance of civis.APIClient.
    csv_path: str
        A string containg the path of CSV
    filename: str
        The name of the file to which data should be written.

    Returns
    -------
    int: file_id of the file stored in S3

    Side Effects
    ------------
    - Stores object passed to df argument as a .csv file in S3
    - Attaches this .csv file as a script output
    """
    file_id = civis.io.file_to_civis(csv_path, name=filename, expires_at=None)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="File",
        object_id=file_id,
    )
    return file_id


def _store_and_attach_metadata(
    client: civis.APIClient, metadata: dict, metadata_paths: dict, filename: str
) -> Tuple[int, dict]:
    """
    Given an APIClient object, metadata read from DDL, a collection of keys
    and paths within the DDL metadata, and a filename, this function:
        (1) writes the cleaned metadata fields to a JSONValue object,
        (2) writes the raw metadata fields to a file object,
        (3) attaches both to the current script as outputs, and
        (4) returns the file_id of the raw metadata and the cleaned metadata
        as a dictionary.

    Parameters
    ----------
    client: APIClient
        An instance of civis.APIClient.
    metadata: dict
        The raw metadata read from DDL.
    metadata_paths: dict
        A dictionary of the paths used to clean the metadata read from DDL.
        This should be the value of ddl_metadata_paths in configs.constants.
    filename: str
        The name of the file to which raw metadata should be written.

    Returns
    -------
    Tuple[int, dict]:
        file_id (int) of the raw metadata stored in S3, and
        cleaned_metadata (dict)

    Side Effects
    ------------
    - Stores object passed to metadata argument as a .json file in S3
    - Attaches this .json file as a script output
    - Stores cleaned metadata object as a JSONValues object
    - Attaches this JSONValues object as a script output
    """

    with open(filename, "w") as f:
        json.dump(metadata, f)
    file_id = file_to_civis(buf=filename, name=filename, expires_at=None,)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="File",
        object_id=file_id,
    )

    cleaned_metadata = _parse_metadata(metadata=metadata, paths=metadata_paths)
    for key, value in cleaned_metadata.items():
        if key.lower().endswith("updated at"):
            value = datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")
        write_and_attach_jsonvalue(json_value=value, name=key, client=client)
    return file_id, cleaned_metadata


def _read_paginated(
    client,
    dataset_id: int,
    point_columns,
    column_order,
    page_limit: int = 90000,
    size_limit: int = None,
):
    """
    Pulls in Socrata data using API Client
        (1) Creates while loop that runs for records retrieved<= size_limitwith
        (2) Starting at offset=0, pulls in number of rows specified by
            page_limit
            (a) if the import is to PostGres database, pandas string comands
                will convert socrata defined point datatype to format required
                by PostGres
            (b) For each chunk of data writes pandas df to .csv and notes csv
                name in array
        (3) Adjusts offset by page_limit and repeats until either size_limit or
            end of dataset reached
        (4) Appends all .csvs using python functions
        (5) Outputs path to appended .csv

    Parameters
    ----------
    client:
        An instance of socrata.APIClient.
    dataset_id: str
        Socrata dataset identifier
    point_columns
        An index of columns that are point types, this is used to edit the
        string of columns to be compatible with PostGres import
    column_order
        Order of columns to match metadata
    page_limit: int
        Number of records that can be pulled in chunk
    size_limit: int
        Desired max number of records

    Returns
    -------
    str:
        Path of appended .csv

    """

    df = pd.DataFrame()
    # create empty dataframe that will be used as transitionary data store

    offset = 0
    # set offset cunter to zero

    paths = []
    # create empty set that .csv paths will be appended to

    while True:
        LOG.debug(f"Downloading data at offset {offset} of {dataset_id}")
        results = client.get(
            dataset_id,
            limit=page_limit,
            content_type="csv",
            exclude_system_fields=False,
            offset=offset,
        )

        if not results[1:]:
            LOG.debug(f"All available results read from dataset {dataset_id}.")
            break

        path = "df" + str(offset) + ".csv"
        paths = np.append(path, paths)
        # note path for chuck of data and append to paths

        df = results_to_df(results)
        # write chuck of data to pandas df

        df = df[column_order]
        # rearage columns to be in same order at metadata_columns

        if len(point_columns) == 0:
            df.to_csv(path, header=False, index=False)

        else:
            for column in point_columns:
                df[column] = df[column].str.replace("POINT ", "")
                df[column] = df[column].str.replace(" ", ", ")
                df.to_csv(path, header=False, index=False)
        # check if there are any point columns in dataset, an if there are
        # edit formating to be readable by PostGres

        if len(results) - 1 < page_limit:
            LOG.debug(f"All available results read from dataset {dataset_id}.")
            break

        if size_limit and offset >= size_limit:
            LOG.info(
                f"Reached requested row count limit, {size_limit}, for dataset"
                f"{dataset_id}."
            )
            LOG.debug(
                f"Pulled {offset} rows, in increments of {page_limit}, up to"
                f"specified limit of {size_limit} rows."
            )
            break

        offset += page_limit
        # move counter

    headers = ",".join(column_order)
    # use column_order to create headers for the .csv

    path = write_csv(paths, headers)
    # use write_csv to merge all csvs together

    return path


def write_csv(paths, headers):
    """
    Takes in an array of .csv paths and appends them all together using
    python fucntions. This allows us to pull in a large dataset, relying
    on disk space, while preserving limited system memory.
    """
    csv_out = "consolidated.csv"
    csv_merge = open(csv_out, "w")
    csv_merge.write(headers)
    csv_merge.write("\n")

    for path in paths:
        with open(path, "r") as data:
            csv = data.read()
        csv_merge.write(csv)
    return csv_out


def Merge(dict1, dict2):
    """
    Appends two dicts and returns a dict.
    """
    res = {**dict1, **dict2}
    return res


def create_col_type_dict(raw_metadata, sample_data, sql_type):
    """
    Uses socrata metadata to set SQL datatypes
        (1) creates two dictionaries, one that maps socrata data type to
            database sql_type and another that maps socrata system_feilds
            to database sql_type
        (2) runs through metadata and creates dictonary of current socrata
            column names and datatypes. Then transfomrs dict to map socrata
            datatypes to specified database datatypes.
                (a) For Redshift, points are mapped to varchar
                (b) For PostGres, points are mapped to points
                (c) All varchar data types are set to 1024, but can be changed
                    when varchar_len is passed in.
        (3) If destination is PostGres, notes all columns that are points in
            an index.
        (4) column order is stored in index
        (4) converts dict to array of dicts to be readable by civis API

    Parameters
    ----------
    raw_metadata : Dict

    varchar_len: str
        Length of varchar to be passed in - defualts to 1024

    sql_type: Dict
        Dict of socrata to sql mappings

    returns
    -------
    table_columns
        Array of dicts to be passed to civis.io.civis_file_to_table()

    point_columns
        If PostGres import an index of columns that are point types

    column_order
        array of columns that corresponds to same order as table_columns

    extra_columns
        array of columns that are present in metadata but not in data pull.
        This gets noted in run logs.

    """

    system_fields = OrderedDict(
        {"id": "VARCHAR(2048)", "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP"}
    )

    column_dict, metadata_columns = metadata_pull_cols_datatypes(raw_metadata)
    # parses raw metadata and outputs array of columns and array of datatypes

    sql_map = map_to_sql(sql_type, column_dict)
    # map sql type to socrata data type

    extra_columns = diff_metadata_columns(metadata_columns, list(sample_data.columns))
    # compares metadata columns to columns in sample pull and notes
    # differences in array

    remove_columns(extra_columns, sql_map)
    # removes columns found in metadata (sql_map) but not dataset

    soct_type_map = Merge(sql_map, system_fields)
    # merges metadata columns with system columns

    column_order = list(OrderedDict(soct_type_map).keys())
    # notes column order of soct_type_map

    table_columns = civis_api_formatting(soct_type_map)
    # re-writes dict in correct formatting to pass into civis file to
    # table API call

    point_columns = find_point_columns(soct_type_map)
    # parses datatype_map for point columns types and notes it

    return table_columns, point_columns, column_order, extra_columns


def find_point_columns(datatype_map):
    """
    parses through datatype_map and outputs array containing all columns of
    data type Point
    """
    point_columns = [
        col for col, col_type in datatype_map.items() if col_type == "POINT"
    ]
    return point_columns


def civis_api_formatting(soct_type_map):
    """
    Converts soct_type_map to be readable by Civis API.
    """
    table_columns = [{"name": n, "sql_type": t} for n, t in soct_type_map.items()]
    return table_columns


def map_to_sql(sql_type, column_dict):
    """
    Converts soct_type_map to be readable by Civis API.
    """
    sql_map = OrderedDict({k: sql_type[v] for k, v in column_dict.items()})
    return sql_map


def metadata_pull_cols_datatypes(raw_metadata):
    """
    Parses through raw_metadata and outputs two arrays
        (1) zipped: a dict of column_name and associated socrata data types
        (2) metadata_columns: an array of the metadata_columns names
    """
    metadata_columns = []
    socrata_datatypes = []

    for i in np.arange(len(raw_metadata["columns"])):
        metadata_columns.append(raw_metadata["columns"][i]["name"])
        socrata_datatypes.append(raw_metadata["columns"][i]["dataTypeName"])

    metadata_columns = [i.strip().lower() for i in metadata_columns]

    metadata_columns = [re.sub(r"[^a-zA-Z0-9_]", "", i) for i in metadata_columns]
    # uses regex function to remove all non-alphanumeric characters

    zipped = OrderedDict(zip(metadata_columns, socrata_datatypes))

    return zipped, metadata_columns


def diff_metadata_columns(li1, li2):
    """
    Compares two lists and returns differnce
    """
    diff = list(set(li1) - set(li2))
    return diff


def remove_columns(entries, the_dict):
    """
    Removes specified entries from a dict
    """
    for key in entries:
        if key in the_dict:
            del the_dict[key]


def select_sql_map(database, varchar_len: str = None):
    """
    Selects Socrata to SQL mapping based on database that is passed in and
    optional varchar_len argument.
    """
    if database.lower() == "redshift":
        if varchar_len is None:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(1024)",
                "location": "VARCHAR(1024)",
                "multiline": "VARCHAR(1024)",
                "multipoint": "VARCHAR(1024)",
                "multipolygon": "VARCHAR(1024)",
                "polygon": "VARCHAR(1024)",
                "calendar_date": "TIMESTAMP",
                "point": "VARCHAR(1024)",
            }
        else:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(1024)",
                "location": "VARCHAR(1024)",
                "multiline": "VARCHAR(1024)",
                "multipoint": "VARCHAR(1024)",
                "multipolygon": "VARCHAR(1024)",
                "polygon": "VARCHAR(1024)",
                "calendar_date": "TIMESTAMP",
                "point": "VARCHAR(1024)",
            }

            varchar_len = "VARCHAR(" + varchar_len + ")"

            sql_type["point"] = varchar_len
            sql_type["text"] = varchar_len
            sql_type["location"] = varchar_len

    elif database.lower() == "postgres":
        if varchar_len is None:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(1024)",
                "location": "VARCHAR(1024)",
                "multiline": "VARCHAR(1024)",
                "multipoint": "VARCHAR(1024)",
                "multipolygon": "VARCHAR(1024)",
                "polygon": "VARCHAR(1024)",
                "calendar_date": "TIMESTAMP",
                "point": "POINT",
            }
        else:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(1024)",
                "location": "VARCHAR(1024)",
                "multiline": "VARCHAR(1024)",
                "multipoint": "VARCHAR(1024)",
                "multipolygon": "VARCHAR(1024)",
                "polygon": "VARCHAR(1024)",
                "calendar_date": "TIMESTAMP",
                "point": "POINT",
            }

            varchar_len = "VARCHAR(" + varchar_len + ")"

            sql_type["point"] = varchar_len
            sql_type["text"] = varchar_len
            sql_type["location"] = varchar_len

    return sql_type


def results_to_df(results):

    df = pd.DataFrame(results[1:], columns=results[0])
    """
    Writes socrata get returns to a pandas dataframe. Also cleans header names
    and standardizes 'id'/'sid' system column name to 'id'.
    """
    df.columns = df.columns.str.strip().str.lower()

    df.rename(columns=lambda x: re.sub(r"[^a-zA-Z0-9_]", "", x), inplace=True)
    df.rename(columns=lambda x: re.sub(r"sid", "id", x), inplace=True)

    return df

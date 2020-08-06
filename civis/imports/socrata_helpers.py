"""
Helpers functions for the socrata import.
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
    page_limit: int = 90000,
    size_limit: int = None,
):
    """
    Pulls in Socrata data using API Client
        (1) Creates while loop that runs for records retrieved <= size_limitwith
        (2) Starting at offset=0, pulls in number of rows specified by page_limit
            (a) if the import is to PostGres database, pandas string comands will
                convert socrata defined point datatype to format required by PostGres
            (b) For chunk of data, writes pandas df to .csv and notes csv name in array
        (3) Adjusts offset by page_limit and repeats until either size_limit
            or end of dataset reached
        (4) Appends all .csvs using python functions
        (5) Outputs path to appended .csv
    Parameters
    ----------
    client:
        An instance of socrata.APIClient.
    dataset_id: str
        Socrata dataset identifier
    point_columns
        An index of columns that are point types, this is used to edit the string
        of columns to be compatible with PostGres import
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
    offset = 0
    paths = []
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
        df = pd.DataFrame(results[1:], columns=results[0])

        if len(point_columns) == 0:
            df.to_csv(path, header=False, index=False)

        else:
            for column in point_columns:
                df[column] = df[column].str.replace("POINT ", "")
                df[column] = df[column].str.replace(" ", ", ")
                df.to_csv(path, header=False, index=False)

        df.columns = map(
            str.lower, df.columns
        )  # converting headers to lower and pulling
        headers = df.columns.str.cat(sep=",")

        if len(results) - 1 < page_limit:
            LOG.debug(f"All available results read from dataset {dataset_id}.")
            break

        if size_limit and offset >= size_limit:
            LOG.info(
                f"Reached requested row count limit, {size_limit}, for dataset "
                f"{dataset_id}."
            )
            LOG.debug(
                f"Pulled {offset} rows, in increments of {page_limit}, up to "
                f"specified limit of {size_limit} rows."
            )
            break

        offset += page_limit

    csv_out = write_csv(paths, headers)

    return csv_out


def write_csv(paths, headers):
    """
    Takes in an array of .csv paths and appends them all together
     using python fucntions.
    This allows us to pull in a large dataset, relying  on disk space,
    while preserving limited system memory.
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


def create_col_type_dict(raw_metadata, database, varchar_len: str = None):
    """
    Uses socrata metadata to set SQL datatypes
        (1) creates two dictionaries, one that maps socrata data type
            to database sql_type and another that maps socrata system_feilds
            to database sql_type
        (2) runs through metadata and creates dictonary of current socrata column
            names and datatypes.
            Then transfomrs dict to map socrata datatypes to specified
            database datatypes.
                (a) For Redshift, points are mapped to varchar
                (b) For PostGres, points are mapped to points
                (c) All varchar data types are set to 256, but can be changed when
                    varchar_len is passed in.
        (3) If destination is PostGres, notes all columns that are points in an index.
        (4) converts dict to array of dicts to be readable by civis API
    Parameters
    ----------
    raw_metadata : Dict
    varchar_len: str
        Length of varchar to be passed in - defualts to 256
    returns
    -------
    table_columns
        Array of dicts to be passed to civis.io.civis_file_to_table()
    point_columns
        If PostGres import an index of columns that are point types
    """

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
    df.columns = (
            df.columns.str.strip()
            .str.lower()
        )

    df.rename(columns=lambda x: re.sub(r'[^a-zA-Z0-9_]','',x), inplace=True)
    df.rename(columns=lambda x: re.sub(r'sid','id',x), inplace=True)

    return df

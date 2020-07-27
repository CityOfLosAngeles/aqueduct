"""
Helpers functions for the socrata import
"""
import pandas as pd
import os
import civis
from typing import Optional, Tuple
from datetime import datetime
from civis import APIClient
import logging
import json
from functools import reduce
import numpy as np
from collections import OrderedDict

from civis.io import dataframe_to_file, file_to_civis, civis_file_to_table, query_civis

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
        (1) Creates while loop that runs for records retrieved <= size_limitwith
        (2) Starting at offset=0, pulls in number of rows specified by page_limit
            (a) if the import is to PostGres database, pandas string comands will
                convert socrata defined point datatype to format required by PostGres
            (b) For chunk of data, uses pandas functions to clean data,
                writes pandas df to .csv and notes csv name in array
        (3) Adjusts offset by page_limit and repeats until either size_limit or end of dataset reached
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
        # write chuck of data to pandas df

        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(".", "")
            .str.replace("-", "")
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace("'", "")
            .str.replace("#", "no")
            .str.replace(":id", "sid")
            .str.replace(":", "")
        )
        # clean up column names to have non-JSON-identifier characters
        df = df[column_order]
        # put columns in same order as metadata

        headers = df.columns.str.cat(sep=",")
        # make headers into index to pass into .csv wirte

        if len(point_columns) == 0:
            df.to_csv(path, header=False, index=False)

        else:
            for column in point_columns:
                df[column] = df[column].str.replace("POINT ", "")
                df[column] = df[column].str.replace(" ", ", ")
                df.to_csv(path, header=False, index=False)
                # if column is point column reformat to be readable by postgres

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
    Takes in an array of .csv paths and appends them all together using python fucntions.
    This allows us to pull in a large dataset, relying  on disk space, while preserving limited system memory.
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


def create_col_type_dict(
    client, dataset_id, raw_metadata, database, varchar_len: str = None
):
    """
    Uses socrata metadata to set SQL datatypes
        (1) creates two dictionaries, one that maps socrata data type to database
            sql_type and another that maps socrata system_feilds to database sql_type
        (2) runs through metadata and creates dictonary of current socrata column names and datatypes.
            Then transfomrs dict to map socrata datatypes to specified database datatypes.
                (a) For Redshift, points are mapped to varchar
                (b) For PostGres, points are mapped to points
                (c) All varchar data types are set to 256, but can be changed when
                    varchar_len is passed in.
        (3) If destination is PostGres, notes all columns that are points in an index.
        (4) column order is stored in index
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

    column_order
        array of columns that corresponds to same order as table_columns

    """

    if database.lower() == "redshift":
        if varchar_len is None:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(256)",
                "location": "VARCHAR(256)",
                "multiline": "VARCHAR(256)",
                "multipoint": "VARCHAR(256)",
                "multipolygon": "VARCHAR(256)",
                "polygon": "VARCHAR(256)",
                "calendar_date": "TIMESTAMP",
                "point": "VARCHAR(256)",
            }
        else:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(256)",
                "location": "VARCHAR(256)",
                "multiline": "VARCHAR(256)",
                "multipoint": "VARCHAR(256)",
                "multipolygon": "VARCHAR(256)",
                "polygon": "VARCHAR(256)",
                "calendar_date": "TIMESTAMP",
                "point": "VARCHAR(256)",
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
                "text": "VARCHAR(256)",
                "location": "VARCHAR(256)",
                "multiline": "VARCHAR(256)",
                "multipoint": "VARCHAR(256)",
                "multipolygon": "VARCHAR(256)",
                "polygon": "VARCHAR(256)",
                "calendar_date": "TIMESTAMP",
                "point": "POINT",
            }
        else:
            sql_type = {
                "number": "DOUBLE PRECISION",
                "double": "DOUBLE PRECISION",
                "money": "DOUBLE PRECISION",
                "checkbox": "boolean",
                "text": "VARCHAR(256)",
                "location": "VARCHAR(256)",
                "multiline": "VARCHAR(256)",
                "multipoint": "VARCHAR(256)",
                "multipolygon": "VARCHAR(256)",
                "polygon": "VARCHAR(256)",
                "calendar_date": "TIMESTAMP",
                "point": "POINT",
            }

            varchar_len = "VARCHAR(" + varchar_len + ")"

            sql_type["point"] = varchar_len
            sql_type["text"] = varchar_len
            sql_type["location"] = varchar_len

    system_fields = OrderedDict(
        {"sid": "VARCHAR(2048)", "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP"}
    )

    cols = []
    datatypes = []

    for i in np.arange(len(raw_metadata["columns"])):
        cols.append(raw_metadata["columns"][i]["name"])
        datatypes.append(raw_metadata["columns"][i]["dataTypeName"])

    cols = [
        i.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
        .replace("'", "")
        .replace("#", "no")
        .replace(":id", "sid")
        .replace(":", "")
        for i in cols
    ]

    sample_data = client.get(
        dataset_id, limit=5, content_type="csv", exclude_system_fields=False, offset=0
    )

    df = pd.DataFrame(sample_data[1:], columns=sample_data[0])

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "")
        .str.replace("-", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("'", "")
        .str.replace("#", "no")
        .str.replace(":id", "sid")
        .str.replace(":", "")
    )

    diff = diff_metadata_columns(cols, list(df.columns))

    socdict = OrderedDict(zip(cols, datatypes))

    soct_type_map = OrderedDict({k: sql_type[v] for k, v in socdict.items()})

    entries_to_remove(diff, soct_type_map)

    soct_type_map = Merge(soct_type_map, system_fields)

    table_columns = [{"name": n, "sql_type": t} for n, t in soct_type_map.items()]
    point_columns = [
        col for col, col_type in soct_type_map.items() if col_type == "POINT"
    ]
    column_order = OrderedDict(soct_type_map).keys()

    return table_columns, point_columns, column_order


def diff_metadata_columns(li1, li2):
    diff = list(set(li1) - set(li2))
    return diff


def entries_to_remove(entries, the_dict):
    for key in entries:
        if key in the_dict:
            del the_dict[key]

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


from civis.io import dataframe_to_file, file_to_civis

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


def _store_and_attach_metadata(
    client: APIClient, metadata: dict, metadata_paths: dict, filename: str,
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
    ---------
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


def _store_and_attach_dataset(
    client: civis.APIClient, df: pd.DataFrame, filename: str
) -> int:
    """
    Given an APIClient object, a DataFrame, and a filename, write the DataFrame
    to a file, attach the file as a script output, and return the file_id.
    Parameters
    ---------
    client: APIClient
        An instance of civis.APIClient.
    df: pd.DataFrame
        A pandas DataFrame containing the data to be stored.
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
    df.columns = map(str.lower, df.columns)
    file_id = dataframe_to_file(df=df, name=filename, expires_at=None, index=False,)
    client.scripts.post_containers_runs_outputs(
        id=os.environ["CIVIS_JOB_ID"],
        run_id=os.environ["CIVIS_RUN_ID"],
        object_type="File",
        object_id=file_id,
    )
    return file_id


def _store_and_attach_metadata(
    client: civis.APIClient, metadata: dict, metadata_paths: dict, filename: str,
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
    ---------
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
    page_limit: int = 50000,
    size_limit: int = None,
    where: str = None,
):
    df = pd.DataFrame()
    offset = 0
    while True:
        LOG.debug(f"Downloading data at offset {offset} of {dataset_id}")
        results = client.get(
            dataset_id,
            limit=page_limit,
            content_type="csv",
            exclude_system_fields=False,
            offset=offset,
            where=where,
        )

        if not results[1:]:
            LOG.debug(f"All available results read from dataset {dataset_id}.")
            break

        df = pd.concat([df, pd.DataFrame(results[1:], columns=results[0])])

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

    return df

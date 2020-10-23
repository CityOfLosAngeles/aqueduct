# Civis-Aqueduct Utils

A small Python module containing utilities for working with Civis and Aqueduct.

## Installation

To install, enter
```bash
pip install .
```
in your terminal from this directory.

## JupyterLab extension

This adds a small button to the toolbar of the classic Jupyter Notebook to launch JupyterLab,
allowing users to toggle back and forth between environments when using a Jupyter notebook
server in the Civis platform.

## Civis-Service CLI

This small CLI tool allows Civis platform users to generate shareable URLs to a deployed service.
This URL can then be shared with external stakeholders to view reports, dashboards, etc.

To list available services, enter
```bash
civis-service list
```

To share a service, enter
```bash
civis-service share SERVICE_ID SHARE_NAME
```
where `SERVICE_ID` is the integer ID of the service, and `SHARE_NAME` is a unique name
for the share link that is generated. You can only use a `SHARE_NAME` once, new
URLs must have a different name.

To unshare a URL that you have created so that it no longer works, enter
```bash
civis-service unshare SERVICE_ID SHARE_NAME
```

## GitHub utils

The `upload_file_to_github` function in `github.py` allows Civis to be used to schedule GitHub commits and overwrite the same file on GitHub. The `token` is the GitHub personal access token; this corresponds to the `GITHUB_TOKEN` credential on Civis.

```
MY_REPO = "CityofLosAngeles/covid19-indicators"
MY_BRANCH = "master"

# The path within the GitHub repo
GITHUB_PATH = "data/my_file.csv"

To upload a local file (the local path can differ from the repo's path)
LOCAL_FILE = "../folder_name/my_file.csv"

To upload an S3 file
LOCAL_FILE = "s3://bucket_name/folder_name/my_file.csv"

COMMIT_MSG = "Update data"

# To use the function with the above-defined constants:
upload_file_to_github(token, MY_REPO, MY_BRANCH, GITHUB_PATH, LOCAL_FILE, COMMIT_MSG)
```

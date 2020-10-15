#!/bin/bash

# Start script for the ita-data-civis-lab Docker image.
# This script does the following:
#    1) Attempts to auto-configure AWS credentials if they are attached in Civis
#    2) Installs any local Python packages in the app directory (attached GH repo)
#    3) Infers based on the presence of a CIVIS_SERVICE_ID whether to launch JupyterLab
#       or a Voila session.

# Run the aws-configure script to try to detect AWS credentials from the environment
aws-configure || true

# If the app includes a setup.py, install the local package.
if [ -f "${APP_DIR:-/app}/setup.py" ]; then
    echo "Installing local repository python packages."
    pip install -e /app || true
fi

# Test if CIVIS_SERVICE_ID is defined. If so, we are likely in a deployed
# service, so start voila. If not, we are likely in a Civis Jupyter notebook
# context, so start that.
if [[ -z $CIVIS_SERVICE_ID ]]; then
    civis-jupyter-notebooks-start
else
    voila \
        --port=3838 \
        --no-browser \
        --Voila.ip='*' \
        --Voila.tornado_settings=$'{"headers":{"Content-Security-Policy":"frame-ancestors platform.civisanalytics.com localhost:*; report-uri /api/security/csp-report" }}' \
        ${APP_DIR:-/app}/${REPO_PATH_DIR}
fi

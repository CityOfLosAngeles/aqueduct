#!/bin/bash

# Run the aws-configure script to try to detect AWS credentials from the environment
aws-configure

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

#!/bin/bash

if [[ -z $CIVIS_SERVICE_ID ]]; then
    civis-jupyter-notebooks-start
else
    voila \
        --port=3838 \
        --no-browser \
        --Voila.ip='*' \
        --Voila.tornado_settings=$'{"headers":{"Content-Security-Policy":"frame-ancestors platform.civisanalytics.com localhost:*; report-uri /api/security/csp-report" }}' \
        /app
fi

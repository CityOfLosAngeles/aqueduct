#!/bin/bash 

set -e 

[ -e /tmp/service-requests.csv ] && rm /tmp/service-requests.csv

curl https://data.lacity.org/api/views/69nn-wkid/rows.csv?accessType=DOWNLOAD \
     -u '$SOCRATA_USERNAME:$SOCRATA_PASSWORD' \
     -o /tmp/service-requests.csv

mkdir -p /tmp/service-requests

ogr2ogr \ 
    -s_srs EPSG:4326 \
    -t_srs EPSG:4326  \
    -f "ESRI Shapefile" \
    /tmp/service-requests/service-requests.shp /tmp/service-requests.csv

zip -r /tmp/service_requests.zip /tmp/service_requests/*
#!/usr/bin/env python
# coding: utf-8

from arcgis import GIS
import os
import intake_civis
import ibis
from arcgis.features import FeatureLayerCollection

# Prepping credentials
lahub_user = os.environ["LAHUB_ACC_USERNAME"]
lahub_pass = os.environ["LAHUB_ACC_PASSWORD"]

pwd = os.getcwd()
OUTPUT_FILE = pwd + "/MyLA311 Service Requests Last 6 Months.csv"
myla311_layer = "4db3e9c3d13543b6a686098e0603ddcf"


# For 311
def prep_311_data(file):
    catalog = intake_civis.open_redshift_catalog()
    expr = catalog.public.import311.to_ibis()
    recent_srs = expr[
        (expr.createddate > (ibis.now() - ibis.interval(months=6)))
        & (expr.requesttype != "Homeless Encampment")
    ]
    df = recent_srs.execute()
    df.to_csv(file, index=False)


def update_geohub_layer(user, pw, layer, update_data):
    geohub = GIS("https://lahub.maps.arcgis.com", user, pw)
    flayer = geohub.content.get(layer)
    flayer_collection = FeatureLayerCollection.fromitem(flayer)
    flayer_collection.manager.overwrite(update_data)


if __name__ == "__main__":
    prep_311_data(OUTPUT_FILE)
    update_geohub_layer(lahub_user, lahub_pass, myla311_layer, OUTPUT_FILE)

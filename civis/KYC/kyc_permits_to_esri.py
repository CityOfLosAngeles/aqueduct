#!/usr/bin/env python
# coding: utf-8

import os
pwd=os.getcwd()
import sys
#!{sys.executable} -m pip install sodapy
from sodapy import Socrata
import pandas as pd
import numpy as np
from arcgis.gis import GIS
from shapely.geometry import Point
import geopandas as gpd
from arcgis.features import FeatureLayerCollection

lahub_user = os.environ["LAHUB_ACC_USERNAME"]
lahub_pass = os.environ["LAHUB_ACC_PASSWORD"]
permit_layer = '48fca217dd5a410bbfd6ce0abcdd3a26'
pwd = os.getcwd()
OUTPUT_FILE = pwd + "/Building and Safety Permits, Last 6 Months.csv"

def prep_permit_data(file):
    client = Socrata("data.lacity.org", None)
    df = pd.DataFrame(client.get('n9nq-vewq', limit=10000000))
    df2=df[((df.permit_sub_type == 'Apartment')|(df.permit_sub_type == 'Commercial'))&(
        (df.permit_type == 'Bldg-Addition')|(df.permit_type == 'Bldg-New')|(df.permit_type == 'Bldg-Demolition'))]
    df2.to_csv(file, index=False)

def update_geohub_layer(user, pw, layer, update_data):
    geohub = GIS('https://lahub.maps.arcgis.com', user, pw)
    flayer = geohub.content.get(layer)
    flayer_collection = FeatureLayerCollection.fromitem(flayer)
    flayer_collection.manager.overwrite(update_data)


if __name__ == "__main__":
    prep_311_data(OUTPUT_FILE)
    update_geohub_layer(lahub_user, lahub_pass, permit_layer, OUTPUT_FILE)

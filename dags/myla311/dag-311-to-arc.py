import pandas as pd
import arcgis 
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import geopandas as gpd 
import os 
import shutil
import pathlib

print('reading init CSV')
df = pd.read_csv('./service_requests.csv')

print('logging into ESRI')

gis = GIS("http://lahub.maps.arcgis.com/home/index.html", 
          os.environ.get('LAHUB_USERNAME'),
          os.environ.get('LAHUB_PASSWORD'))

df[["Latitude", "Longitude"]] = df[["Latitude", "Longitude"]].apply(pd.to_numeric)
gdf = gpd.GeoDataFrame(
      df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))

print(f'Loaded Geodataframe, currently at {len(gdf)} rows')
gdf.crs = {'init': 'epsg:4326'}

gdf_small = gdf.head(10)

def prep_gdf(gdf, file_path='/tmp/requests-311', file_name='/test.shp'):
    """
    Given a geodataframe, 
    return a path with a zipped shapefile 
    """
    # make a folder 
    pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)
    gdf_small.to_file(file_path + file_name, driver="ESRI Shapefile")

    print('zipping file')
    # zip the file
    data_file_location = file_path 
    path = shutil.make_archive(data_file_location, 'zip', file_path)
    return path

data_file_location = prep_gdf(gdf_small)
print('uploading')
item_prop = {'title':'311_test',
             'type': 'Shapefile'}
item = gis.content.add(item_properties = item_prop, data=data_file_location)
feature_layer_item = item.publish()

print("published initial dataset")
# load the new file 
gdf_slightly_larger = gdf.head(100)
large_path = prep_gdf(gdf_slightly_larger, file_path='/tmp/requests-larger')

print("overwriting")
# overwrite the file 
feature_layer_collection = FeatureLayerCollection.fromitem(feature_layer_item)

print(feature_layer_collection.manager.overwrite(large_path))


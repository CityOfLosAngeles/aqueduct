import pandas as pd
import arcgis 

df = pd.read_csv('./service-requests.csv')

srid = 4326
df['latitude'] = pd.to_numeric(df['latitude'])
df['longitude'] = pd.to_numeric(df['longitude'])


import arcgis 
import os 
from sqlalchemy import create_engine

def ratstat_loader():
    """
    load the ratstat data 
    into a postgres DB
    """ 
    hub = arcgis.gis.GIS(url='https://lahub.maps.arcgis.com',
                         username=os.environ.get('LAHUB_USERNAME'),
                         password=os.environ.get('LAHUB_PASSWORD'))
    rat_stat_group = arcgis.gis.Group(hub, 
                                      '7f3d66478dd846598e76a8e334a03988') #this is the groupid of the ratstat group       
    content = rat_stat_group.content()
    feature_layers = []
    for item in content:
        try:
            [feature_layers.append(feature_layer) for feature_layer in item.layers]
        except KeyError: 
            pass
    dfs = {layer.properties['name']: layer.query(as_df=True, as_shapely=True) for layer in feature_layers}
    conn = create_engine(os.environ.get('POSTGRES_URI'))
    for k,table in dfs.items(): 
        table['SHAPE'] = str(table['SHAPE'])
        table.to_sql(k,
                     conn, 
                     schema='public-health',
                     if_exists='replace')
    return True
                 
if __name__ == '__main__': 
    dfs = ratstat_loader()
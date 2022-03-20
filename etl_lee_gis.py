'''
convert (manipulate) SHP file data to Python data with fiona
after converting to python data I can decide how I will 
transfer it to the MYSQL db. Two options atm. Two options atm
    1. PyMySql and SQLAlchemy
    2. Pandas to MySQL through pandas method

shapefile that Im working with is in local dir so no path needed to specify an abs.path
'''
from collections import defaultdict
from pprint import pprint
from sqlalchemy import create_engine
import fiona
import numpy as np
import pandas as pd
import pprint
import pymysql
import re
import secrets

## TWO SHAPEFILES: {Polk and Lee}
def lee_shapefile_path():
    shapefilepath = '/home/odin/michaela/gis/data_raw/Lee/lee-parcels/Parcels.shp'
    return shapefilepath

def gis_data(shapefile_path,sample_size=None):
    # creates and reteurns python dictioanry objects for gis data
    print(f"{shapefile_path = }")
    with fiona.open(shapefile_path, 'r') as shapefile:
        gis_data_dict = defaultdict(list)
        for row in shapefile:
            row_data =  row['properties']
            for column, column_value in row['properties'].items():
                gis_data_dict[column].append(column_value)
    return gis_data_dict

def get_lee_gis():
    ''' returns Lee gis dataframe object '''
    gis_dict = gis_data(lee_shapefile_path())
    gis_dataframe = pd.DataFrame(gis_dict)
    print(gis_dataframe.head())
    return gis_dataframe


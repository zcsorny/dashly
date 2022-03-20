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

## SHAPEFILE PATHS
def fetch_shapefiles():
    ''' returns a dict w/ tablename as key & shapefile path as value for all 3 '''
    polk_shapefiles = {
            'account' : '/home/odin/michaela/gis/data/data_raw/Polk/account',
            'lot' : '/home/odin/michaela/gis/data/data_raw/Polk/lot',
            'parcel' : '/home/odin/michaela/gis/data/data_raw/Polk/parcel'
            }
    return polk_shapefiles

# creates a python dict from .shp file  
def extract_data(fpath):
    with fiona.open(fpath,'r') as shapef:
        gis_data = defaultdict(list)
        for row in shapef:
            row_value =  row['properties']
            for column, column_value in row['properties'].items():
                gis_data[column].append(column_value)
    return gis_data

# ftp get
def gis_dataframe():
    ''' returns dataframe object for Polk GIS data '''
    # holds dataframe objects created by for loop iteration
    dataframe_container = {}
    # creates  dataframe from GIS dict 
    for table,fpath in fetch_shapefiles().items():
        print(f'\n{table =  }')
        gisDataframe = pd.DataFrame(extract_data(fpath))
        dataframe_container.update({table:gisDataframe})
        # printing for spot check
        print(gisDataframe.head())
        print(gisDataframe.info(verbose=True))
    return dataframe_container


def db_credentials():
    ''' load in protected mysql db credentials required for sqlalchemy connector string '''
    # string connection components
    database_name = secrets.dbname
    hostname = secrets.dbhost
    username = secrets.dbuser
    pwd = secrets.dbpw
    return database_name,hostname,username,pwd
  
#  ## connection funct
def get_db_connection():
  ''' returns pymysql connection object used by SQL alchemy to connect to the database '''
  # getting credentials
  database, host, user, password = db_credentials()
  mysql_uri = f'mysql+pymysql://{user}:{password}@{host}/{database}'
  # connect to the database
  connection = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                database=database,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
  sqlalch_url = f'mysql+pymysql://{user}:{password}@{host}/{database}'
  sqlEngine = create_engine(sqlalch_url,pool_recycle=3600)
  database_connection = sqlEngine.connect()
  return database_connection
  
def db_upload():
    ''' cycles through dataframe dict and uploads each Shapefile as a table to MySQL '''
    # establishing connection with db
    db_connection = get_db_connection()
    # gives us tablename and daatframe object
    for tablename, df in gis_dataframe().items():
        try:
            upload = df.to_sql(tablename,db_connection, if_exists='fail')
        except ValueError as valueError:
            print(valueError)
        except Exception as exception:
            print(exception)
        else:
            print(f"\n\n{tablename} has been generated successfully")
        finally:
            db_connection

def main():
    db_upload()
    return

if __name__ == "__main__":
    main()

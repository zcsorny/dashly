'''
    fetches gis zip files from link 
    stores in raw data folder for sql upload


    *** SCRIPT NOT WORKING FOR POLK/LEE ***
'''
from io import BytesIO
from zipfile import ZipFile
import os
import re
import requests


# def charlotte_url():
#     # has 2 potential links: appraiser and accounts ;; only doing appraiser
#     charlotte_appraiser_url = 'https://www.ccappraiser.com/downloads/charlotte.zip'
#     charlotte_accounts_parcels_url = 'https://data.charlottecountyfl.gov/ccgis/data/zips/accounts.zip'
#     return charlotte_appraiser_url 

#  def lee_url():
#      lee_parcels = 'https://leegisopendata2-leegis.opendata.arcgis.com/datasets/parcels-shapefil://www.arcgis.com/sharing/rest/content/items/42aaecadf5a3406a9295fd8e53cec902/data'
#      return lee_parcels 
#  
def polk_url():
    polk_parcels_url = 'https://www.polkpa.org/FTPPage/downloader.ashx?filename=parcel.zip&dir=%5CGISData%5C'
    return polk_parcels_url 
  
def download_gis_zip(url):
    url = url
    print(f'\n\nExtracting GIS data from:  {url}')
    # fetching url
    r = requests.get(url,stream=True)
    if re.search('\.zip$',url) is not None:
        z = ZipFile(BytesIO(r.content))
        z.extractall(r'/home/odin/michaela/gis/data_raw/all')
    else:
        if re.search('lee',url) is not None:
            county = 'lee'
        else:
            county = 'polk'
        with open(f'/home/odin/michaela/gis/data_raw/all/{county}', 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

#  def main():
#      #charlotte_gis = download_gis_zip(charlotte_url())
#      lee_gis = download_gis_zip(lee_url())
#      polk_gis = download_gis_zip(polk_url())
#      return None
#  
if __name__ == "__main__":
    main()

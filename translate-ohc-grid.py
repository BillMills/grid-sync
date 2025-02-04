# usage: python translate-ohc-grid.py
# expects the source .mat file in /tmp/ohc

import pandas as pd
import numpy as np
import scipy.io, datetime
import xarray as xr
from pymongo import MongoClient
import util.helpers as h
import sys, re

def extract_date_from_filename(filename):
    # Define a regular expression to capture MM and YYYY from the filename
    pattern = r'intTempFullFieldSpaceTimeTrend_\d+_\d+_(\d{2})_(\d{4})\.mat'
    
    match = re.search(pattern, filename)
    if match:
        month, year = match.groups()
        # Return the formatted date string
        return f"{year}-{month}-15T00:00:00Z"
    else:
        raise ValueError("Filename does not match the expected pattern.")

client = MongoClient('mongodb://database/argo')
db = client.argo
basins = xr.open_dataset('parameters/basinmask_01.nc')

# extract data from .mat to xarray, compliments Jacopo
mat=scipy.io.loadmat(sys.argv[1])
lon = np.arange(start=20.5, stop=380.5, step=1)
lat = np.arange(start=-89.5, stop=90.5, step=1)
timestamp = extract_date_from_filename(sys.argv[1])
cp0 = 3989.244
rho0 = 1030
d_GCOS_temp_zint = mat['fullFieldGrid']

bfr = xr.DataArray(
        data=d_GCOS_temp_zint,
        dims=["LONGITUDE","LATITUDE"],
        coords=dict(
            LONGITUDE=(["LONGITUDE"], lon),
            LATITUDE=(["LATITUDE"], lat),
        ),
        attrs=dict(
            description="Ocean heat content.",
            units="J/m2",
        ),
    )

# construct a metadata record
# timesteps = list(bfr['TIME'].data) 
# dates = [datetime.datetime.utcfromtimestamp((t - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')) for t in timesteps]
latpoints = [float(x) for x in list(bfr['LATITUDE'].data)]
lonpoints = [float(x) for x in list(bfr['LONGITUDE'].data)]
tidylon = [h.tidylon(x) for x in lonpoints]

meta = {}
meta['_id'] = "kg21_ohc15to300"
meta['data_type'] = 'ocean_heat_content'
meta['date_updated_argovis'] = datetime.datetime.now()
meta['source'] = [{
	'source': ['Kuusela_Giglio2022'],
	'doi': '10.5281/zenodo.6131625',
	'url': 'https://doi.org/10.5281/zenodo.6131625'
}]
meta['levels'] = [15] # really anywhere from 15-300
meta['level_units'] = 'integral from 15 dbar to 300 dbar'

meta['data_info'] = [
	['kg21_ohc15to300'],
	['units'],
	[['J/m^2']]
]

meta['lattice'] = {
		"center" : [
			0.5,
			0.5
		],
		"spacing" : [
			1,
			1
		],
		"minLat" : -89.5,  # should recompute in future updates
		"minLon" : -179.5,
		"maxLat" : 89.5,
		"maxLon" : 179.5
	}

meta['constants'] = {
    'cp0': [cp0, 'J/kg/K'],
    'rho0': [rho0, 'kg/m^3']
}

# # write metadata to grid metadata collection
# try:
# 	db['kg21Meta'].insert_one(meta)
# except BaseException as err:
# 	print('error: db write failure')
# 	print(err)
# 	print(meta)

# construct data records
for lat in latpoints[50:51]:
    for lon in lonpoints[100:101]:
        data = {
            "metadata": ["kg21_ohc15to300"],
            "geolocation": {"type":"Point", "coordinates":[h.tidylon(lon),lat]},
            "basin": h.find_basin(basins, h.tidylon(lon), lat),
            "timestamp": datetime.datetime.fromisoformat(timestamp.replace('Z', '')),
            "data": [bfr.loc[dict(LONGITUDE=lon, LATITUDE=lat)].data]
        }
        data['_id'] = data['timestamp'].strftime('%Y%m%d%H%M%S') + '_' + str(h.tidylon(lon)) + '_' + str(lat) 

        # nothing to record, drop it
        if np.isnan(data['data']).all():
            continue 

        # mongo doesn't like numpy types, only want 6 decimal places, and grid data is packed as [[grid 1's levels], [grid 2's levels, ...]]:
        data['data'] = [[round(float(x*rho0*cp0),6) for x in data['data']]]

        print(data)

        # # check and see if this lat/long/timestamp lattice point already exists
        # record = db['kg21'].find_one(data['_id'])
        # if record:
        #     # append and replace
        #     record['metadata'] = record['metadata'] + data['metadata']
        #     record['data'] = record['data'] + data['data']

        #     try:
        #         db['kg21'].replace_one({'_id': data['_id']}, record)
        #     except BaseException as err:
        #         print('error: db write replace failure')
        #         print(err)
        #         print(data)
        # else:
        #     # insert new record
        #     try:
        #         db['kg21'].insert_one(data)
        #     except BaseException as err:
        #         print('error: db write insert failure')
        #         print(err)
        #         print(data)
import os
from requests import Session
import pandas as pd
from requests.sessions import HTTPAdapter
import numpy as np
from io import StringIO
from time import time
from urllib3.util.retry import Retry
from datetime import datetime, date
from joblib import Parallel, delayed

retries = Retry(total=5, backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])

def get_station_ids(state : str, start_year: int, s : Session):
    response = s.get(f'https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson')
    if response.ok:
        j = response.json()
        sites = [site["properties"]  for site in j["features"]]
        valid_sites = []
        for site in sites:
            try:
                first_year = int(site["time_domain"][1:5])
                if first_year <= start_year and site["time_domain"][6:9].lower() == "now":
                    valid_sites.append(site["sid"])
            except:
                continue
        print(f'Found {len(sites)} valid sites for {state}')
        return valid_sites
    else:
        print(f'Request for {state} failed: {response.status_code} {response.reason}')

def get_station_data(id: str, start: datetime, end: datetime, s : Session):
    asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
    data_query = 'data=tmpf&data=feel'
    date_query = f'year1={start.year}&month1={start.month}&day1={start.day}&year2={end.year}&month2={end.month}&day2={end.day}'
    query = f'?{data_query}&tz=Etc/UTC&format=comma&latlon=yes&{date_query}'
    response = s.get(f'{asos_url}{query}&station={id}')
    if response.ok:
        csv = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        print(f'Retrieved {df.size} observations for {id}')
        if df.size > 0:
            return df[['station', 'valid', 'tmpf', 'lat', 'lon', 'feel']]
        else:
            return None
    else:
        print(f'Request for {id} in failed: {response.status_code} {response.reason}')

def write_station_df(sid : str, s : Session):
    file_name = f'data/weather/{state}_{sid}_weather_data.parquet'
    if os.path.isfile(file_name):
        print(f'{file_name} already exists')
        return ''

    station = get_station_data(sid, datetime(2014,12,19), date.today(), s)
    if station is None:
        print(f'Retrieve {sid} failed')
        return ''

    start = time()
    station['feel'] = np.where(station['feel'] == 'M', station['tmpf'], station['feel'])
    station['valid'] = pd.to_datetime(station['valid'], utc = True)
    numeric_cols = ['tmpf', 'lat', 'lon', 'feel']
    station[numeric_cols] = station[numeric_cols].apply(pd.to_numeric, errors='coerce', axis=1)
    # interpolate only within station temporally (sorted by timestamp 'valid')
    #station['tmpf'] = station['tmpf'].interpolate()
    print(f'Completed {state} {sid} in {time() - start} seconds')
    station.to_parquet(file_name)
    return file_name


states = ['ND', 'SD', 'MN', 'IA', 'AR', 'IN', 'IL', 'NE', 'KS', 'MO', 'WI', 'KY', 'LA', 'AR', 'MS']
# additional states to consider: East TX,OH

with Session() as s:
    s.mount('http://', HTTPAdapter(max_retries=retries))
    for state in states:
        state_df = pd.DataFrame()
        stations = get_station_ids(state, 2014, s)
        Parallel(n_jobs=4, prefer="threads")(delayed(write_station_df)(sid, s) for sid in stations) 
            
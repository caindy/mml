from os import error
from pandas.core.tools import numeric
from requests import Session
import pandas as pd
from requests.sessions import HTTPAdapter
import numpy as np
from io import StringIO
from time import time
from urllib3.util.retry import Retry

retries = Retry(total=5, backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])

def get_station_ids(state : str, s : Session):
    response = s.get(f'https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson')
    if response.ok:
        j = response.json()
        return [site["properties"]["sid"] for site in j["features"]]
    else:
        print(f'Request for {state} failed: {response.status_code} {response.reason}')

def get_station_data(id: str, year : int, s : Session):
    asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
    query = f'?data=all&tz=Etc/UTC&format=comma&latlon=yes&year1={year}&month1=1&day1=1&year2={year}&month2=12&day2=31'
    response = s.get(f'{asos_url}{query}&station={id}')
    if response.ok:
        csv = response.content.decode('utf-8')
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        print(f'Retrieved {df.size} observations for {id} in {year}')
        if df.size > 0:
            return df[['station', 'valid', 'tmpf', 'lat', 'lon', 'feel']]
        else:
            return None
    else:
        print(f'Request for {id} in {year} failed: {response.status_code} {response.reason}')

states = ['LA', 'ND', 'SD', 'MN', 'IA', 'AR', 'IN']
years = range(2008, 2021)

with Session() as s:
    s.mount('http://', HTTPAdapter(max_retries=retries))
    for state in states:
        for year in years:
            state_year = pd.DataFrame()
            for sid in get_station_ids(state, s):
                station = get_station_data(sid, year, s)
                if station is None:
                    continue
                start = time()
                station['feel'] = np.where(station['feel'] == 'M', station['tmpf'], station['feel'])
                station['valid'] = pd.to_datetime(station['valid'], utc = True)
                numeric_cols = ['tmpf', 'lat', 'lon', 'feel']
                station[numeric_cols] = station[numeric_cols].apply(pd.to_numeric, errors='coerce', axis=1)
                # interpolate only within stationa temporally (sorted by )
                station['tmpf'] = station['tmpf'].interpolate()
                state_year = state_year.append(station)
                print(f'Completed {state} {year} {sid} in {time() - start} seconds')
            state_year.to_parquet(f'{state}{year}_weather_data.parquet')

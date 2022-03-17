from requests import Session
import pandas as pd
import numpy as np
from io import StringIO

from datetime import datetime
from pytz import utc
from request_session import get_session


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

def get_station_csv(id: str, start: datetime, end: datetime, s : Session):
    asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
    data_query = 'data=tmpf&data=feel'
    start = start.astimezone(tz=utc) 
    end = end.astimezone(tz=utc)
    date_query = f'year1={start.year}&month1={start.month}&day1={start.day}&year2={end.year}&month2={end.month}&day2={end.day}'
    query = f'?{data_query}&tz=Etc/UTC&format=comma&latlon=yes&{date_query}'
    url = f'{asos_url}{query}&station={id}'
    response = s.get(url)
    if response.ok:
        return response.content.decode('utf-8')
    else:
        return None

def get_station_data(id: str, start: datetime, end: datetime, s : Session):
    if csv := get_station_csv(id, start, end, s):
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        print(f'Retrieved {df.size} observations for {id}')
        if df.size > 0:
            return df[['station', 'valid', 'tmpf', 'lat', 'lon', 'feel']]
        else:
            print(csv)
    return None

def cast_df(df, observation_hours):
    df = df[df['tmpf'] != 'M']
    df['valid'] = pd.to_datetime(df['valid'], utc = True)
    numeric_cols = ['tmpf', 'lat', 'lon']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, axis=1)
    df = df.drop(columns=['feel'])
    idx = df.drop_duplicates('valid').set_index('valid').index.get_indexer(observation_hours, method='nearest')
    df = df.iloc[idx]
    df['valid'] = df['valid'].dt.round(freq='H')
    return df.drop_duplicates('valid') 

def get_station_df(sid : str, start_date, end_date):
    station = None
    with get_session() as s:
        station = get_station_data(sid, start_date, end_date, s)
        if station is None:
            return None
    return station

# states = ['ND', 'SD', 'MN', 'IA', 'AR', 'IN', 'IL', 'NE', 'KS', 'MO', 'WI', 'KY', 'LA', 'AR', 'MS']
# additional states to consider: East TX,OH

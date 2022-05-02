from datetime import datetime, timedelta
from functools import cached_property
from io import StringIO
import pandas as pd
from pytz import utc
from util import WebClient
from MISO import miso_states

class ASOS(WebClient):
    """Pandas Adapter for the Iowa State ASOS Network downloads JSON API"""
    def __init__(self, first_year = 2016) -> None:
        self.first_year = first_year or 2016

    @cached_property
    def stations(self):
        return pd.concat([self.__get_stations(state, self.first_year)
                            for state in miso_states])

    def __get_station_csv(self, id: str, start: datetime, end: datetime):
        asos_url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py'
        data_query = 'data=tmpf&data=feel'
        start = start.astimezone(tz=utc) 
        end = end.astimezone(tz=utc) + timedelta(days=1)
        date_query = f'year1={start.year}&month1={start.month}&day1={start.day}&year2={end.year}&month2={end.month}&day2={end.day}'
        query = f'?{data_query}&tz=Etc/UTC&format=comma&latlon=yes&{date_query}'
        url = f'{asos_url}{query}&station={id}'
        response = self.get_cached(url)
        if response.ok:
            return response.content.decode('utf-8')
        raise Exception(f'Request failed for {url}')

    def get_hourly_observations(self, id: str, start: datetime, end: datetime) -> pd.DataFrame:
        # we require extra data on ends for interpolation
        buffer = timedelta(days=7)

        # regardless of requested timezone, the API works in UTC
        start_utc = start.astimezone(tz=utc) - buffer
        end_utc = end.astimezone(tz=utc) + buffer

        # the CSV returned has a 5 line header
        csv = self.__get_station_csv(id, start_utc, end_utc)
        df = pd.read_csv(StringIO(csv), skiprows = 5)
        if df.size < 1:
            raise Exception(f'Error parsing {csv}')

        # we will make this an hourly dataset through interpolation,
        # but we want to preserve the original observation time
        df['idx'] = pd.to_datetime(df['valid'], utc=True)
        df['observation_time'] = df['idx']

        # missing observations will have the value 'M', make that NaN
        df['temp'] = pd.to_numeric(df['tmpf'], errors='coerce')

        # we do not want to reuse NaN values when we reindex to hourly
        df = df.dropna()
        # some of the high sample rate sites have observations with duplicate timestamps
        df = df.drop_duplicates('idx')

        # re-index to hourly 
        df = df.set_index('idx')
        utc_hours = pd.date_range(start_utc, end_utc, freq='H')
        df = df.reindex(utc_hours, method='nearest', tolerance=timedelta(minutes=30))

        # TODO: evaluate alternatives
        df['temp'] = df['temp'].interpolate(method='spline', order=3)

        # having interpolated, we can trim back to original window
        df = df[start.astimezone(tz=utc):end.astimezone(tz=utc)]

        # switch the hourly index to the requested time zone
        df['hour'] = pd.date_range(start, end, freq='H')
        df['station'] = id
        return df[['station', 'observation_time', 'temp', 'hour']].set_index('hour')

    def __get_stations(self, state, start_year):
        """ DEFUNCT
            This method could used with the following code to fillna observations
            with the geographically nearest observations.

            from geopy import distance
            nearby = iter(self.get_nearest_stations(sid))
            for nearby in self.get_nearest_stations(sid):
                print(f'trying {nearby}')
                other = self.get_station_data(nearby, start_date, end_date)
                df = df.fillna(other)
                if not any(pd.isna(df['tmpf'])):
                    return df
            return df
            def get_nearest_stations(self, sid: str, radius_miles = 20):
                stations = self.stations.copy()
                target = stations[stations['sid'] == sid].iloc[0]['latlon']
                stations['distance'] = stations['latlon'].apply(lambda c: distance.distance(target, c).mi)
                stations = stations[stations['distance'] < radius_miles]
                stations.sort_values(by=['distance'], inplace=True)
                return stations[1:]['sid'].array
        """
        url = f'https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson'
        response = self.get_cached(url)
        if response.ok:
            j = response.json()
            sites = [site["properties"] for site in j["features"]]
            for (i, site) in enumerate(sites):
                coords = j['features'][i]['geometry']['coordinates']
                site['latlon'] = (coords[1], coords[0])
            valid_sites = []
            for site in sites:
                try:
                    first_year = int(site["time_domain"][1:5])
                    if first_year <= start_year and site["time_domain"][6:9].lower() == "now":
                        valid_sites.append(site)
                except:
                    continue
            return pd.DataFrame(valid_sites)
        else:
            raise Exception(f'Request for {state} failed: {response.status_code} {response.reason}')

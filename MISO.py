regions = {'North'   : ['IA', 'MN', 'MT', 'ND', 'SD'],
           'Central' : ['IL', 'IN', 'KY', 'MI', 'MO', 'WI'],
           'South'   : ['AR', 'LA', 'MS', 'TX']}
stations_dict = {'IA' : ['DSM', 'CID'], 
                 'MN' : ['DLH', 'JKJ', 'LYV', 'MSP', 'RST'], 
                 'WI' : ['MSN', 'MKE', 'EAU', 'GRB'],
                 'MI' : ['ANJ', 'GRR', 'LAN', 'DET', 'ARB'],
                 'IN' : ['EVV', 'FWA', 'IND', 'SBN', 'SPI'],
                 'IL' : ['BMI', 'CMI', 'ARR', 'PIA'],
                 'MO' : ['STL', 'COU', 'SGF', 'MKC'],
                 'MS' : ['HKS', 'TUP', 'MEI'],
                 'LA' : ['BTR', 'LFT', 'LCH', 'SHV', 'AEX'],
                 'TX' : ['LFK'] }
miso_states = ['AR', 'IL', 'IN', 'IA', 'KY', 'LA', 'MI', 'MN',
               'MS', 'MO', 'MT', 'ND', 'SD', 'TX', 'WI']


from pathlib import Path
from xmlrpc.client import DateTime
import pandas as pd

MISO_PREDICTION_COLUMN_NAME : str = 'MISO MTLF (MWh)'
TARGET_NAME : str = 'MISO ActualLoad (MWh)'

__data__ = pd.read_parquet('./archive/linear_model/data/harmonized_data.parquet')
WEATHER_STATIONS = __data__.columns[0:38]

def prior_load_colname(i : int):
    return f"Actual Load {i} hours prior"

def add_prior_load_features(X, y, num_hours_prior = 36):
    new_features = []
    for i in range(num_hours_prior, 0, -1):
        col_name = prior_load_colname(i)
        X[col_name] = y.shift(i)
        new_features.append(col_name)
    return (X, new_features)

def get_data():
    data = __data__
    pd.set_option('mode.chained_assignment',  None)

    target_name = 'MISO ActualLoad (MWh)'
    y = data[target_name]
    feature_names = [col for col in data.columns if not col.endswith('(MWh)') and not col == 'Market Day']
    X = data[feature_names]
    X['Day of Year'] = data['Market Day'].dt.day_of_year
    feature_names.append('Day of Year')

    num_hours_prior=36
    (X, new_features) = add_prior_load_features(X, y, num_hours_prior)
    feature_names.append(new_features)
    data = X[num_hours_prior:].join(y[num_hours_prior:])

    mtlf = pd.read_parquet('./archive/linear_model/data/actuals_mtlf.parquet')
    data = data.join(mtlf[[MISO_PREDICTION_COLUMN_NAME,'time_idx']].set_index('time_idx'), how='inner')

    return data


from joblib import Parallel, delayed
from datetime import date, datetime, timedelta, timezone, time
from multiprocessing import cpu_count
from zipfile import ZipFile
from genericpath import isfile
# Downloads Daily Regional Forecast and Actual Load (xls)

# The MISO website keeps older data in a different location than more recent data
market_timezone = timezone(timedelta(hours=-5))
def prevailing_time(yyyy, mm, dd, hh):
    return datetime(yyyy, mm, dd, hh, tzinfo=market_timezone)

archive_cutoff = datetime(date.today().year - 3, 12, 31, tzinfo=market_timezone)

parallel = Parallel(n_jobs=cpu_count())

import util

class MarketReports(util.WebClient):
    def __init__(self, output_dir) -> None:
        Path(output_dir).mkdir(exist_ok=True)
        self.output_dir = output_dir

    def download(self, file_name):
        output_path = f'{self.output_dir}/{file_name}'
        if not isfile(output_path):
            url = f'https://docs.misoenergy.org/marketreports/{file_name}'
            response = self.get(url)
            with open(output_path, 'wb') as f:
                f.write(response.content)
        return output_path

    def __get_archive(self, start_date, end_date, suffix):
        month_starts = pd.date_range(start_date, end_date, freq='MS')
        for ms in month_starts:
            YYYYMM = ms.strftime('%Y%m')
            filename = f'{YYYYMM}_{suffix}_xls.zip'
            zipfile = self.download(filename)
            with ZipFile(zipfile, 'r') as zip_ref:
                zip_ref.extractall(self.output_dir)

    @staticmethod
    def __file_name(day, suffix):
        YYYYMMDD = day.strftime('%Y%m%d')
        return f'{YYYYMMDD}_{suffix}.xls'

    @staticmethod
    def __load_data(p, cols, header_rows):
        df = pd.read_excel(p, skiprows=header_rows) 
        df = df[1:25]
        common_cols = ['HourEnding', 'MISO MTLF (MWh)', 'MISO ActualLoad (MWh)']
        #HACK
        if 'LRZ8_9 MTLF (MWh)' in df.columns:
            df['LRZ8_9_10 MTLF (MWh)'] = df['LRZ8_9 MTLF (MWh)']
            df['LRZ8_9_10 ActualLoad (MWh)'] = df['LRZ8_9 ActualLoad (MWh)']

        numeric_cols = common_cols + cols
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
        df['Market Day'] = pd.to_datetime(df['Market Day'])
        df = df[['Market Day'] + numeric_cols]
        assert df.iloc[23].HourEnding == 24
        return df


    def __hourly_load(self, start_date, desired_end_date, cols, suffix, header_rows):
        # the actuals aren't available until the next day
        # so we need to shift begin and end
        end_date = desired_end_date + timedelta(days=2.0) 
        num_days = range(1, (end_date - start_date).days)
        paths = [f'{self.output_dir}/{MarketReports.__file_name(start_date + timedelta(days = d), suffix)}' for d in num_days]

        if start_date < archive_cutoff:
            archive_end = end_date if end_date <= archive_cutoff else archive_cutoff
            self.__get_archive(start_date, archive_end, suffix)

        begin = start_date if start_date > archive_cutoff else archive_cutoff + timedelta(days=1)
        all_days = pd.date_range(begin, end_date)
        _ = parallel(delayed(self.download)(MarketReports.__file_name(day, suffix)) for day in all_days)

        dfs = [MarketReports.__load_data(p, cols, header_rows) for p in paths]

        actuals = pd.concat(dfs)
        def mktime_idx(row): 
            dt = row['Market Day'].date()
            return prevailing_time(dt.year, dt.month, dt.day, row['HourEnding'] - 1)
        actuals['market_hour'] = actuals.apply(mktime_idx, axis = 1)
        return actuals.set_index('market_hour')

    def regional_hourly_load(self, start_date, end_date):
        suffix = 'rf_al'
        cols = ['North MTLF (MWh)', 'North ActualLoad (MWh)',
                'South MTLF (MWh)', 'South ActualLoad (MWh)',
                'Central MTLF (MWh)', 'Central ActualLoad (MWh)']
        return self.__hourly_load(start_date, end_date, cols, suffix, header_rows=5)

    def zonal_hourly_load(self, start_date, end_date):
        suffix = 'df_al'
        cols = ['LRZ1 MTLF (MWh)','LRZ1 ActualLoad (MWh)', 'LRZ2_7 MTLF (MWh)',
                'LRZ2_7 ActualLoad (MWh)', 'LRZ3_5 MTLF (MWh)', 'LRZ3_5 ActualLoad (MWh)',
                'LRZ4 MTLF (MWh)','LRZ4 ActualLoad (MWh)', 'LRZ6 MTLF (MWh)',
                'LRZ6 ActualLoad (MWh)', 'LRZ8_9_10 MTLF (MWh)', 'LRZ8_9_10 ActualLoad (MWh)']
        return self.__hourly_load(start_date, end_date, cols, suffix, header_rows=4)
        


from dataclasses import InitVar, dataclass
from datetime import timedelta, datetime
from functools import cached_property
import pandas as pd

@dataclass
class DataSet():
    mtlf: str
    actual: str
    features: list[str]
    test_start: datetime
    test_end: datetime
    validation_start: datetime
    validation_end: datetime
    train_start: datetime
    train_end: datetime
    
    data: InitVar[pd.DataFrame] = None
    validation_data: InitVar[pd.DataFrame] = None
    test_data: InitVar[pd.DataFrame] = None

    def __init__(self, path, mtlf, actual, features = None) -> None:
        self.data = pd.read_parquet(path)
        self.mtlf = mtlf
        self.actual = actual
        if not features:
            self.features = self.data.drop([mtlf, actual], axis=1).columns.to_list()
        else:
            self.features = features

        hours = self.data.index.to_series()
        first_hour = hours.iloc[0]
        last_hour = hours.iloc[-1]

        self.test_start = last_hour - timedelta(days=364, hours=23)
        self.test_end = last_hour
        self.validation_start = self.test_start - timedelta(days=365)
        self.validation_end = self.test_start - timedelta(hours=1)
        self.train_start = first_hour
        self.train_end = self.validation_start - timedelta(hours=1)

        self.validation_data = self.data[self.validation_start:self.validation_end]
        self.test_data = self.data[self.test_start:self.test_end]
        self.train_data = self.data[self.train_start:self.train_end]

@dataclass
class Zone1(DataSet):
    num_hours_prior = 32
    correlated_prior_hours = [1, 2, 23, 24, 25]

    @staticmethod
    def next_hour(actual, previous_hour, predicted_load):
        colname = Zone1.prior_load_colname
        next = actual.copy()
        next[colname(1)] = predicted_load
        for i in Zone1.correlated_prior_hours[1:]:
            next[colname(i)] = previous_hour[colname(i-1)].iloc[0]
        return next

    @staticmethod
    def prior_load_colname(i):
        return f"Actual Load {i} hours prior"

    def __init__(self, path, features=None) -> None:
        uncorrelated_features = ['DayOfYear', 'IsBusinessHour', 'HourEnding']
        features = self.correlated_columns + uncorrelated_features
        super().__init__(path, 'LRZ1 MTLF (MWh)', 'LRZ1 ActualLoad (MWh)', features)

    @cached_property
    def correlated_columns(self):
        return [Zone1.prior_load_colname(i) for i in Zone1.correlated_prior_hours]

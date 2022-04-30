from datetime import datetime, timedelta
from typing import List
import numpy as np
from sklearn.metrics import mean_absolute_error, max_error
from MISO import prior_load_colname, WEATHER_STATIONS

def mtlf_predict_window(model, X_test, forecasts = None):
    window_size = X_test.shape[0]
    assert(window_size > 0)
    forecast_index = window_size - 1
    if forecasts is None:
        forecasts = np.zeros(window_size)
    if computed_forecast := forecasts[forecast_index] > 0:
        return computed_forecast

    if window_size > 1:
        # predict the next window size hours, using the first prediction in the second,
        # the second in the third, and so on. Return only the last.
        for i in range(1, window_size):
            col_idx = X_test.columns.get_loc(prior_load_colname(i))
            X_test.iloc[-1, col_idx] = mtlf_predict_window(model, X_test[:-i], forecasts)

    forecasts[forecast_index] = model.predict(X_test.tail(1))
    return forecasts[forecast_index]

def mtlf_predict(model, X):
    n = X.shape[0]
    yhat = np.zeros(n)

    # "cheat" on first 17 hours
    yhat[0:16] = model.predict(X[0:16])

    mtlf_window_size = 18
    for start in range(0, n - mtlf_window_size + 2):
        stop  = mtlf_window_size + start - 1
        yhat[stop-1] = mtlf_predict_window(model, X[start:stop].copy())
    return yhat

def simulate_weather_forecast(data, stddev = 2.25):
    df = data.copy()
    forecast_error = np.random.normal(0, 2.25, df[WEATHER_STATIONS].shape)
    data_test_forecast_error[WEATHER_STATIONS] = df[WEATHER_STATIONS] + forecast_error
    data_test_forecast_error = data_test_forecast_error.drop(target_name, axis=1)

def show_error(y, yhat):
    MAE = mean_absolute_error(y, yhat)
    m = max_error(y, yhat)
    total = sum(abs(y - yhat))
    return f"Mean Absolute Error = {MAE}, Max Error = {m}, Total Error = {total}"

from dataclasses import InitVar, dataclass
@dataclass
class Error():
    mae:   float
    max:   float
    total: float
    y: InitVar[List] = None
    yhat: InitVar[List] = None

    def __init__(self, y, yhat) -> None:
        self.mae = mean_absolute_error(y, yhat)
        self.max = max_error(y, yhat)
        self.total = sum(abs(y - yhat))

import pandas as pd

from joblib import Parallel, delayed
from multiprocessing import cpu_count
parallel = Parallel(n_jobs=cpu_count())

def walkforward(model, all_X, all_y, start_hour, end_hour, next_hour, active_features):
    stride = 24 # hours
    d = (end_hour - start_hour) + timedelta(hours=1) 
    total_hours = d.days * 24 + d.seconds // 3600
    strides = total_hours / stride
    assert strides - int(strides) == 0
    strides = int(strides)

    X = all_X[:start_hour]
    y = all_y[:start_hour]

    Xvalid = all_X[start_hour:end_hour]
    yvalid = all_y[start_hour:end_hour]

    def step(d):
        next_Xtrain = pd.concat([X, Xvalid.iloc[:stride*d]])
        next_ytrain = pd.concat([y, yvalid.iloc[:stride*d]])

        next_Xpredict = Xvalid.iloc[stride*d:stride*(d+1)]
        next_y        = yvalid.iloc[stride*d:stride*(d+1)]

        #print(f'Predicting {next_Xpredict.index.to_series().iloc[0]}')
        #print(f'Predicting {next_Xpredict.index.to_series().iloc[-1]}')

        prediction = hourly_prediction(model.fit(next_Xtrain[active_features], next_ytrain), next_Xpredict, next_hour, active_features)
        error = Error(y=next_y, yhat=prediction)
        return d, prediction, error 

    #results = parallel(delayed(step)(d) for d in range(0, strides))
    results = [step(d) for d in range(0, strides)]
    results = sorted(results, key=lambda r: r[0])
    predictions = [r[1] for r in results]
    errors = [r[2] for r in results]
    
    return (predictions, errors)

# we cannot let the actuals leak into the validation set
def hourly_prediction(fitted_model, Xpredict, next_hour, active_features):
    yhats = []
    # HACK: assume the first hour has the current hour STLF 
    # and short term weather forecast instead of actual
    # TODO: implement some noise to simulate these
    x = Xpredict.iloc[0:1]
    yhats.append(fitted_model.predict(x[active_features])[0])
    for i in range(1, Xpredict.shape[0]):
        yhat = yhats[i - 1]
        x = next_hour(Xpredict.iloc[i:i+1], x, yhat)
        yhats.append(fitted_model.predict(x[active_features])[0])
    return yhats

from datetime import timedelta
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

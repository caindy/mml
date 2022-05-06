from datetime import datetime, timedelta
from typing import List
import numpy as np
from sklearn.metrics import mean_absolute_error, max_error

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

        fitted_model = model.fit(next_Xtrain[active_features], next_ytrain)
        prediction = hourly_prediction(fitted_model, next_Xpredict, next_hour, active_features)
        error = Error(y=next_y, yhat=prediction)
        return d, prediction, error 

    results = parallel(delayed(step)(d) for d in range(0, strides))
    #results = [step(d) for d in range(0, strides)]
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


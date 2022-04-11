import numpy as np
from sklearn.metrics import mean_absolute_error, max_error
from data import prior_load_colname, WEATHER_STATIONS

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

def validate(model, data, start_hour, end_hour):
    pass

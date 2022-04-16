
import pandas as pd

MISO_PREDICTION_COLUMN_NAME : str = 'MISO MTLF (MWh)'
TARGET_NAME : str = 'MISO ActualLoad (MWh)'

__data__ = pd.read_parquet('../linear_model/data/harmonized_data.parquet')
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

    mtlf = pd.read_parquet('../linear_model/data/actuals_mtlf.parquet')
    data = data.join(mtlf[[MISO_PREDICTION_COLUMN_NAME,'time_idx']].set_index('time_idx'), how='inner')

    return data

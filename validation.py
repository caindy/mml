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

import numpy as np
import tensorflow as tf
import matplotlib.pyplot as plt

class WindowGenerator():
    def __init__(self, input_width, label_width, shift,
                train_df, val_df, test_df,
                label_columns=None):
        # Store the raw data.
        self.train_df = train_df
        self.val_df = val_df
        self.test_df = test_df

        # Work out the label column indices.
        self.label_columns = label_columns
        if label_columns is not None:
            self.label_columns_indices = {name: i for i, name in
                                        enumerate(label_columns)}
        self.column_indices = {name: i for i, name in
                                enumerate(train_df.columns)}

        # Work out the window parameters.
        self.input_width = input_width
        self.label_width = label_width
        self.shift = shift

        self.total_window_size = input_width + shift

        self.input_slice = slice(0, input_width)
        self.input_indices = np.arange(self.total_window_size)[self.input_slice]

        self.label_start = self.total_window_size - self.label_width
        self.labels_slice = slice(self.label_start, None)
        self.label_indices = np.arange(self.total_window_size)[self.labels_slice]

    def __repr__(self):
        return '\n'.join([
            f'Total window size: {self.total_window_size}',
            f'Input indices: {self.input_indices}',
            f'Label indices: {self.label_indices}',
            f'Label column name(s): {self.label_columns}'])

    def split_window(self, features):
        inputs = features[:, self.input_slice, :]
        labels = features[:, self.labels_slice, :]
        if self.label_columns is not None:
            labels = tf.stack(
                [labels[:, :, self.column_indices[name]] for name in self.label_columns],
                axis=-1)

        # Slicing doesn't preserve static shape information, so set the shapes
        # manually. This way the `tf.data.Datasets` are easier to inspect.
        inputs.set_shape([None, self.input_width, None])
        labels.set_shape([None, self.label_width, None])
        return inputs, labels

    def make_dataset(self, data):
        data = np.array(data, dtype=np.float32)
        ds = tf.keras.utils.timeseries_dataset_from_array(
            data=data,
            targets=None,
            sequence_length=self.total_window_size,
            sequence_stride=1,
            shuffle=True,
            batch_size=32,)

        ds = ds.map(self.split_window)
        return ds

    def plot(self, model, plot_col = None, max_subplots=3):
        plot_col = self.label_columns[0] if plot_col is None else plot_col
        inputs, labels = self.example
        plt.figure(figsize=(12, 8))
        plot_col_index = self.column_indices[plot_col]
        max_n = min(max_subplots, len(inputs))
        for n in range(max_n):
            plt.subplot(max_n, 1, n+1)
            plt.ylabel(f'{plot_col} [normed]')
            plt.plot(self.input_indices, inputs[n, :, plot_col_index],
                    label='Inputs', marker='.', zorder=-10)

            if self.label_columns:
                label_col_index = self.label_columns_indices.get(plot_col, None)
            else:
                label_col_index = plot_col_index

            if label_col_index is None:
                continue

            plt.scatter(self.label_indices, labels[n, :, label_col_index],
                    edgecolors='k', label='Labels', c='#2ca02c', s=64)
            if model is not None:
                predictions = model(inputs)
                plt.scatter(self.label_indices, predictions[n, :, label_col_index],
                        marker='X', edgecolors='k', label='Predictions',
                        c='#ff7f0e', s=64)

            if n == 0:
                plt.legend()

        plt.xlabel('Time [h]')


    @property
    def train(self):
        return self.make_dataset(self.train_df)

    @property
    def val(self):
        return self.make_dataset(self.val_df)

    @property
    def test(self):
        return self.make_dataset(self.test_df)

    @property
    def example(self):
        """Get and cache an example batch of `inputs, labels` for plotting."""
        result = getattr(self, '_example', None)
        if result is None:
            # No example batch was found, so get one from the `.train` dataset
            result = next(iter(self.train))
            # And cache it for next time
            self._example = result
        return result

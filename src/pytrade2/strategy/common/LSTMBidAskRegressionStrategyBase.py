from typing import Dict

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, FunctionTransformer

from pytrade2.exch.Exchange import Exchange
from pytrade2.strategy.common.BidAskRegressionStrategyBase import BidAskRegressionStrategyBase
from pytrade2.features.PredictBidAskFeatures import PredictBidAskFeatures


class LSTMBidAskRegressionStrategyBase(BidAskRegressionStrategyBase):
    """ LSTM Strategies base class with lstm window support"""

    def __init__(self, config: Dict, exchange_provider: Exchange):
        BidAskRegressionStrategyBase.__init__(self, config=config, exchange_provider=exchange_provider)
        # lstm window
        self.lstm_window_size = config["pytrade2.strategy.lstm.window.size"]
        self.min_xy_len = self.lstm_window_size + 1
        self._logger.info(f"LSTM window size: {self.lstm_window_size}")

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """

        time_cols = [col for col in X.columns if col.startswith("time")]
        float_cols = list(set(X.columns) - set(time_cols))

        x_pipe = Pipeline([
            ("xscaler", ColumnTransformer([("xrs", RobustScaler(), float_cols)], remainder="passthrough")),
             ("xmms", MinMaxScaler()),
             ("reshape",
              FunctionTransformer(LSTMBidAskRegressionStrategyBase.reshape_x,
                                  kw_args={'window_shape': (self.lstm_window_size, len(X.columns))}))
             ])
        x_pipe.fit(X)

        y_pipe = Pipeline(
            [("yrs", RobustScaler()),
             ("ymms", MinMaxScaler())])
        y_pipe.fit(y)
        return x_pipe, y_pipe

    @staticmethod
    def reshape_x(x, window_shape):
        arr = x.values if isinstance(x, pd.DataFrame) else x
        shape = (max(0,arr.shape[0] - window_shape[0]+1), window_shape[0], arr.shape[1])
        strides = (arr.strides[0], arr.strides[0], arr.strides[1])
        windowed = np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)
        return windowed

    def prepare_last_x(self) -> pd.DataFrame:
        """ Reshape last features to lstm window"""
        x = PredictBidAskFeatures.last_features_of(self.bid_ask_feed.bid_ask,
                                                   self.lstm_window_size,
                                                   self.level2_feed.level2,
                                                   self.candles_feed.candles_by_interval,
                                                   self.candles_feed.candles_cnt_by_interval,
                                                   past_window=self.past_window)
        return x

import math
from typing import Dict

import numpy as np
import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from numpy import ndarray

from exch.Exchange import Exchange
from strategy.common.PredictBidAskStrategyBase import PredictBidAskStrategyBase
from strategy.common.features.CandlesFeatures import CandlesFeatures
from strategy.common.features.PredictBidAskFeatures import PredictBidAskFeatures


class LSTMStrategyBase(PredictBidAskStrategyBase):
    """ LSTM Strategies base class with lstm window support"""

    def __init__(self, config: Dict, exchange_provider: Exchange):
        PredictBidAskStrategyBase.__init__(self, config=config, exchange_provider=exchange_provider)
        # lstm window
        self.lstm_window_size = config["pytrade2.strategy.lstm.window.size"]
        self.min_xy_len = self.lstm_window_size + 1
        self._log.info(f"LSTM window size: {self.lstm_window_size}")

    def prepare_last_X(self) -> (pd.DataFrame, ndarray):
        """ Reshape last features to lstm window"""
        X = PredictBidAskFeatures.last_features_of(self.bid_ask,
                                                   self.lstm_window_size,
                                                   self.level2,
                                                   self.candles_by_interval,
                                                   self.candles_cnt_by_interval,
                                                   past_window=self.past_window)
        X_trans = self.X_pipe.transform(X)

        if math.prod(X_trans.shape) >= self.lstm_window_size * X_trans.shape[1]:
            X_reshaped = np.reshape(X_trans, (-1, self.lstm_window_size, X_trans.shape[1]))
        else:
            X_reshaped = np.empty((0, self.lstm_window_size, X_trans.shape[1]))

        return X, X_reshaped

    def generator_of(self, train_X, train_y):
        """ Learning data generator. Override here to return shapes with lstm window"""
        return TimeseriesGenerator(train_X, train_y, length=self.lstm_window_size)

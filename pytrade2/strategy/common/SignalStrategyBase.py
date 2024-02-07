import logging
from typing import Dict

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, OneHotEncoder

from exch.Exchange import Exchange
from strategy.common.LearnDataBalancer import LearnDataBalancer
from strategy.common.StrategyBase import StrategyBase
from strategy.features.LongCandleFeatures import LongCandleFeatures
from strategy.signal.OrderParamsByLastCandle import OrderParamsByLastCandle


class SignalStrategyBase(StrategyBase):

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """
        x_pipe, _ = super().create_pipe(X, y)

        # One hot encode y
        y_pipe = Pipeline([('adjust_labels', OneHotEncoder(categories=[[-1, 0, 1]], sparse_output=False, drop=None))])
        y_pipe.fit(y)

        return x_pipe, y_pipe

from functools import reduce

import pandas as pd

from features.Features import Features
from features.Targets import TargetFeatures


class FeatureEngineering:

    def features_and_targets_balanced(self, data: pd.DataFrame, window_size: int) -> (pd.DataFrame, pd.DataFrame):
        """
        Get features, targets, balanced by buy/sell singal
        """
        return self.balanced(*self.features_and_targets(data, window_size))

    def balanced(self, X: pd.DataFrame, y: pd.DataFrame):
        """
        Make X, y balanced by buy/sell/offmarket signal count
        """
        signals = ["signal_buy", "signal_sell", "signal_off_market"]
        mincount = min([y["signal_buy"].sum(), y["signal_sell"].sum(), y["signal_off_market"].sum()])
        y_bal = reduce(lambda df1, df2: df1.append(df2).sort_index(),
                       [y[y[signal] == 1].sample(n=mincount) for signal in signals])
        X_bal = X[X.index.isin(y_bal.index)].sort_index()
        return X_bal, y_bal

    def features_and_targets(self, data: pd.DataFrame, window_size: int) -> (pd.DataFrame, pd.DataFrame):
        """
        Features and target of
        :param data: candles data
        :return: (features, target)
        """
        features = Features().features_of(candles=data, period=1, freq="min", n=window_size).diff().dropna()
        target = TargetFeatures().target_of(df=data, periods=1, freq="min", loss=0, trailing=0, ratio=4).dropna()
        # features and target should have the same indices
        target = target[target.index.isin(features.index)]
        features = features[features.index.isin(target.index)]
        return features, target

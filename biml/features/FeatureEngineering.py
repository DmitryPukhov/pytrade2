from functools import reduce

import pandas as pd
from sklearn.compose import ColumnTransformer, make_column_transformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder

from features.Features import Features
from features.Targets import TargetFeatures


class FeatureEngineering:
    def column_transformer(self, X: pd.DataFrame, y: pd.DataFrame):
        return ColumnTransformer(
            [
                ('scaler', StandardScaler(), X.columns)
                # ('cat_encoder', OneHotEncoder(handle_unknown="ignore"), y.columns)
            ]
        )

    def features_and_targets_balanced(self, data: pd.DataFrame, window_size: int, predict_window_size: int) \
            -> (pd.DataFrame, pd.DataFrame):
        """
        Get features, targets, balanced by buy/sell signal
        """
        return self.balanced(*self.features_and_targets(data, window_size, predict_window_size))

    def balanced(self, X: pd.DataFrame, y: pd.DataFrame):
        """
        Make X, y balanced by buy/sell/offmarket signal count
        """
        vc = y["signal"].value_counts()
        mincount = min(vc.values)
        y_bal = reduce(lambda df1, df2: df1.append(df2).sort_index(),
                       [y[y["signal"] == signal].sample(n=mincount) for signal in vc.index.values])
        X_bal = X[X.index.isin(y_bal.index)].sort_index()
        return X_bal, y_bal

    def features(self, data: pd.DataFrame, window_size: int):
        return Features().features_of(candles=data, period=1, freq="min", n=window_size).diff()

    def features_and_targets(self, data: pd.DataFrame, window_size: int, predict_window_size: int) -> (
            pd.DataFrame, pd.DataFrame):
        """
        Features and target of the data
        :return: (features, target)
        """
        features = self.features(data, window_size).dropna()
        target = TargetFeatures().target_of(df=data, periods=predict_window_size, freq="min", loss=0, trailing=0,
                                            ratio=4).dropna()
        # features and target should have the same indices
        target = target[target.index.isin(features.index)]
        features = features[features.index.isin(target.index)]
        return features, target

from functools import reduce
from typing import Dict

import pandas as pd
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier

from features.Features import Features
from features.TargetFeatures import TargetFeatures


class FutureLowHigh:
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """
    def __init__(self):
        self.model = DecisionTreeClassifier(criterion="entropy", max_depth=3)

    def prepare(self, data: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        """
        Features and target of
        :param data: candles data
        :return: (features, target)
        """
        features = Features().features_of(candles=data, period=1, freq="min", n=60).dropna()
        target = TargetFeatures().target_of(df=data, periods=5, freq="min", loss=0, trailing=0, ratio=4).dropna()
        # features and target should have the same indices
        target = target[target.index.isin(features.index)]
        features = features[features.index.isin(target.index)]

        return features, target

    def learn(self, data_items: Dict):
        """
        Learn the model on historical data
        :param data_items: Dict{(ticker, interval): dataframe]
        """
        # this strategy is single asset and interval, the only item in dict,
        # but for the sake of logic let's concatenate the dict vals
        data = reduce(lambda df1, df2: df1.append(df2), data_items.values()).sort_index()

        # Train-test data
        X, y = self.prepare(data)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, shuffle=False)

        # Fit the model
        model = self.model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
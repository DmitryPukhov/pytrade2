from functools import reduce
from typing import Dict

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from features.FeatureEngineering import FeatureEngineering


class FutureLowHigh:
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """
    def __init__(self):
        self.model = DecisionTreeClassifier(criterion="entropy", max_depth=3)

    def learn(self, data_items: Dict):
        """
        Learn the model on historical data
        :param data_items: Dict{(ticker, interval): dataframe]
        """
        # this strategy is single asset and interval, the only item in dict,
        # but for the sake of logic let's concatenate the dict vals
        data = reduce(lambda df1, df2: df1.append(df2), data_items.values()).sort_index()

        # Feature engineering. todo: balance the data
        fe = FeatureEngineering()
        X, y = fe.features_and_targets_balanced(data)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, shuffle=False)
        print(f"Train set size: {len(X_train)}, test set size: {len(X_test)}")

        # ax = sns.countplot(y_train["signal"])
        # ax.bar_label(ax.containers[0])
        # ax.set_title("Signal distribution balanced")
        # plt.show()

        # Fit the model
        model = self.model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        print("Accuracy:", metrics.balanced_accuracy_score(y_test, y_pred))

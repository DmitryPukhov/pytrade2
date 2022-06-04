from functools import reduce
from typing import List, Dict

import pandas as pd


class FutureLowHigh:
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    def learn(self, data_items: Dict):
        """
        Learn the model on historical data
        :param data_items: Dict{(ticker, interval): dataframe]
        """
        # this strategy is single asset and interval, the only item in dict,
        # but for the sake of logic let's concatenate the dict vals
        data = reduce(lambda df1, df2: df1.append(df2), data_items.values())
        print(data.head())

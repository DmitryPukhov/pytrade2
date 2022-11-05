import collections
import glob
import logging
from collections import defaultdict
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Dict, List
import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential, Model
from scikeras.wrappers import KerasRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from AppTools import AppTools
from feed.BinanceCandlesFeed import BinanceCandlesFeed
from feed.BinanceWebsocketFeed import BinanceWebsocketFeed
from feed.TickerInfo import TickerInfo
from strategy.predictlowhighcandles.LowHighCandlesFeatures import LowHighCandlesFeatures
from strategy.StrategyBase import StrategyBase


class PredictLowHighStrategy(StrategyBase):
    """
    Candles based. Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    # def __init__(self, broker, ticker: str, model_dir: str):
    def __init__(self, broker, config: Dict):
        super().__init__(broker)
        self.config = config
        self.tickers = self.config["biml.tickers"].split(",")
        self.model_dir = self.config["biml.model.dir"]

        if self.model_dir:
            self.model_weights_dir = str(Path(self.model_dir, self.__class__.__name__, "weights"))
            self.model_Xy_dir = str(Path(self.model_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)
        self.model = None
        self.window_size = 15
        self.candles_size = self.window_size * 100
        self.predict_sindow_size = 1
        self.candles = pd.DataFrame()
        self.model = None

        # Minimum stop loss ratio = (price-stop_loss)/price
        self.min_stop_loss_ratio = 0.005
        # Minimum profit/loss
        self.profit_loss_ratio = 4

        self.logger = logging.getLogger(self.__class__.__name__)

        if self.broker:
            for ticker in self.tickers:
                self.broker.close_opened_positions(ticker)
                # Raise exception if we are in trade for this ticker
                self.assert_out_of_market(ticker)

    def run(self, client):
        """
        Attach to the feed and listen
        """
        feed = BinanceWebsocketFeed(tickers=self.tickers)
        feed.consumers.append(self)
        feed.run()

    def on_bid_ask(self, bid_ask: dict):
        logging.info(f"Got bid_ask {bid_ask}")
        pass



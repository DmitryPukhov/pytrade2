import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from exch.binance.broker.BinanceBroker import BinanceBrokerSpot
from model.Trade import Trade
from strategy.common.PredictBidAskStrategyBase import PredictBidAskStrategyBase


class ModelStub:
    """ Model emulation for unit tests"""

    def predict(self, X, verbose):
        # Emulate some prediction: bid_min_fut_diff, bit_max_fut_diff, ask_min_fut_diff, ask_max_fut_diff
        return np.array([1, 2, 3, 4])


class BrokerStub(BinanceBrokerSpot):
    """ Broker emulation, don't trade """

    def __init__(self):
        self._log = logging.getLogger(self.__class__.__name__)
        self.cur_trade: Trade = None

    def update_trade_status(self, trade: Trade) -> Trade:
        pass

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: Optional[float],
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        """ Don't trade, just emulate """
        self.cur_trade = Trade(ticker=symbol, side=Trade.order_side_names.get(direction),
                               open_time=datetime.utcnow(), open_price=price, open_order_id=None,
                               stop_loss_price=stop_loss_price, take_profit_price=take_profit_price,
                               stop_loss_order_id=None,
                               quantity=quantity)

    def end_cur_trade(self):
        self.cur_trade = None


class StrategyStub(PredictBidAskStrategyBase):
    """ Strategy wrapper for tests """

    def __init__(self):
        conf = {"pytrade2.tickers": "test", "pytrade2.strategy.learn.interval.sec": 60,
                "pytrade2.data.dir": "tmp",
                "pytrade2.price.precision": 2,
                "pytrade2.amount.precision": 2,
                "pytrade2.strategy.predict.window": "10s",
                "pytrade2.strategy.past.window": "1s",
                "pytrade2.strategy.history.min.window": "10s",
                "pytrade2.strategy.history.max.window": "10s",

                "pytrade2.feed.candles.periods": "1min,5min",
                "pytrade2.feed.candles.counts": "1,1",
                "pytrade2.order.quantity": 0.001}
        PredictBidAskStrategyBase.__init__(self, conf, None)
        self.profit_loss_ratio = 4
        self.close_profit_loss_ratio = 2
        self.model = ModelStub()
        self.broker = BrokerStub()
        self.min_stop_loss = 0
        self.max_stop_loss_coeff = float('inf')
        self.candles_by_interval = {"1min":
            pd.DataFrame([
                {
                    "close_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                    "open_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                    "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5},
                {
                    "close_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                    "open_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                    "open": 1.1, "high": 2.2, "low": 3.3, "close": 4.4, "vol": 5.5}
            ]),
            "5min":
                pd.DataFrame([
                    {
                        "close_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                        "open_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                        "open": 1, "high": 2, "low": 3, "close": 4, "vol": 5
                    },
                    {
                        "close_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                        "open_time": datetime.fromisoformat("2023-03-17 15:56:01"),
                        "open": 1.1, "high": 2.2, "low": 3.3, "close": 4.4, "vol": 5.5
                    },
                ])
                .set_index("close_time", drop=False)}
        self.candles_cnt_by_interval={"1min":1, "5min":1}

    def save_lastXy(self, X_last: pd.DataFrame, y_pred_last: pd.DataFrame, data_last: pd.DataFrame):
        pass

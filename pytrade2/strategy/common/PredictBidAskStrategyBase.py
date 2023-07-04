import multiprocessing
import traceback
from datetime import datetime
from io import StringIO
from threading import Event, Timer
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from numpy import ndarray

from exch.Exchange import Exchange
from strategy.common.CandlesStrategy import CandlesStrategy
from strategy.common.StrategyBase import StrategyBase
from strategy.common.features.PredictBidAskFeatures import PredictBidAskFeatures


class PredictBidAskStrategyBase(StrategyBase, CandlesStrategy):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None
        self.candles_feed = None

        StrategyBase.__init__(self, config, exchange_provider)
        CandlesStrategy.__init__(self, config=config, ticker=self.ticker, candles_feed=self.candles_feed)
        self.data_lock = multiprocessing.RLock()
        self.new_data_event: Event = Event()

        # Learn params
        self.predict_window = config["pytrade2.strategy.predict.window"]
        self.past_window = config["pytrade2.strategy.past.window"]
        self.history_min_window = pd.Timedelta(config["pytrade2.strategy.history.min.window"])
        self.history_max_window = pd.Timedelta(config["pytrade2.strategy.history.max.window"])



        # Price, level2 dataframes and their buffers
        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.bid_ask_buf: pd.DataFrame = pd.DataFrame()  # Buffer
        self.level2: pd.DataFrame = pd.DataFrame()
        self.level2_buf: pd.DataFrame = pd.DataFrame()  # Buffer

        self.fut_low_high: pd.DataFrame = pd.DataFrame()
        self.last_learn_bidask_time = datetime(1970, 1, 1)

        self._log.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

    def get_report(self):
        """ Short info for report """

        msg = StringIO()
        # Broker report
        if hasattr(self.broker, "get_report"):
            msg.write(self.broker.get_report())
        # Bid ask report
        msg.write(f"\nBid ask ")
        msg.write(
            f"cnt:{self.bid_ask.index.size}, first:{self.bid_ask.index.min()}, last: {self.bid_ask.index.max()}" if not self.bid_ask.empty else "is empty")

        # Level2 report
        msg.write(f"\nLevel2 ")
        msg.write(
            f"cnt:{self.level2.index.size}, first: {self.level2.index.min()}, last: {self.level2.index.max()}" if not self.level2.empty else "is empty")

        # Candles report
        for i, t in self.last_candles_info().items():
            msg.write(f"\nLast {i} candle: {t}")
        return msg.getvalue()

    def run(self):
        """
        Attach to the feed and listen
        """
        exchange_name = self.config["pytrade2.exchange"]

        # Create feed and broker
        self.websocket_feed = self.exchange_provider.websocket_feed(exchange_name)
        self.websocket_feed.consumers.add(self)
        self.candles_feed = self.exchange_provider.candles_feed(exchange_name)
        self.candles_feed.consumers.add(self)

        self.broker = self.exchange_provider.broker(exchange_name)

        self.read_initial_candles()

        StrategyBase.run(self)

        if self.purge_interval:
            self._log.info(f"Starting periodical purging, interval: {self.purge_interval}")
            Timer(self.purge_interval.seconds, self.purge_all).start()
        # Run the feed, listen events
        self.websocket_feed.run()
        self.candles_feed.run()
        self.broker.run()

    def is_alive(self):
        maxdelta = self.history_min_window + pd.Timedelta("60s")
        dt = datetime.utcnow()

        if not self.bid_ask.empty and dt - self.bid_ask.index.max() > maxdelta:
            is_alive = False
        elif not self.level2.empty and dt - self.level2.index.max() > maxdelta:
            is_alive = False
        else:
            is_alive = CandlesStrategy.is_alive(self)
        if not is_alive:
            last_bid_ask = self.bid_ask.index.max() if not self.bid_ask.empty else None
            last_level2 = self.level2.index.max() if not self.level2.empty else None
            last_candles = [(i, c.index.max()) for i, c in self.candles_by_interval.items()]
            self._log.info(f"isNow: {dt}, maxdelta: {maxdelta}, last bid ask: {last_bid_ask}, last level2: {last_level2},"
                           f"last candles: {last_candles}")
        return is_alive

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

        # Add new data to df
        new_df = pd.DataFrame(level2, columns=bid_ask_columns).set_index("datetime", drop=False)
        with self.data_lock:
            self.level2_buf = pd.concat([self.level2_buf, new_df])

        self.new_data_event.set()

    def on_ticker(self, ticker: dict):
        # Add new data to df
        new_df = pd.DataFrame([ticker], columns=list(ticker.keys())).set_index("datetime")
        with self.data_lock:
            self.bid_ask_buf = pd.concat([self.bid_ask_buf, new_df])

        self.new_data_event.set()

    def purge_all(self):
        """ Purge old data to reduce memory usage"""
        try:
            with self.data_lock:
                self.bid_ask = self.purged(self.bid_ask, "bid_ask")
                self.level2 = self.purged(self.level2, "level2")
                self.fut_low_high = self.purged(self.fut_low_high, "fut_low_high")
        finally:
            if self.purge_interval:
                Timer(self.purge_interval.seconds, self.purge_all).start()

    def purged(self, df: Optional[pd.DataFrame], tag: str) -> Optional[pd.DataFrame]:
        """
        Purge data frame
        """
        purge_window = self.history_max_window
        self._log.debug(f"Purging old {tag} data using window {purge_window}")
        if df is None or df.empty:
            return df
        left_bound = df.index.max() - pd.Timedelta(purge_window)
        return df[df.index >= left_bound]

    def process_new_data(self):

        # Append new data from buffers to main data frames
        with self.data_lock:
            self.bid_ask = pd.concat([self.bid_ask, self.bid_ask_buf]).sort_index()
            self.bid_ask_buf = pd.DataFrame()
            self.level2 = pd.concat([self.level2, self.level2_buf]).sort_index()
            self.level2_buf = pd.DataFrame()

        if not self.bid_ask.empty and not self.level2.empty and self.model \
                and not self.is_processing and self.X_pipe and self.y_pipe:
            try:
                self.is_processing = True
                # Predict
                X, y = self.predict_low_high()
                if X.empty:
                    return
                self.fut_low_high = y

                # Open or close or do nothing
                open_signal = self.process_new_prediction()
                cur_trade_direction = self.broker.cur_trade.direction() if self.broker.cur_trade else 0
                y[["predict_window", "open_signal", "cur_trade"]] = \
                    [self.predict_window, open_signal, cur_trade_direction]

                # Save to historical data
                self.save_lastXy(X.tail(1), y.tail(1), self.bid_ask.tail(1))
            except Exception as e:
                self._log.error(f"{e}. Traceback: {traceback.format_exc()}")
            finally:
                self.is_processing = False

    def process_new_prediction(self) -> int:
        """ Process last prediction, open a new order, save history if needed
        @:return open signal where signal can be 0,-1,1 """

        bid = self.bid_ask.loc[self.bid_ask.index[-1], "bid"]
        ask = self.bid_ask.loc[self.bid_ask.index[-1], "ask"]

        # Update current trade status
        self.check_cur_trade()

        bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut = self.fut_low_high.loc[self.fut_low_high.index[-1], \
            ["bid_min_fut", "bid_max_fut",
             "ask_min_fut", "ask_max_fut"]]
        open_signal = 0
        if not self.broker.cur_trade and self.risk_manager.can_trade():
            # Maybe open a new order
            open_signal, open_price, stop_loss, take_profit = self.get_signal(bid, ask, bid_min_fut, bid_max_fut,
                                                                              ask_min_fut,
                                                                              ask_max_fut)
            if open_signal:
                self.broker.create_cur_trade(symbol=self.ticker, direction=open_signal, quantity=self.order_quantity,
                                             price=open_price,
                                             stop_loss_price=stop_loss,
                                             take_profit_price=take_profit)
        return open_signal

    def get_signal(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float, ask_min_fut: float,
                   ask_max_fut: float) -> (int, float, float, float):
        """ Calculate buy, sell or nothing signal based on predictions and profit/loss ratio
        :return (<-1 for sell, 0 for none, 1 for buy>, stop loss, take profit)"""

        buy_profit = bid_max_fut - ask
        buy_loss = ask - bid_min_fut
        sell_profit = bid - ask_min_fut
        sell_loss = ask_max_fut - bid

        # Buy signal
        # Not zeroes and ratio is ok and max/min are ok
        is_buy_ratio = buy_profit > 0 and (buy_loss <= 0 or buy_profit / buy_loss >= self.profit_loss_ratio)
        is_buy_loss = abs(buy_loss) < self.stop_loss_max_coeff * ask
        is_buy_profit = abs(buy_profit) >= self.profit_min_coeff * ask
        is_buy = is_buy_ratio and is_buy_loss and is_buy_profit

        # Sell signal
        # Not zeroes and ratio is ok and max/min are ok
        is_sell_ratio = sell_profit > 0 and (sell_loss <= 0 or sell_profit / sell_loss >= self.profit_loss_ratio)
        is_sell_loss = abs(sell_loss) < self.stop_loss_max_coeff * bid
        is_sell_profit = abs(sell_profit) >= self.profit_min_coeff * bid
        is_sell = is_sell_ratio and is_sell_loss and is_sell_profit

        # This should not happen, but let's handle it and clear the flags
        if is_buy and is_sell:
            is_buy = is_sell = False

        if is_buy:
            # Buy and possibly fix the loss
            stop_loss_adj = ask - abs(buy_loss) * 1.25
            stop_loss_adj = min(stop_loss_adj, round(ask * (1 - self.stop_loss_min_coeff), self.price_precision))
            return 1, ask, stop_loss_adj, ask + buy_profit
        elif is_sell:
            # Sell and possibly fix the loss
            stop_loss_adj = bid + abs(sell_loss) * 1.25
            stop_loss_adj = max(stop_loss_adj, round(bid * (1 + self.stop_loss_min_coeff), self.price_precision))
            return -1, bid, stop_loss_adj, bid - sell_profit
        else:
            # No action
            return 0, None, None, None

    def predict_low_high(self) -> (pd.DataFrame, pd.DataFrame):
        # X - features with absolute values, x_prepared - nd array fith final scaling and normalization
        X, X_prepared = self.prepare_last_X()
        # Predict
        y = self.model.predict(X_prepared, verbose=0) if not X.empty else np.array([np.nan, np.nan, np.nan, np.nan])
        y = y.reshape((-1, 4))

        # Get prediction result
        y = self.y_pipe.inverse_transform(y)
        (bid_max_fut_diff, bid_spread_fut, ask_min_fut_diff, ask_spread_fut) = y[-1]  # if y.shape[0] < 2 else y
        y_df = self.bid_ask.loc[X.index][["bid", "ask"]]
        y_df["bid_max_fut"] = y_df["bid"] + bid_max_fut_diff
        y_df["bid_min_fut"] = y_df["bid_max_fut"] - bid_spread_fut
        y_df["ask_min_fut"] = y_df["ask"] + ask_min_fut_diff
        y_df["ask_max_fut"] = y_df["ask_min_fut"] + ask_spread_fut
        return X, y_df

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""

        no_bidask = self.bid_ask.empty
        no_candles = not self.has_all_candles()
        no_level2 = self.level2.empty
        no_level2_bid = "bid" not in self.level2.columns or self.level2["bid"].empty
        no_level2_ask = "ask" not in self.level2.columns or self.level2["ask"].empty

        if no_bidask or no_candles or no_level2 or no_level2_bid or no_level2_ask:
            self._log.debug(f"Can not learn because some datasets are empty. "
                            f"level2.empty: {no_level2}, "
                            f"level2.bid.empty: {no_level2_bid}, "
                            f"level2.ask.empty: {no_level2_ask}, "
                            f"candles.empty: {no_candles}")
            return False

        # Check If we have enough data to learn
        interval = self.bid_ask.index.max() - self.bid_ask.index.min()
        if interval < self.history_min_window:
            self._log.debug(
                f"Can not learn because not enough history. We have {interval}, but we need {self.history_min_window}")
            return False
        return True

    def prepare_last_X(self) -> (pd.DataFrame, ndarray):
        """ Get last X for prediction"""
        X = PredictBidAskFeatures.last_features_of(self.bid_ask,
                                                   1,  # For diff
                                                   self.level2,
                                                   self.candles_by_interval,
                                                   self.candles_cnt_by_interval,
                                                   past_window=self.past_window)
        return X, (self.X_pipe.transform(X) if X.shape[0] else pd.DataFrame())

    def prepare_Xy(self)->(pd.DataFrame, pd.DataFrame):
        """ Prepare train data """
        with self.data_lock:
            # Copy data for this thread only
            bid_ask = self.bid_ask.copy()
            level2 = self.level2.copy()

        return PredictBidAskFeatures.features_targets_of(
            bid_ask,
            level2,
            self.candles_by_interval,
            self.candles_cnt_by_interval,
            self.predict_window,
            self.past_window)



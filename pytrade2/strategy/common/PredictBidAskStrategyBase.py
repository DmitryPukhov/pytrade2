import logging
import multiprocessing
import traceback
from datetime import datetime
from io import StringIO
from threading import Event, Timer
from typing import Dict, Optional

import numpy as np
import pandas as pd
from numpy import ndarray

from exch.Exchange import Exchange
from strategy.feed.BidAskFeed import BidAskFeed
from strategy.feed.CandlesFeed import CandlesFeed
from strategy.feed.Level2Feed import Level2Feed
from strategy.common.StrategyBase import StrategyBase
from strategy.features.PredictBidAskFeatures import PredictBidAskFeatures


class PredictBidAskStrategyBase(StrategyBase):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None

        StrategyBase.__init__(self, config, exchange_provider)
        self.candles_feed = CandlesFeed(config, self.ticker, exchange_provider, self.data_lock, self.new_data_event)
        self.level2_feed = Level2Feed(config,  exchange_provider, self.data_lock, self.new_data_event)
        self.bid_ask_feed = BidAskFeed(config,  exchange_provider, self.data_lock, self.new_data_event)

        # Learn params
        self.predict_window = config["pytrade2.strategy.predict.window"]
        self.past_window = config["pytrade2.strategy.past.window"]
        self.history_min_window = pd.Timedelta(config["pytrade2.strategy.history.min.window"])
        self.history_max_window = pd.Timedelta(config["pytrade2.strategy.history.max.window"])

        self.fut_low_high: pd.DataFrame = pd.DataFrame()

        logging.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

    def get_report(self):
        """ Short info for report """

        msg = StringIO()
        # Broker report
        if hasattr(self.broker, "get_report"):
            msg.write(self.broker.get_report())
        # BidAsk report
        msg.write(self.bid_ask_feed.get_report())
        msg.write("\n")
        # Level2 report
        msg.write(self.level2_feed.get_report())
        msg.write("\n")
        # Candles report
        msg.write(self.candles_feed.get_report())
        return msg.getvalue()

    def is_alive(self):
        maxdelta = self.history_min_window + pd.Timedelta("60s")
        dt = datetime.utcnow()

        if not self.bid_ask_feed.bid_ask.empty and dt - self.bid_ask_feed.bid_ask.index.max() > maxdelta:
            is_alive = False
        elif not self.level2_feed.level2.empty and dt - self.level2_feed.level2.index.max() > maxdelta:
            is_alive = False
        else:
            is_alive = self.candles_feed.is_alive()
        if not is_alive:
            last_bid_ask = self.bid_ask_feed.bid_ask.index.max() if not self.bid_ask_feed.bid_ask.empty else None
            last_level2 = self.level2_feed.level2.index.max() if not self.level2_feed.level2.empty else None
            last_candles = [(i, c.index.max()) for i, c in self.candles_feed.candles_by_interval.items()]
            logging.info(
                f"isNow: {dt}, maxdelta: {maxdelta}, last bid ask: {last_bid_ask}, last level2: {last_level2},"
                f"last candles: {last_candles}")
        return is_alive

    def process_new_data(self):
        self.apply_buffers()

        if not self.bid_ask_feed.bid_ask.empty and not self.level2_feed.level2.empty and self.model \
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
                self.data_persister.save_last_data(self.ticker, {
                    "x": X.tail(1),
                    "y_pred": y.tail(1)
                })
            except Exception as e:
                logging.error(f"{e}. Traceback: {traceback.format_exc()}")
            finally:
                self.is_processing = False

    def process_new_prediction(self) -> int:
        """ Process last prediction, open a new order, save history if needed
        @:return open signal where signal can be 0,-1,1 """

        bid = self.bid_ask_feed.bid_ask.loc[self.bid_ask_feed.bid_ask.index[-1], "bid"]
        ask = self.bid_ask_feed.bid_ask.loc[self.bid_ask_feed.bid_ask.index[-1], "ask"]

        # Update current trade status
        self.check_cur_trade()

        bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut = self.fut_low_high.loc[self.fut_low_high.index[-1], \
            ["bid_min_fut", "bid_max_fut",
             "ask_min_fut", "ask_max_fut"]]
        open_signal = 0
        if not self.broker.cur_trade and self.risk_manager.can_trade():
            # Maybe open a new order
            open_signal, open_price, stop_loss, take_profit, tr_delta = self.get_signal(bid, ask, bid_min_fut,
                                                                                        bid_max_fut,
                                                                                        ask_min_fut,
                                                                                        ask_max_fut)
            if open_signal:
                self.broker.create_cur_trade(symbol=self.ticker, direction=open_signal, quantity=self.order_quantity,
                                             price=open_price,
                                             stop_loss_price=stop_loss,
                                             take_profit_price=take_profit,
                                             trailing_delta=tr_delta)
        return open_signal

    def get_signal(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float, ask_min_fut: float,
                   ask_max_fut: float) -> (int, float, float, float):
        """ Calculate buy, sell or nothing signal based on predictions and profit/loss ratio
        :return (<-1 for sell, 0 for none, 1 for buy>, stop loss, take profit, trailing delta)"""

        buy_profit = bid_max_fut - ask
        buy_loss = ask - bid_min_fut
        sell_profit = bid - ask_min_fut
        sell_loss = ask_max_fut - bid

        # Buy signal
        # Not zeroes and ratio is ok and max/min are ok
        is_buy_ratio = buy_profit > 0 and (buy_loss <= 0 or buy_profit / buy_loss >= self.profit_loss_ratio)
        # is_buy_loss = abs(buy_loss) < self.stop_loss_max_coeff * ask
        is_buy_loss = self.stop_loss_min_coeff * ask <= abs(buy_loss) < self.stop_loss_max_coeff * ask
        is_buy_profit = abs(buy_profit) >= self.profit_min_coeff * ask
        is_buy = is_buy_ratio and is_buy_loss and is_buy_profit

        # Sell signal
        # Not zeroes and ratio is ok and max/min are ok
        is_sell_ratio = sell_profit > 0 and (sell_loss <= 0 or sell_profit / sell_loss >= self.profit_loss_ratio)
        # is_sell_loss = abs(sell_loss) < self.stop_loss_max_coeff * bid
        is_sell_loss = self.stop_loss_min_coeff * bid <= abs(sell_loss) < self.stop_loss_max_coeff * bid
        is_sell_profit = abs(sell_profit) >= self.profit_min_coeff * bid
        is_sell = is_sell_ratio and is_sell_loss and is_sell_profit

        # This should not happen, but let's handle it and clear the flags
        if is_buy and is_sell:
            is_buy = is_sell = False

        if is_buy:
            # Buy and possibly fix the loss
            stop_loss_adj = ask - abs(buy_loss) * 1.25
            tr_delta = abs(stop_loss_adj)
            # stop_loss_adj = min(stop_loss_adj, round(ask * (1 - self.stop_loss_min_coeff), self.price_precision))
            return 1, ask, stop_loss_adj, ask + buy_profit, tr_delta
        elif is_sell:
            # Sell and possibly fix the loss
            stop_loss_adj = bid + abs(sell_loss) * 1.25
            tr_delta = abs(stop_loss_adj)
            # stop_loss_adj = max(stop_loss_adj, round(bid * (1 + self.stop_loss_min_coeff), self.price_precision))
            return -1, bid, stop_loss_adj, bid - sell_profit, tr_delta
        else:
            # No action
            return 0, None, None, None, None

    def predict_low_high(self) -> (pd.DataFrame, pd.DataFrame):
        # X - features with absolute values, x_prepared - nd array fith final scaling and normalization
        X, X_prepared = self.prepare_last_X()
        # Predict
        y = self.model.predict(X_prepared, verbose=0) if not X.empty and X_prepared.size > 0 else np.array(
            [np.nan, np.nan, np.nan, np.nan])
        y = y.reshape((-1, 4))

        # Get prediction result
        y = self.y_pipe.inverse_transform(y)
        (bid_max_fut_diff, bid_spread_fut, ask_min_fut_diff, ask_spread_fut) = y[-1]  # if y.shape[0] < 2 else y
        y_df = self.bid_ask_feed.bid_ask.loc[X.index][["bid", "ask"]]
        y_df["bid_max_fut"] = y_df["bid"] + bid_max_fut_diff
        y_df["bid_min_fut"] = y_df["bid_max_fut"] - bid_spread_fut
        y_df["ask_min_fut"] = y_df["ask"] + ask_min_fut_diff
        y_df["ask_max_fut"] = y_df["ask_min_fut"] + ask_spread_fut
        return X, y_df

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""

        no_bidask = self.bid_ask_feed.bid_ask.empty
        no_candles = not self.candles_feed.has_all_candles()
        no_level2 = self.level2_feed.level2.empty and self.level2_feed.level2_buf.empty

        if no_bidask or no_candles or no_level2:
            logging.info(f"Can not learn because some datasets are empty. "
                         f"level2.empty: {no_level2}, "
                         f"candles.empty: {no_candles}")
            return False

        # Check If we have enough data to learn
        interval = self.bid_ask_feed.bid_ask.index.max() - self.bid_ask_feed.bid_ask.index.min()
        if interval < self.history_min_window:
            logging.info(
                f"Can not learn because not enough history. We have {interval}, but we need {self.history_min_window}")
            return False
        return True

    def prepare_last_X(self) -> (pd.DataFrame, ndarray):
        """ Get last X for prediction"""
        X = PredictBidAskFeatures.last_features_of(self.bid_ask_feed.bid_ask,
                                                   1,  # For diff
                                                   self.level2_feed.level2,
                                                   self.candles_feed.candles_by_interval,
                                                   self.candles_feed.candles_cnt_by_interval,
                                                   past_window=self.past_window)
        return X, (self.X_pipe.transform(X) if X.shape[0] else pd.DataFrame())

    def prepare_Xy(self) -> (pd.DataFrame, pd.DataFrame):
        """ Prepare train data """
        with self.data_lock:
            # Copy data for this thread only
            bid_ask = self.bid_ask_feed.bid_ask.copy()
            level2 = self.level2_feed.level2.copy()

        return PredictBidAskFeatures.features_targets_of(
            bid_ask,
            level2,
            self.candles_feed.candles_by_interval,
            self.candles_feed.candles_cnt_by_interval,
            self.predict_window,
            self.past_window)

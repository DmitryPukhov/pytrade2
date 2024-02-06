import logging
from typing import Dict

import pandas as pd
from numpy import ndarray

from exch.Exchange import Exchange
from strategy.common.StrategyBase import StrategyBase
from strategy.features.PredictBidAskFeatures import PredictBidAskFeatures


class PredictBidAskStrategyBase(StrategyBase):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None

        StrategyBase.__init__(self, config, exchange_provider, True, True, True)

        # Learn params
        self.predict_window = config["pytrade2.strategy.predict.window"]
        self.past_window = config["pytrade2.strategy.past.window"]

        self.fut_low_high: pd.DataFrame = pd.DataFrame()

        logging.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

    def process_prediction(self, y_pred) -> int:
        """ Process last prediction, open a new order, save history if needed
        @:return open signal where signal can be 0,-1,1 """

        bid = self.bid_ask_feed.bid_ask.loc[self.bid_ask_feed.bid_ask.index[-1], "bid"]
        ask = self.bid_ask_feed.bid_ask.loc[self.bid_ask_feed.bid_ask.index[-1], "ask"]
        bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut = y_pred.loc[y_pred.index[-1],
        ["bid_min_fut", "bid_max_fut",
         "ask_min_fut", "ask_max_fut"]]

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

    def predict(self, x) -> pd.DataFrame:
        # X - features with absolute values, x_prepared - nd array fith final scaling and normalization
        x_trans = self.X_pipe.transform(x)

        # Predict
        y = self.model.predict(x_trans, verbose=0)
        y = y.reshape((-1, 4))

        # Get prediction result
        y = self.y_pipe.inverse_transform(y)
        (bid_max_fut_diff, bid_spread_fut, ask_min_fut_diff, ask_spread_fut) = y[-1]  # if y.shape[0] < 2 else y
        y_df = self.bid_ask_feed.bid_ask.loc[x.index][["bid", "ask"]]
        y_df["bid_max_fut"] = y_df["bid"] + bid_max_fut_diff
        y_df["bid_min_fut"] = y_df["bid_max_fut"] - bid_spread_fut
        y_df["ask_min_fut"] = y_df["ask"] + ask_min_fut_diff
        y_df["ask_max_fut"] = y_df["ask_min_fut"] + ask_spread_fut
        return y_df

    def prepare_last_x(self) -> (pd.DataFrame, ndarray):
        """ Get last X for prediction"""
        return PredictBidAskFeatures.last_features_of(self.bid_ask_feed.bid_ask,
                                                      1,  # For diff
                                                      self.level2_feed.level2,
                                                      self.candles_feed.candles_by_interval,
                                                      self.candles_feed.candles_cnt_by_interval,
                                                      past_window=self.past_window)

    def prepare_xy(self) -> (pd.DataFrame, pd.DataFrame):
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

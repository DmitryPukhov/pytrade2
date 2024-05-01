import logging
from typing import Dict
import pandas as pd
from numpy import ndarray
from exch.Exchange import Exchange
from strategy.common.StrategyBase import StrategyBase
from strategy.features.PredictBidAskFeatures import PredictBidAskFeatures
from strategy.signal.SignalByFutBidAsk import SignalByFutBidAsk


class BidAskRegressionStrategyBase(StrategyBase):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):
        self.websocket_feed = None

        StrategyBase.__init__(self, config, exchange_provider, True, True, True)
        self.signal_calc = SignalByFutBidAsk(self.profit_loss_ratio, self.stop_loss_min_coeff, self.stop_loss_max_coeff,
                                             self.take_profit_min_coeff, self.take_profit_max_coeff, self.price_precision)
        # Learn params
        self.predict_window = config["pytrade2.strategy.predict.window"]
        self.past_window = config["pytrade2.strategy.past.window"]

        self.fut_low_high: pd.DataFrame = pd.DataFrame()

        self._logger.info("Strategy parameters:\n" + "\n".join(
            [f"{key}: {value}" for key, value in self.config.items() if key.startswith("pytrade2.strategy.")]))

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

    def process_prediction(self, y_pred) -> int:
        """ Process last prediction, open a new order, save history if needed
        @:return open signal where signal can be 0,-1,1 """

        bid = self.bid_ask_feed.bid_ask.loc[self.bid_ask_feed.bid_ask.index[-1], "bid"]
        ask = self.bid_ask_feed.bid_ask.loc[self.bid_ask_feed.bid_ask.index[-1], "ask"]
        bid_min_fut, bid_max_fut, ask_min_fut, ask_max_fut = y_pred.loc[
            y_pred.index[-1],
            ["bid_min_fut", "bid_max_fut", "ask_min_fut", "ask_max_fut"]]

        # Maybe open a new order
        open_signal, open_price, stop_loss, take_profit, tr_delta = self.signal_calc.get_signal_sl_tp_trdelta(bid, ask,
                                                                                                              bid_min_fut,
                                                                                                              bid_max_fut,
                                                                                                              ask_min_fut,
                                                                                                              ask_max_fut)
        if open_signal:
            self.broker.create_cur_trade(symbol=self.ticker,
                                         direction=open_signal,
                                         quantity=self.order_quantity,
                                         price=open_price,
                                         stop_loss_price=stop_loss,
                                         take_profit_price=take_profit,
                                         trailing_delta=tr_delta)
        return open_signal

from datetime import datetime

import pandas as pd

from pytrade2.exch.Exchange import Exchange
from pytrade2.feed.KafkaFeaturesFeed import KafkaFeaturesFeed
from pytrade2.feed.LastCandle1MinFeed import LastCandle1MinFeed
from pytrade2.metrics.MetricServer import MetricServer
from pytrade2.strategy.common.StrategyBase import StrategyBase


class SignalClassificationFeaturesStrategy(StrategyBase):
    """
    Model generates buy, sel or oom signal. Strategy is based on this signal.
    Precalculated features are red from kafka.
   """

    def __init__(self, config: dict[str, str], exchange_provider: Exchange):
        # self.websocket_feed = None
        StrategyBase.__init__(self, config=config,
                              exchange_provider=exchange_provider,
                              is_candles_feed=False,
                              is_bid_ask_feed=False,
                              is_level2_feed=False)
        self._features_feed = KafkaFeaturesFeed(config=config, data_lock=self.data_lock, new_data_event=self.new_data_event)
        self.last_candle_feed = LastCandle1MinFeed(config=config, ticker = self.ticker, exchange_provider=exchange_provider, data_lock=self.data_lock, new_data_event=None)
        self._feeds.extend([self.last_candle_feed, self._features_feed])
        self.target_period = config["pytrade2.strategy.predict.window"]
        self.is_learn_enabled = False # no learn, getting features from kafka
        self.is_learned = True
        self.model_name = "SignalClassifierLgb"

        # Parameters to update from model
        self.stop_loss_coeff = float(config["pytrade2.strategy.stoploss.coeff"])
        self.profit_loss_ratio = float(config["pytrade2.strategy.profit.loss.ratio"])

        self._logger.info(f"Target period: {self.target_period}")

    def can_learn(self) -> bool:
        """ Don't learn, only use the model from mlflow"""
        return False
    #
    # def apply_buffers(self):
    #     self._features_feed.apply_buf()

    def prepare_last_x(self) -> pd.DataFrame:
        self._logger.debug(f"Got {len(self._features_feed.data)} features")
        return self._features_feed.data

    def prepare_xy(self):
        return pd.DataFrame(), pd.DataFrame()

    def predict(self, x) -> pd.DataFrame:
        # Save to buffer, actual persist by schedule of data persister
        if not self.is_learned:
            self._logger.debug("Cannot predict, strategy is not learned yet")
        self._logger.debug(f"Predicting signal")
        self.data_persister.add_to_buf(self.ticker, {'x': x})
        with self.data_lock:
            x_trans = self.X_pipe.transform(x)
            y_arr = self.model.predict(x_trans)
            self.y_pipe.is_fitted = False
            y_arr = self.y_pipe.inverse_transform(y_arr)

            y_arr = y_arr.reshape((-1, 1))[-1]  # Last and only row
            signal = y_arr[0]
            self._logger.debug(f"Predicted signal: {signal}")
            y_df = pd.DataFrame(data={'signal': signal},
                                index=x.index[-1:])
            return y_df

    def process_prediction(self, signal_df: pd.DataFrame):
        if signal_df.empty:
            self._logger.debug(f"Cannot process empty prediction")
        signal = signal_df['signal'].values[-1]

        # Metrics
        MetricServer.metrics.strategy.signal.signal.set(signal)


        if signal != 0:
            if self.last_candle_feed.last_candle is None:
                self._logger.info(f"Signal {signal}, but last candle is absent, cannot calculate last price")
                return

            # Calc last price, we expect new trade to be opened at this if signal is 1 or -1
            price = self.last_candle_feed.last_candle["close"]
            stop_loss_price = price + price * self.stop_loss_coeff * signal
            take_profit_price = price + price * self.stop_loss_coeff * self.profit_loss_ratio

            MetricServer.metrics.strategy.signal.signal_price.set(price)
            MetricServer.metrics.strategy.signal.signal_sl.set(stop_loss_price)
            MetricServer.metrics.strategy.signal.signal_tp.set(take_profit_price)

            signal_ext = {"datetime": datetime.now(), "signal": signal, "price": price, "sl": stop_loss_price,
                          "tp": take_profit_price}
            cur_trade_ok = self.broker.cur_trade is None
            risk_manager_ok = self.risk_manager.can_trade()
            # Trade
            if signal and cur_trade_ok and risk_manager_ok:
                self.broker.create_cur_trade(symbol=self.ticker,
                                             direction=signal,
                                             quantity=self.order_quantity,
                                             price=price,
                                             stop_loss_price=stop_loss_price,
                                             take_profit_price=take_profit_price)
                if self.broker.cur_trade:
                    signal_ext['open_price'] = self.broker.cur_trade.open_price
                    signal_ext['status'] = 'order_created'
                else:
                    signal_ext['open_price'] = None
                    signal_ext['status'] = 'order_not_created'
            elif not cur_trade_ok:
                signal_ext["status"] = "already_in_market"
            elif not risk_manager_ok:
                signal_ext["status"] = "risk_manager_deny"

            self._logger.debug(f"Processed prediction info: {signal_ext}")

            # Persist the data for later analysis
            signal_ext_df = pd.DataFrame(data=[signal_ext]).set_index('datetime')
            self.data_persister.save_last_data(self.ticker, {'signal_ext': signal_ext_df})


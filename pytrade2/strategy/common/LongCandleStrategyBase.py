import logging
import multiprocessing
from io import StringIO
from threading import Event
from typing import Dict

import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, OneHotEncoder

from exch.Exchange import Exchange
from strategy.common.CandlesStrategy import CandlesStrategy
from strategy.common.StrategyBase import StrategyBase
from strategy.common.features.CandlesFeatures import CandlesFeatures


class LongCandleStrategyBase(StrategyBase, CandlesStrategy):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None
        self.candles_feed = None

        StrategyBase.__init__(self, config, exchange_provider)
        CandlesStrategy.__init__(self, config=config, ticker=self.ticker, candles_feed=self.candles_feed)
        self.target_period = min(self.candles_cnt_by_interval.keys())
        logging.info(f"Target period: {self.target_period}")
        self.data_lock = multiprocessing.RLock()
        self.new_data_event: Event = Event()

    def get_report(self):
        """ Short info for report """

        msg = StringIO()
        # Broker report
        if hasattr(self.broker, "get_report"):
            msg.write(self.broker.get_report())

        # Candles report
        msg.write(CandlesStrategy.get_report(self))

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

        self.read_candles()

        StrategyBase.run(self)

        # Run the feed, listen events
        self.candles_feed.run()
        self.broker.run()

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""

        if not self.has_all_candles():
            logging.info(f"Can not learn because not enough candles.")
            return False

        # Check If we have enough data to learn
        return True

    def process_new_data(self):
        if self.model:
            with self.data_lock:
                # Get features
                x = CandlesFeatures.candles_last_combined_features_of(self.candles_by_interval,
                                                                      self.candles_cnt_by_interval)
                x_trans = self.X_pipe.transform(x)

                # Get last signal
                y = self.model.predict(x_trans)
                y_trans = self.y_pipe.inverse_transform(y)
                signal = y_trans[-1][0] if y_trans else 0
                y_df = pd.DataFrame(data=[{"signal": signal}], index=[x.index[-1]])

                # Buy or sell or skip
                self.process_signal(signal)

                # Save to historical data
                self.save_last_data(self.ticker, {"x": x.tail(1), "y_pred": y_df.tail(1)})

    def get_sl_tp_trdelta(self, signal: int) -> (float, float, float):
        """
        Stop loss is low of last candle
        :return stop loss, take profit, trailing delta
        """

        last_candle = self.candles_by_interval[self.target_period].iloc[-1]
        td = last_candle["high"] - last_candle["low"]
        if signal == 1:
            sl, tp = last_candle["low"], last_candle["high"]
        elif signal == -1:
            sl, tp = last_candle["high"], last_candle["low"]
        else:
            # Should never come here
            return None
        return sl, tp, td

    def process_signal(self, signal: int):
        if not signal:
            return
        sl, tp, tdelta = self.get_sl_tp_trdelta(signal)
        self.broker.create_cur_trade(symbol=self.ticker,
                                     direction=signal,
                                     quantity=self.order_quantity,
                                     price=None,
                                     stop_loss_price=sl,
                                     take_profit_price=tp,
                                     trailing_delta=tdelta)

    def is_alive(self):
        return CandlesStrategy.is_alive(self)

    def prepare_Xy(self) -> (pd.DataFrame, pd.DataFrame):
        # Get features and targets
        x, y = CandlesFeatures.features_targets_of(self.candles_by_interval,
                                                   self.candles_cnt_by_interval,
                                                   target_period=self.target_period)
        # Balance by signal
        self.learn_data_balancer.add(x, y)
        balanced_x, balanced_y = self.learn_data_balancer.get_balanced_xy()

        # Log each signal count
        msgs = ["Prepared balanced xy for learning."]
        for signal in [-1, 0, 1]:
            cnt = balanced_y[balanced_y['signal'] == signal].size
            msgs.append(f"signal{signal}:{cnt}")
        logging.info(' '.join(msgs))

        return balanced_x, balanced_y

    def generator_of(self, train_X, train_y):
        """ Data generator for learning """

        return TimeseriesGenerator(train_X, train_y.data, length=1)

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """

        time_cols = [col for col in X.columns if col.startswith("time")]
        float_cols = list(set(X.columns) - set(time_cols))

        # Scale x
        x_pipe = Pipeline(
            [("xscaler", ColumnTransformer([("xrs", RobustScaler(), float_cols)], remainder="passthrough")),
             ("xmms", MinMaxScaler())])
        x_pipe.fit(X)

        # One hot encode y
        y_pipe = Pipeline([('adjust_labels', OneHotEncoder(categories=[[-1, 0, 1]], sparse=True, drop=None))])
        return x_pipe, y_pipe

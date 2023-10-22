import logging
import multiprocessing
import time
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
from strategy.common.features.LongCandleFeatures import LongCandleFeatures


class LongCandleStrategyBase(StrategyBase, CandlesStrategy):
    """
    Predict long candle, classification model, target signals: -1, 0, 1
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None
        self.candles_feed = None

        StrategyBase.__init__(self, config, exchange_provider)
        CandlesStrategy.__init__(self, config=config, ticker=self.ticker, candles_feed=self.candles_feed)

        self.target_period = min(self.candles_cnt_by_interval.keys())
        # Should keep 2 more candles for targets
        self.candles_cnt_by_interval[self.target_period] += 2
        self.candles_history_cnt_by_interval[self.target_period] += 2
        self.processing_interval = pd.Timedelta(config.get('pytrade2.strategy.processing.interval', '1 minute'))

        logging.info(f"Target period: {self.target_period}")
        self.data_lock = multiprocessing.RLock()
        self.new_data_event = None  # No new data by event

        # x, y buffer to validate later when target can be calculated
        self.x_unchecked = pd.DataFrame()
        self.y_unchecked = pd.DataFrame()

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
        # Do not listen feeds
        self.websocket_feed.consumers.add(self)
        self.candles_feed = self.exchange_provider.candles_feed(exchange_name)
        # Do not listen feeds
        # self.candles_feed.consumers.add(self)

        self.broker = self.exchange_provider.broker(exchange_name)

        # self.read_candles()

        StrategyBase.run(self)

        # Run the feed, listen events
        # Do not listen candles
        # self.candles_feed.run()
        self.broker.run()

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""

        if not self.has_all_candles():
            logging.info(f"Can not learn because not enough candles.")
            return False
        else:
            return True

    def process_new_data(self):
        with self.data_lock:
            # Get candles starting from current moment to the past
            with self.data_lock:
                self.read_candles(index_col='close_time')

            x, y, x_wo_targets = LongCandleFeatures.features_targets_of(self.candles_by_interval,
                                                                        self.candles_cnt_by_interval,
                                                                        self.target_period,
                                                                        with_empty_targets=True)

            # We could calculate targets for x, so add x and targets to learn data
            self.learn_data_balancer.add(x, y)
            if not self.model:
                self.model = self.create_model(len(x.columns), 3)  # y_size - one hot encoded signals: -1,0.1
            if not (self.X_pipe and self.y_pipe):
                self.X_pipe, self.y_pipe = self.create_pipe(x, y)

            # Predict last signal
            x_trans = self.X_pipe.transform(x_wo_targets)
            y_pred_raw = self.model.predict(x_trans, verbose=0)
            y_pred_trans = self.y_pipe.inverse_transform(y_pred_raw)
            last_signal = y_pred_trans[-1][0] if y_pred_trans.size > 0 else 0
            # y_pred_raw = self.model.predict(x_trans, verbose=0)
            # last_signal = y_pred_raw[-1][0] if y_pred_raw else 0
            y_pred_df = pd.DataFrame(data=[{"signal": last_signal}], index=[x_wo_targets.index[-1]])

            # Buy or sell or skip
            self.process_signal(last_signal)

            # Save to disk for analysis
            self.save_last_data(self.ticker, {'y_pred': y_pred_df})

        # Delay before next processing cycle
        time.sleep(self.processing_interval.seconds)

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
        with self.data_lock:
            balanced_x, balanced_y = self.learn_data_balancer.get_balanced_xy()
        # Log each signal count
        msgs = ["Prepared balanced xy for learning."]
        for signal in [-1, 0, 1]:
            cnt = balanced_y[balanced_y['signal'] == signal].size if not balanced_y.empty else 0
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
        y_pipe = Pipeline([('adjust_labels', OneHotEncoder(categories=[[-1, 0, 1]], sparse_output=True, drop=None))])
        y_pipe.fit(y)
        return x_pipe, y_pipe

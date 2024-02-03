import logging
import multiprocessing
import time
from io import StringIO
from typing import Dict
import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, OneHotEncoder
from exch.Exchange import Exchange
from strategy.feed.CandlesFeed import CandlesFeed
from strategy.feed.Level2Feed import Level2Feed
from strategy.common.StrategyBase import StrategyBase
from strategy.features.LongCandleFeatures import LongCandleFeatures
from threading import Event


class LongCandleStrategyBase(StrategyBase):
    """
    Predict long candle, classification model, target signals: -1, 0, 1
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None

        StrategyBase.__init__(self, config, exchange_provider)
        self.candles_feed = CandlesFeed(config, self.ticker, exchange_provider, self.data_lock, self.new_data_event)
        # Should keep 1 more candle for targets
        predict_window = config["pytrade2.strategy.predict.window"]
        self.target_period = predict_window
        self.candles_feed.candles_cnt_by_interval[self.target_period] += 1
        self.candles_feed.candles_history_cnt_by_interval[self.target_period] += 1

        self.level2_feed = Level2Feed(config, exchange_provider, self.data_lock, self.new_data_event)
        # CandlesFeed.__init__(self, config=config, ticker=self.ticker, exchange_provider=exchange_provider)
        #Level2Feed.__init__(self, config)

        self.processing_interval = pd.Timedelta(config.get('pytrade2.strategy.processing.interval', '30 seconds'))

        logging.info(f"Target period: {self.target_period}")

    def get_report(self):
        """ Short info for report """

        msg = StringIO()
        # Broker report
        if hasattr(self.broker, "get_report"):
            msg.write(self.broker.get_report())

        # msg.write("\n")
        # msg.write(Level2Strategy.get_report(self))
        msg.write("\n")
        msg.write(self.candles_feed.get_report())

        msg.write(self.learn_data_balancer.get_report())

        return msg.getvalue()

    def run(self):
        """
        Attach to the feed and listen
        """
        exchange_name = self.config["pytrade2.exchange"]

        # Create feeds and broker
        self.websocket_feed = self.exchange_provider.websocket_feed(exchange_name)
        #self.websocket_feed.consumers.add(self)

        self.candles_feed = self.exchange_provider.candles_feed(exchange_name)
        self.candles_feed.consumers.add(self)

        self.broker = self.exchange_provider.broker(exchange_name)

        self.candles_feed.read_candles()

        StrategyBase.run(self)

        # Run the feed, listen events
        # Run the feed, listen events
        #self.websocket_feed.run()
        self.candles_feed.run()
        self.broker.run()

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""
        if self.candles_feed.has_all_candles():
            return True
        else:
            logging.info("Cannot learn, not enough data")
        # no_candles = not self.has_all_candles()
        # no_level2 = self.level2.empty
        #
        # if no_candles or no_level2:
        #     logging.info(f"Can not learn because some datasets are empty. "
        #                  f"level2.empty: {no_level2}, "
        #                  f"candles.empty: {no_candles}")
        #     return False
        # return True

    def predict_last_signal(self, x):
        x_trans = self.X_pipe.transform(x)
        y_pred_raw = self.model.predict(x_trans, verbose=0)
        y_pred_trans = self.y_pipe.inverse_transform(y_pred_raw)
        last_signal = y_pred_trans[-1][0] if y_pred_trans.size > 0 else 0
        return pd.DataFrame(data=[{"signal": last_signal}], index=x.tail(1).index)

    def features_targets(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        #level2_past__window = self.target_period
        x, y, x_wo_targets = LongCandleFeatures.features_targets_of(self.candles_feed.candles_by_interval,
                                                                    self.candles_feed.candles_cnt_by_interval,
                                                                    #self.level2,
                                                                    #level2_past__window,
                                                                    self.target_period,
                                                                    self.stop_loss_min_coeff,
                                                                    self.profit_min_coeff)
        return x, y, x_wo_targets

    def apply_buffers(self):
        # Append new data from buffers to main data frames
        with (self.data_lock):
            # Save raw buffers to history
            save_dict = {f"raw_candles_{period}": buf for period, buf in self.candles_feed.candles_by_interval_buf.items()}
            self.data_persister.save_last_data(self.ticker, save_dict)
            self.candles_feed.update_candles()

    def process_new_data(self):
        self.apply_buffers()
        #
        # with (self.data_lock):
        #     # Save raw candles to history
        #     save_dict = {f"raw_candles_{period}": buf for period, buf in self.candles_feed.candles_by_interval_buf.items()}
        #     self.data_persister.save_last_data(self.ticker, save_dict)
        #     self.candles_feed.update_candles()

        x, y, x_wo_targets = self.features_targets()

        if x.empty or y.empty or x_wo_targets.empty:
            logging.info(
                f'Cannot process new data: features or targets are empty. Waiting {self.target_period} for next attempt')
            # Wait until level2 data accumulated
            #time.sleep(pd.Timedelta(self.target_period).seconds)
            return

        # We could calculate targets for x, so add x and targets to learn data
        self.learn_data_balancer.add(x, y)
        if not self.model:
            self.model = self.create_model(len(x.columns), 3)  # y_size - one hot encoded signals: -1,0.1
        if not (self.X_pipe and self.y_pipe):
            self.X_pipe, self.y_pipe = self.create_pipe(x, y)

        # Predict last signal
        y_pred_last = self.predict_last_signal(x_wo_targets)
        last_signal = y_pred_last['signal'].iloc[-1]

        # Buy or sell or skip
        self.process_signal(last_signal)

        # Save to disk for analysis
        self.data_persister.save_last_data(self.ticker, {'y_pred': y_pred_last})

        # Predict last signal for old x with y. To save and analyse actual and predicted values.
        y_pred = self.predict_last_signal(x).join(y.tail(1), lsuffix='_pred', rsuffix='_actual')
        self.data_persister.save_last_data(self.ticker, {'x': x.tail(1), 'y': y_pred})

        # Delay before next processing cycle
        time.sleep(self.processing_interval.seconds)

    def get_sl_tp_trdelta(self, signal: int) -> (float, float, float):
        """
        Stop loss is low of last candle
        :return stop loss, take profit, trailing delta
        """

        last_candle = self.candles_feed.candles_by_interval[self.target_period].iloc[-1]
        sl_delta_min = self.stop_loss_min_coeff * last_candle["close"]
        sl_delta_max = self.stop_loss_max_coeff * last_candle["close"]
        tp_delta_min = self.profit_min_coeff * last_candle["close"]
        tp_delta_max = self.profit_max_coeff * last_candle["close"]
        tr_delta_min, tr_delta_max = sl_delta_min, sl_delta_max
        open_price = last_candle["close"]  # Order open price

        tr_delta = max(abs(last_candle["high"] - last_candle["low"]), tr_delta_min)  # trailing delta
        tr_delta = min(tr_delta, tr_delta_max)
        if signal == 1:
            sl = min(last_candle["low"], open_price - sl_delta_min)
            sl = max(sl, open_price - sl_delta_max)
            tp = max(last_candle["high"], open_price + tp_delta_min)
            tp = min(tp, open_price + tp_delta_max)
        elif signal == -1:
            sl = max(last_candle["high"], open_price + sl_delta_min)
            sl = min(sl, open_price + sl_delta_max)
            tp = min(last_candle["low"], open_price - tp_delta_min)
            tp = max(tp, open_price - tp_delta_max)
        else:
            # Should never come here
            return None

        return sl, tp, tr_delta

    def process_signal(self, signal: int):
        if not signal:
            return
        sl, tp, tdelta = self.get_sl_tp_trdelta(signal)
        # last_candle = self.candles_by_interval[self.target_period].iloc[-1]
        # price = last_candle["close"]
        self.broker.create_cur_trade(symbol=self.ticker,
                                     direction=signal,
                                     quantity=self.order_quantity,
                                     price=None,
                                     stop_loss_price=sl,
                                     take_profit_price=tp,
                                     trailing_delta=tdelta)

    def is_alive(self):
        return CandlesFeed.is_alive(self)

    def prepare_Xy(self) -> (pd.DataFrame, pd.DataFrame):
        # with self.data_lock:
        #     self.update_level2()
        #     balanced_x, balanced_y = self.learn_data_balancer.pop_balanced_xy()
        balanced_x, balanced_y = self.learn_data_balancer.pop_balanced_xy()

        # Log each signal count
        msgs_bal = ["Prepared balanced xy for learning. Balanced counts"]
        msgs_unbal = ["Unbalanced left counts"]
        for signal in [-1, 0, 1]:
            cnt_bal = len(balanced_y[balanced_y['signal'] == signal]) if not balanced_y.empty else 0
            msgs_bal.append(f"signal{signal}:{cnt_bal}")

            cnt_unbal = len(self.learn_data_balancer.y_dict[signal])
            msgs_unbal.append(f"signal{signal}:{cnt_unbal}")

        logging.info(' '.join(msgs_bal))
        logging.info(' '.join(msgs_unbal))

        return balanced_x, balanced_y

    def generator_of(self, train_X, train_y):
        """ Data generator for learning """
        return TimeseriesGenerator(train_X, train_y, length=1)

    def create_pipe(self, X, y) -> (Pipeline, Pipeline):
        """ Create feature and target pipelines to use for transform and inverse transform """

        time_cols = [col for col in X.columns if col.startswith("time") or col.endswith("time")]
        float_cols = list(set(X.columns) - set(time_cols))

        # Scale x
        x_pipe = Pipeline(
            [("xscaler", ColumnTransformer([("xrs", RobustScaler(), float_cols)], remainder="passthrough")),
             ("xmms", MinMaxScaler())])
        x_pipe.fit(X)

        # One hot encode y
        y_pipe = Pipeline([('adjust_labels', OneHotEncoder(categories=[[-1, 0, 1]], sparse_output=False, drop=None))])
        y_pipe.fit(y)
        return x_pipe, y_pipe

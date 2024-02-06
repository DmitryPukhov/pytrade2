import logging
import time
from io import StringIO
from typing import Dict
import pandas as pd
from keras.preprocessing.sequence import TimeseriesGenerator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, OneHotEncoder
from exch.Exchange import Exchange
from strategy.common.LearnDataBalancer import LearnDataBalancer
from strategy.common.StrategyBase import StrategyBase
from strategy.features.LongCandleFeatures import LongCandleFeatures
from strategy.signal.OrderParamsByLastCandle import OrderParamsByLastCandle


class LongCandleStrategyBase(StrategyBase):
    """
    Predict long candle signal, classification model, target signals: -1, 0, 1
    """

    def __init__(self, config: Dict, exchange_provider: Exchange):

        self.websocket_feed = None

        StrategyBase.__init__(self, config=config,
                              exchange_provider=exchange_provider,
                              is_candles_feed=True,
                              is_bid_ask_feed=False,
                              is_level2_feed=True)
        self.signal_calc = OrderParamsByLastCandle(self.profit_loss_ratio, self.stop_loss_min_coeff, self.stop_loss_max_coeff, self.profit_min_coeff, self.profit_max_coeff)
        # Should keep 1 more candle for targets
        predict_window = config["pytrade2.strategy.predict.window"]
        self.target_period = predict_window
        self.candles_feed.candles_cnt_by_interval[self.target_period] += 1
        self.candles_feed.candles_history_cnt_by_interval[self.target_period] += 1

        self.processing_interval = pd.Timedelta(config.get('pytrade2.strategy.processing.interval', '30 seconds'))

        logging.info(f"Target period: {self.target_period}")

    def get_report(self):
        msg = StringIO()
        msg.write(super().get_report())
        return msg.getvalue()

    def predict(self, x):
        x_trans = self.X_pipe.transform(x)
        y_pred_raw = self.model.predict(x_trans, verbose=0)
        y_pred_trans = self.y_pipe.inverse_transform(y_pred_raw)
        last_signal = y_pred_trans[-1][0] if y_pred_trans.size > 0 else 0
        return pd.DataFrame(data=[{"signal": last_signal}], index=x.tail(1).index)

    def prepare_last_x(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        return LongCandleFeatures.features_of(self.candles_feed.candles_by_interval,
                                              self.candles_feed.candles_cnt_by_interval)

    def process_new_data(self):
        super().process_new_data()

        # Delay before next processing cycle
        time.sleep(self.processing_interval.seconds)

    def process_prediction(self, y_pred: pd.DataFrame):
        signal = y_pred['signal'].iloc[-1]

        if not signal: # signal = 0 => oom
            return
        last_candle = self.candles_feed.candles_by_interval[self.target_period].iloc[-1]
        sl, tp, tdelta = self.signal_calc.get_sl_tp_trdelta(signal, last_candle)
        self.broker.create_cur_trade(symbol=self.ticker,
                                     direction=signal,
                                     quantity=self.order_quantity,
                                     price=None,
                                     stop_loss_price=sl,
                                     take_profit_price=tp,
                                     trailing_delta=tdelta)

    def prepare_xy(self) -> (pd.DataFrame, pd.DataFrame):
        x, y = LongCandleFeatures.features_targets_of(
            self.candles_feed.candles_by_interval,
            self.candles_feed.candles_cnt_by_interval,
            self.target_period,
            self.stop_loss_min_coeff,
            self.profit_min_coeff)
        # Balance by signal
        return LearnDataBalancer.balanced(x, y)

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

import logging
from typing import Dict

import pandas as pd
from keras import Sequential, Input
from keras.layers import Dense, Dropout
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, MinMaxScaler, OneHotEncoder

from exch.Exchange import Exchange
from strategy.common.LearnDataBalancer import LearnDataBalancer
from strategy.common.SignalStrategyBase import SignalStrategyBase
from strategy.common.StrategyBase import StrategyBase
from strategy.features.LongCandleFeatures import LongCandleFeatures
from strategy.signal.OrderParamsByLastCandle import OrderParamsByLastCandle


class SignalSignalStrategy(SignalStrategyBase):
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
        self.signal_calc = OrderParamsByLastCandle(self.profit_loss_ratio, self.stop_loss_min_coeff,
                                                   self.stop_loss_max_coeff, self.profit_min_coeff,
                                                   self.profit_max_coeff)
        # Should keep 1 more candle for targets
        predict_window = config["pytrade2.strategy.predict.window"]
        self.target_period = predict_window
        self.candles_feed.candles_cnt_by_interval[self.target_period] += 1
        self.candles_feed.candles_history_cnt_by_interval[self.target_period] += 1

        logging.info(f"Target period: {self.target_period}")

    def prepare_xy(self) -> (pd.DataFrame, pd.DataFrame):
        x, y = LongCandleFeatures.features_targets_of(
            self.candles_feed.candles_by_interval,
            self.candles_feed.candles_cnt_by_interval,
            self.target_period,
            self.stop_loss_min_coeff,
            self.profit_min_coeff)
        # Balance by signal
        return LearnDataBalancer.balanced(x, y)

    def prepare_last_x(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        return LongCandleFeatures.features_of(self.candles_feed.candles_by_interval,
                                              self.candles_feed.candles_cnt_by_interval)

    def predict(self, x):
        x_trans = self.X_pipe.transform(x)
        y_pred_raw = self.model.predict(x_trans, verbose=0)
        y_pred_trans = self.y_pipe.inverse_transform(y_pred_raw)
        last_signal = y_pred_trans[-1][0] if y_pred_trans.size > 0 else 0
        return pd.DataFrame(data=[{"signal": last_signal}], index=x.tail(1).index)

    def process_prediction(self, y_pred: pd.DataFrame):
        signal = y_pred['signal'].iloc[-1]

        if not signal:  # signal = 0 => oom
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

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(Input(shape=(X_size,)))
        model.add(Dense(64, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(512, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(128, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(32, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(y_size, activation='softmax'))
        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['categorical_accuracy'])

        # Load weights
        self.model_persister.load_last_model(model)
        model.summary()
        return model


from typing import Dict

import lightgbm as lgb
import pandas as pd
from sklearn.multioutput import MultiOutputRegressor

from metrics.MetricServer import MetricServer
from exch.Exchange import Exchange
from strategy.common.StrategyBase import StrategyBase
from strategy.features.LowHighTargets import LowHighTargets
from strategy.features.MultiIndiFeatures import MultiIndiFeatures
from strategy.signal.SignalByFutLowHigh import SignalByFutLowHigh


class LgbLowHighRegressionStrategy(StrategyBase):
    """
     Lgb regression, multiple indicators are features, predicts low/high
   """

    def __init__(self, config: Dict, exchange_provider: Exchange):
        self.websocket_feed = None
        StrategyBase.__init__(self, config=config,
                              exchange_provider=exchange_provider,
                              is_candles_feed=True,
                              is_bid_ask_feed=False,
                              is_level2_feed=False)
        self.comissionpct = float(config.get('pytrade2.broker.comissionpct'))
        self.signal_calc = SignalByFutLowHigh(self.profit_loss_ratio, self.stop_loss_min_coeff,
                                              self.stop_loss_max_coeff, self.take_profit_min_coeff,
                                              self.take_profit_max_coeff, self.comissionpct, self.price_precision)
        self._logger.info(f"SignalCalc: {self.signal_calc}")
        # Should keep 1 more candle for targets
        predict_window = config["pytrade2.strategy.predict.window"]
        self.target_period = predict_window
        self.model_name = "MultiOutputRegressorLgb"
        self.history_days = config.get("pytrade2.feed.candles.history.days", 2)

        self._logger.info(f"Target period: {self.target_period}")

    def can_learn(self) -> bool:
        # Only candles feed is for data. Bid ask feed is for trailing stop support, don't check it.

        return bool(self.candles_feed.candles_by_interval)

    def prepare_xy(self) -> (pd.DataFrame, pd.DataFrame):
        with self.data_lock:
            x = MultiIndiFeatures.multi_indi_features(self.candles_feed.candles_by_interval)

            # Candles with minimal period
            min_period = min(self.candles_feed.candles_by_interval.keys(), key=pd.Timedelta)
            candles = self.candles_feed.candles_by_interval[min_period]
            y = LowHighTargets.fut_lohi(candles, self.target_period)

            # y has less items because of diff()
            x = x[x.index.isin(y.index)]

        return x, y

    def prepare_last_x(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        with self.data_lock:
            x = MultiIndiFeatures.multi_indi_features_last(
                self.candles_feed.candles_by_interval) if self.candles_feed.candles_by_interval else pd.DataFrame.empty
        return x

    def predict(self, x):
        # Save to buffer, actual persist by schedule of data persister
        self.data_persister.save_last_data(self.ticker, {'x': x})
        with self.data_lock:
            x_trans = self.X_pipe.transform(x)
            y_arr = self.model.predict(x_trans)
            y_arr = self.y_pipe.inverse_transform(y_arr)
            y_arr = y_arr.reshape((-1, 2))[-1]  # Last and only row
            fut_low_diff, fut_high_diff = y_arr[0], y_arr[1]
            y_df = pd.DataFrame(data={'fut_low_diff': fut_low_diff, 'fut_high_diff': fut_high_diff},
                                index=x.tail(1).index)
        return y_df

    def process_prediction(self, y_pred: pd.DataFrame):
        # Calc signal
        close_time, open_, high, low, close = \
            self.candles_feed.candles_by_interval[self.target_period][
                ['close_time', 'open', 'high', 'low', 'close']].iloc[-1]
        fut_low_diff, fut_high_diff = y_pred.loc[y_pred.index[-1], ["fut_low_diff", "fut_high_diff"]]
        fut_low, fut_high = close + fut_low_diff, close + fut_high_diff

        # signal, sl, tp = self.signal_calc.calc_signal(close, low, high, fut_low, fut_high)
        signal_ext = self.signal_calc.calc_signal_ext(close, fut_low, fut_high)
        dt, signal, price, sl, tp = (signal_ext['datetime'],
                                     signal_ext['signal'],
                                     signal_ext['price'],
                                     signal_ext['sl'],
                                     signal_ext['tp'])
        tr_delta = abs(close - sl) if sl else None
        signal_ext['tr_delta'] = tr_delta

        # Metrics
        MetricServer.metrics.strategy.signal.signal.set(signal)
        MetricServer.metrics.strategy.feed.candles.open.set(open_)
        MetricServer.metrics.strategy.feed.candles.high.set(high)
        MetricServer.metrics.strategy.feed.candles.low.set(low)
        MetricServer.metrics.strategy.feed.candles.close.set(close)

        if signal:
            MetricServer.metrics.strategy.signal.signal_price.set(price)
            MetricServer.metrics.strategy.signal.signal_sl.set(sl)
            MetricServer.metrics.strategy.signal.signal_tp.set(tp)
            MetricServer.metrics.strategy.signal.signal_tr_delta.set(tr_delta)

        MetricServer.metrics.strategy.prediction.pred_fut_low_diff.set(fut_low_diff)
        MetricServer.metrics.strategy.prediction.pred_fut_high_diff.set(fut_high_diff)

        risk_manager_ok = self.risk_manager.can_trade()
        signal_ext['status'] = None
        signal_ext['open_price'] = None
        if not risk_manager_ok:
            signal_ext["status"] = "risk_manager_oom"
        cur_trade_ok = self.broker.cur_trade is None
        if not cur_trade_ok:
            signal_ext["status"] = "already_in_market"
        if signal == 0:
            signal_ext["status"] = "signal_oom"

        # Trade
        if signal and cur_trade_ok and risk_manager_ok:
            self.broker.create_cur_trade(symbol=self.ticker,
                                         direction=signal,
                                         quantity=self.order_quantity,
                                         price=price,
                                         stop_loss_price=sl,
                                         take_profit_price=tp,
                                         trailing_delta=tr_delta)
            if self.broker.cur_trade:
                signal_ext['open_price'] = self.broker.cur_trade.open_price
                signal_ext['status'] = 'order_created'
            else:
                signal_ext['open_price'] = None
                signal_ext['status'] = 'order_not_created'

        # Persist the data for later analysis
        y_pred["datetime"] = dt
        signal_ext_df = pd.DataFrame(data=[signal_ext]).set_index('datetime')
        self.data_persister.save_last_data(self.ticker, {'signal_ext': signal_ext_df, 'y_pred': y_pred})

    def create_model(self, X_size, y_size):
        if not self.model:
            lgb_model = lgb.LGBMRegressor(verbose=-1)
            self.model = MultiOutputRegressor(lgb_model)
        self._logger.info(f'Created lgb model: {self.model}')
        return self.model

    def apply_params(self, params: dict) -> None:
        """ After last model and params read from mlflow, apply params to strategy"""
        self._logger.info("Applying new params")
        super().apply_params(params)

        with self.data_lock:

            # Signal calculator should be recreated with new params
            self.signal_calc = SignalByFutLowHigh(self.profit_loss_ratio, self.stop_loss_min_coeff,
                                                  self.stop_loss_max_coeff, self.take_profit_min_coeff,
                                                  self.take_profit_max_coeff, self.comissionpct, self.price_precision)
            self._logger.info(f"Updated signal calc: {self.signal_calc}")

            # Set new history days and candles periods
            self.candles_feed.apply_history_days(self.history_days)
            periods = params["features_candles_periods"]
            self.candles_feed.apply_periods(periods, self.history_days)
            MetricServer.app_params["candles_cnt_by_interval"] = self.candles_feed.candles_cnt_by_interval

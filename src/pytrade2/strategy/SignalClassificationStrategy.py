import logging
from datetime import datetime
from typing import Dict
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler, MaxAbsScaler

from pytrade2.exch.Exchange import Exchange
from pytrade2.features.CandlesMultiIndiFeatures import CandlesMultiIndiFeatures
from pytrade2.features.LowHighTargets import LowHighTargets
from pytrade2.features.level2.Level2MultiIndiFeatures import Level2MultiIndiFeatures
from pytrade2.feed.StreamWithHistoryPreprocFeed import StreamWithHistoryPreprocFeed
from pytrade2.metrics.MetricServer import MetricServer
from pytrade2.strategy.common.StrategyBase import StrategyBase


class SignalClassificationStrategy(StrategyBase):
    """
    Model generates buy, sel or oom signal. Strategy is based on this signal.
   """

    def __init__(self, config: Dict, exchange_provider: Exchange):
        # self.websocket_feed = None
        StrategyBase.__init__(self, config=config,
                              exchange_provider=exchange_provider,
                              is_candles_feed=True,
                              is_bid_ask_feed=False,
                              is_level2_feed=True)

        self.candles_feed = StreamWithHistoryPreprocFeed(config=config, stream_feed=self.candles_feed)
        self.level2_feed = StreamWithHistoryPreprocFeed(config=config, stream_feed=self.level2_feed)

        # self.comissionpct = float(config.get('pytrade2.broker.comissionpct'))
        self.history_min_window = config["pytrade2.strategy.history.min.window"]
        self.history_max_window = config["pytrade2.strategy.history.max.window"]
        self.target_period = config["pytrade2.strategy.predict.window"]

        self.model_name = "SignalClassifierLgb"
        self.candles_periods = [period.strip() for period in
                                config["pytrade2.strategy.features.candles.periods"].split(",")]
        self.level2_periods = [period.strip() for period in
                               config["pytrade2.strategy.features.level2.periods"].split(",")]
        self.stop_loss_coeff = float(config["pytrade2.strategy.stoploss.coeff"])
        self.profit_loss_ratio = float(config["pytrade2.strategy.profit.loss.ratio"])

        self._logger.info(f"Target period: {self.target_period}")

    def can_learn(self) -> bool:
        """ Check preconditions for learning"""
        self._logger.debug(
            f"Level2 filled: {self.level2_feed.is_good_history}, candles filled: {self.candles_feed.is_good_history}")
        return self.level2_feed.is_good_history and self.candles_feed.is_good_history

    def apply_buffers(self):
        self.level2_feed.apply_buf()
        self.candles_feed.apply_buf()

    def features_targets(self, history_window: str, with_targets: bool = True) -> (pd.DataFrame, pd.DataFrame):

        with self.data_lock:
            if not (self.level2_feed.is_good_history and self.candles_feed.is_good_history):
                self._logger.debug(
                    f"Non enough history. Level2 is good:{self.level2_feed.is_good_history}, candles are good:{self.candles_feed.is_good_history}")
                return pd.DataFrame(), pd.DataFrame()

            full_candles_1min = self.candles_feed.preproc_data_df
            full_level2_1min = self.level2_feed.preproc_data_df
            last_time = max(full_candles_1min.index.max(), full_level2_1min.index.max())
            window_start = last_time - pd.to_timedelta(history_window)

            # Candles features within history window
            candles_1min = full_candles_1min[full_candles_1min.index >= window_start]

            candles_by_periods: Dict[str, pd.DataFrame] = CandlesMultiIndiFeatures.resample_by_periods(candles_1min,
                                                                                                       self.candles_periods)
            # candles_by_periods: dict[str, pd.DataFrame] = CandlesFeatures.rolling_candles_by_periods(candles_1min,
            #                                                                                          self.candles_periods)
            candles_features = CandlesMultiIndiFeatures.multi_indi_features(candles_by_periods)

            # Merge candles with level2 features
            level2 = full_level2_1min[full_level2_1min.index >= window_start]
            level2_features = Level2MultiIndiFeatures.level2_features_of(level2, self.level2_periods)
            combined_features = pd.merge_asof(candles_features, level2_features, left_index=True, right_index=True)
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(f"Features calculation status. window start: {window_start}, "
                                   f"candles1min: {len(full_candles_1min)}, level21min: {len(full_level2_1min)}, "
                                   f"candles features: {len(candles_features)}, periods: {self.candles_periods},"
                                   f"level2 features: {len(level2_features)},"
                                   f"combined features: {len(combined_features)}")

            if with_targets:
                # Targets
                targets = LowHighTargets.fut_lohi_signal(candles_1min, self.target_period, self.stop_loss_coeff,
                                                         self.profit_loss_ratio)
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.debug(
                        f"Targets calculation status. target period: {self.target_period}, targets: {len(targets)}, "
                        f"stop loss coeff: {self.stop_loss_coeff}, profit loss ratio: {self.profit_loss_ratio}")

                # Clean bad features before min_history_window after time gaps in input data
                # candles_level2_intersection = candles_1min.index.intersection(self.level2_feed.level2.index)
                # targets = feature_cleaner.clean(candles_level2_intersection, targets, min_history_window)

                # Merge level2 and candles features
                common_index = combined_features.dropna().index.intersection(targets.dropna().index)
                features = combined_features.loc[common_index]
                targets = targets.loc[common_index]

            else:
                # All features, we cannot calculate targets for recent data
                features, targets = combined_features, None
            self._logger.debug(
                f"Final features and targets calculation status. With targets: {with_targets}\n "
                f"features:\n {features.tail()}\n targets:\n { str(targets.tail()) if targets is not None else 'not required'}")

            return features, targets

    def prepare_xy(self) -> (pd.DataFrame, pd.DataFrame):
        time_window = self.history_max_window
        self._logger.debug(f"Preparing xy for {time_window}")
        return self.features_targets(time_window, with_targets=True)

    def prepare_last_x(self) -> pd.DataFrame:
        self._logger.debug(f"Preparing last x with minimum history required {self.history_min_window}")
        features, _ = self.features_targets(self.history_min_window, with_targets=False)
        return features

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
            # Calc last price, we expect new trade to be opened at this if signal is 1 or -1
            price = self.candles_feed.preproc_data_df['close'][-1]
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

    def create_model(self, X_size, y_size):
        import lightgbm as lgb
        if not self.model:
            # Initialize with multi-class parameters
            model = lgb.LGBMClassifier(
                objective='multiclass',
                num_class=3,
                num_leaves=31,
                learning_rate=0.05,
                n_estimators=100,
                random_state=42
            )
            print(f'Created new model {model}')
            return model

    def create_pipe(self, x, y) -> (Pipeline, Pipeline):
        self._logger.debug(f"Creating x,y pipelines")
        # Scale features time and float columns differently
        time_cols = [col for col in x.columns if col.startswith("time")]
        float_cols = list(set(x.columns) - set(time_cols))
        x_pipe = Pipeline(
            [("xscaler", ColumnTransformer([("xrs", StandardScaler(), float_cols)], remainder="passthrough").set_output(transform="pandas")),
             ("xmms", MaxAbsScaler().set_output(transform="pandas"))]).set_output(transform="pandas")
        x_pipe.fit(x)

        # LBGM requires integer labels from 1 to n_classes
        class AddTwo(BaseEstimator, TransformerMixin):
            def __init__(self):
                self.is_fitted_ = True

            def fit(self, X, _=None):
                return self

            def transform(self, X):
                # Adding 2 to every value in the dataset
                #return  sklearn.utils.validation.column_or_1d(X) + 2
                #return X + 2
                return np.array(X).ravel() + 2

            def inverse_transform(self, X):
                #return X - 2
                return np.array(X).ravel() - 2

        y_pipe = make_pipeline(AddTwo())
        y_pipe.fit(y)
        return x_pipe, y_pipe

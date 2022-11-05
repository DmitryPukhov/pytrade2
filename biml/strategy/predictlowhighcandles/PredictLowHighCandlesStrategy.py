import collections
import glob
import logging
from collections import defaultdict
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Dict, List
import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential, Model
from scikeras.wrappers import KerasRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from AppTools import AppTools
from feed.BinanceCandlesFeed import BinanceCandlesFeed
from feed.TickerInfo import TickerInfo
from strategy.predictlowhighcandles.LowHighCandlesFeatures import LowHighCandlesFeatures
from strategy.StrategyBase import StrategyBase


class PredictLowHighCandlesStrategy(StrategyBase):
    """
    Candles based. Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    # def __init__(self, broker, ticker: str, model_dir: str):
    def __init__(self, broker, config: Dict):
        super().__init__(broker)
        self.config = config
        self.tickers = AppTools.read_candles_tickers(self.config)
        self.ticker: str = self.tickers[-1].ticker
        self.model_dir = self.config["biml.model.dir"]

        if self.model_dir:
            self.model_weights_dir = str(Path(self.model_dir, self.__class__.__name__, "weights"))
            self.model_Xy_dir = str(Path(self.model_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)
        self.model = None
        self.window_size = 15
        self.candles_size = self.window_size * 100
        self.predict_sindow_size = 1
        self.candles = pd.DataFrame()
        self.model = None

        # Minimum stop loss ratio = (price-stop_loss)/price
        self.min_stop_loss_ratio = 0.005
        # Minimum profit/loss
        self.profit_loss_ratio = 4

        self.logger = logging.getLogger(self.__class__.__name__)

        if self.broker:
            self.broker.close_opened_positions(self.ticker)
            # Raise exception if we are in trade for this ticker
            self.assert_out_of_market(self.ticker)

    def run(self, client):
        """
        Attach to the feed and listen
        """
        feed = BinanceCandlesFeed(spot_client=client, tickers=self.tickers)
        feed.consumers.append(self)
        feed.run()

    def on_candles(self, ticker: str, interval: str, new_candles: pd.DataFrame):
        """
        Received new candles from feed
        """

        if ticker != self.ticker or interval != "1m":
            return
        # Append new candles to current candles
        # This strategy is a single ticker and interval and only these candles can come
        new_candles["signal"] = 0
        self.candles = self.candles.append(new_candles).tail(self.candles_size + self.predict_sindow_size)
        if len(self.candles) < (self.window_size + self.predict_sindow_size):
            return
        # Fit on last
        self.learn_on_last()

        # Get last predicted signal
        signal, price, stop_loss, take_profit = self.last_signal(self.candles)
        if signal:
            opened_quantity, opened_orders = self.broker.opened_positions(self.ticker)
            if not opened_quantity and not opened_orders:
                # Buy or sell
                self.broker.create_order(symbol=self.ticker,
                                         order_type=signal,
                                         quantity=self.order_quantity,
                                         price=price,
                                         stop_loss=stop_loss,
                                         take_profit=take_profit)
            else:
                logging.info(
                    f"Do not create {signal} order for {self.ticker} because we already have {len(opened_orders)}"
                    f" orders and {opened_quantity} quantity")

    def last_signal(self, df: pd.DataFrame) -> (int, int, int, int):
        """
        Return buy or sell or no signal using predicted prices
        :param df: candles dataframe
        :return: 1 for buy, -1 sell, 0 no signal
        """
        signal, price, stop_loss, stop_loss_adj, take_profit = 0, None, None, None, None

        if df.empty:
            self.logger.debug("Candles are empty")
            return signal, price, stop_loss, take_profit
        close, fut_high, fut_low = df["close"].iloc[-1], df["fut_high"].iloc[-1], df["fut_low"].iloc[-1]
        delta_high, delta_low = (fut_high - close), (close - fut_low)
        ratio = abs(delta_high / delta_low)

        # delta_high, delta_low = fut_high - close, close - fut_low

        logging.debug(
            f"Calculating signal. close: {close}, fut_high:{fut_high}, fut_low:{fut_low},"
            f" delta_high:{delta_high}, delta_low: {delta_low}, "
            f"ratio: {max(ratio, 1 / ratio)}, profit_loss_ratio:{self.profit_loss_ratio}")
        if ratio >= self.profit_loss_ratio:
            # Buy signal
            signal, price, stop_loss, take_profit = 1, close, fut_low, fut_high
            # Adjust stop loss if predicted is too small
            stop_loss_adj = min(stop_loss, price * (1 - self.min_stop_loss_ratio))
        elif ratio <= 1 / self.profit_loss_ratio:
            # Sell signal
            signal, price, stop_loss, take_profit = -1, close, fut_high, fut_low
            # Adjust stop loss if predicted is too small
            stop_loss_adj = max(stop_loss, price * (1 + self.min_stop_loss_ratio))

        if signal:
            self.logger.debug(
                f"Calculated signal: {signal}, price:str(price), "
                f"stop_loss: {stop_loss} ({stop_loss - price}),"
                f"stop_loss adjusted: {stop_loss} ({stop_loss_adj - price})  for min ratio {self.min_stop_loss_ratio},"
                f"take_profit: {take_profit} ({take_profit - price}).")

        if signal and abs(take_profit - price) < abs(price - stop_loss_adj) * self.profit_loss_ratio:
            self.logger.debug(
                f"Expected profit {abs(take_profit - price)} is too small "
                f"for loss {abs(price - stop_loss)}, adjusted loss {abs(price - stop_loss_adj)}"
                f" and profit loss ratio {self.profit_loss_ratio}. set signal to 0")
            signal, price, stop_loss_adj, take_profit = 0, None, None, None

        self.logger.debug(
            f"Calculated adjusted signal: {signal}, price:{price}, stop_loss: {stop_loss_adj}, take_profit: {take_profit}.")

        return signal, price, stop_loss_adj, take_profit

    def learn_on_last(self):
        """
        Fit the model on last data window with new candle
        """
        # Fit
        train_X, train_y = LowHighCandlesFeatures.features_and_targets(self.candles, self.window_size,
                                                                       self.predict_sindow_size)
        self.model = self.create_pipe(train_X, train_y, 1, 1) if not self.model else self.model
        self.model.fit(train_X, train_y)

        # Predict
        X_last = LowHighCandlesFeatures.features_of(self.candles, self.window_size).tail(1)
        y_pred = self.model.predict(X_last)
        LowHighCandlesFeatures.set_predicted_fields(self.candles, y_pred)
        self.logger.debug(f"Predicted fut_low-close: {y_pred[0][0]}, fut_high-fut_low:{y_pred[0][1]}")
        #
        # y_low = y_pred[0][0]
        # self.candles.loc[self.candles.index[-1], "fut_low"] = self.candles.loc[self.candles.index[-1], "close"] + y_low
        # y_high = y_pred[0][1]
        # self.candles.loc[self.candles.index[-1], "fut_high"] = self.candles.loc[
        #                                                            self.candles.index[-1], "close"] + y_high

        # Save model
        self.save_model()
        self.save_lastXy(X_last, y_pred, self.candles.tail(1))

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(Input(shape=(X_size,)))
        model.add(Dense(512, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(1024, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(512, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(128, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(64, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(y_size, activation='softmax'))
        model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['mean_squared_error'])

        # Load weights
        self.load_last_model(model)
        # model.summary()
        return model

    def learn(self, data_items: Dict):
        """
        Learn the model on historical data
        :param data_items: Dict{(ticker, interval): dataframe]
        """
        # this strategy is single asset and interval, the only item in dict,
        # but for the sake of logic let's concatenate the dict vals
        data = reduce(lambda df1, df2: df1.append(df2), data_items.values()).sort_index()

        # Feature engineering.

        X, y = LowHighCandlesFeatures.features_and_targets(data, self.window_size, self.predict_sindow_size)

        logging.info(f"Learn set size: {len(X)}")

        # self.pipe = self.create_pipe(X, y,epochs= 100, batch_size=100) if not self.pipe else self.pipe
        # tscv = TimeSeriesSplit(n_splits=20)
        self.model = self.create_pipe(X, y, epochs=50, batch_size=100) if not self.model else self.model
        tscv = TimeSeriesSplit(n_splits=7)
        cv = cross_val_score(self.model, X=X, y=y, cv=tscv, error_score="raise")
        print(cv)
        # Save weights
        self.save_model()

    def load_last_model(self, model: Model):
        saved_models = glob.glob(str(Path(self.model_weights_dir, "*.index")))
        if saved_models:
            last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
            logging.debug(f"Load model from {last_model_path}")
            model.load_weights(last_model_path)
        else:
            logging.info(f"No saved models in {self.model_weights_dir}")

    def save_model(self):
        # Save the model
        model: Model = self.model.regressor.named_steps["model"].model

        model_path = str(Path(self.model_weights_dir, datetime.now().isoformat()))
        logging.debug(f"Save model to {model_path}")
        model.save_weights(model_path)

    def save_lastXy(self, X_last: pd.DataFrame, y_pred_last, candles: pd.DataFrame):
        """
        Write model X,y data to csv for analysis
        """
        time = X_last.index[-1]
        file_name_prefix = f"{pd.to_datetime(time).date()}_{self.ticker}_"
        Xpath = str(Path(self.model_Xy_dir, file_name_prefix + "X.csv"))
        ypath = str(Path(self.model_Xy_dir, file_name_prefix + "y.csv"))
        candlespath = str(Path(self.model_Xy_dir, file_name_prefix + "candles.csv"))
        logging.debug(f"Save X to {Xpath},y to {ypath}, candles to {candlespath}")

        # Save X
        X_last.to_csv(Xpath, header=not Path(Xpath).exists(), mode='a')
        # Create y_pred from ndarray and save
        y_pred_last_df = pd.DataFrame(index=X_last.index, data=y_pred_last,
                                      columns=["fut_delta_low", "fut_candle_size"])
        y_pred_last_df.to_csv(ypath, header=not Path(ypath).exists(), mode='a')
        # Save candles with predicted data
        # candles = candles.join(y_pred_last_df)
        candles.to_csv(candlespath, header=not Path(candlespath).exists(), mode='a')

    def create_pipe(self, X: pd.DataFrame, y: pd.DataFrame, epochs: int, batch_size: int) -> TransformedTargetRegressor:
        # Fit the model
        regressor = KerasRegressor(model=self.create_model(X_size=len(X.columns), y_size=len(y.columns)),
                                   epochs=epochs, batch_size=batch_size, verbose=1)
        column_transformer = ColumnTransformer(
            [
                ('xscaler', StandardScaler(), X.columns)
                # ('yscaler', StandardScaler(), y.columns)
                # ('cat_encoder', OneHotEncoder(handle_unknown="ignore"), y.columns)
            ]
        )

        pipe = Pipeline([("column_transformer", column_transformer), ('model', regressor)])
        wrapped = TransformedTargetRegressor(regressor=pipe, transformer=StandardScaler())
        return wrapped

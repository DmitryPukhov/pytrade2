import glob
import logging
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Dict
from binance.spot import Spot as Client
import pandas as pd
from keras import Input
from keras.layers import Dense
from keras.layers.core.dropout import Dropout
from keras.models import Sequential, Model
from scikeras.wrappers import KerasClassifier
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from features.FeatureEngineering import FeatureEngineering
from strategy.StrategyBase import StrategyBase


class FutureLowHigh(StrategyBase):
    """
    Predict low/high value in the nearest future period.
    Buy if future high/future low > ratio, sell if symmetrically. Off market if both below ratio
    """

    def __init__(self, client: Client, ticker: str, model_dir: str):
        super().__init__(client)
        self.model_weights_dir = str(Path(model_dir, self.__class__.__name__, "weights"))
        self.model_Xy_dir = str(Path(model_dir, self.__class__.__name__, "Xy"))
        Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)
        self.ticker = ticker
        self.order_quantity = 0.001
        self.stop_loss_ratio = 0.02
        self.model = None
        self.window_size = 15
        self.candles_size = self.window_size * 100
        self.predict_sindow_size = 1
        self.candles = pd.DataFrame()
        self.fe = FeatureEngineering()
        self.pipe = None

        self.close_opened_positions(ticker)
        # Raise exception if we are in trade for this ticker
        self.assert_out_of_market(ticker)

    def on_candles(self, ticker: str, interval: str, new_candles: pd.DataFrame):
        """
        Received new candles from feed
        """
        if ticker != self.ticker:
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
        signal = {-1: "SELL", 0: None, 1: "BUY"}[self.candles.signal[-1]]
        logging.debug(f"Last signal: {signal}")
        if signal:
            opened_quantity,opened_orders = self.opened_positions(self.ticker)
            if opened_quantity == 0:
                # Buy or sell
                close_price = self.candles.close[-1]
                self.create_order(symbol=self.ticker, side=signal, price=close_price, quantity=self.order_quantity,
                                  stop_loss_ratio=self.stop_loss_ratio, )
            else:
                logging.info(f"Do not create an order because we already have {opened_quantity} {self.ticker}: {opened_orders}")

    def learn_on_last(self):
        """
        Fit the model on last data window with new candle
        """
        # Fit
        train_X, train_y = self.fe.features_and_targets(self.candles, self.window_size, self.predict_sindow_size)
        self.pipe = self.create_pipe(train_X, train_y, 1, 1) if not self.pipe else self.pipe
        self.pipe.fit(train_X, train_y)

        # Predict
        X_last = self.fe.features(self.candles, self.window_size).tail(1)
        y_pred = self.pipe.predict(X_last)

        signal = int(train_y.columns[y_pred.argmax(axis=1)][0].lstrip("signal_"))
        self.candles.loc[self.candles.index[-1], "signal"] = signal

        # Save model
        self.save_model()
        self.save_lastXy(X_last, self.candles["signal"].tail(1))

        return signal

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
        model.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])

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
        X, y = self.fe.features_and_targets_balanced(data, self.window_size, self.predict_sindow_size)
        logging.info(f"Learn set size: {len(X)}")

        # self.pipe = self.create_pipe(X, y,epochs= 100, batch_size=100) if not self.pipe else self.pipe
        # tscv = TimeSeriesSplit(n_splits=20)
        self.pipe = self.create_pipe(X, y, epochs=50, batch_size=100) if not self.pipe else self.pipe
        tscv = TimeSeriesSplit(n_splits=7)
        cv = cross_val_score(self.pipe, X=X, y=y, cv=tscv, error_score="raise")
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
        model: Model = self.pipe.named_steps["model"].model

        model_path = str(Path(self.model_weights_dir, datetime.now().isoformat()))
        logging.debug(f"Save model to {model_path}")
        model.save_weights(model_path)

    def save_lastXy(self, X_last: pd.DataFrame, y_last: pd.DataFrame):
        """
        Write model X,y data to csv for analysis
        """
        time = X_last.index[-1]
        file_name_prefix = f"{pd.to_datetime(time).date()}_{self.ticker}_"
        Xpath = str(Path(self.model_Xy_dir, file_name_prefix + "X.csv"))
        ypath = str(Path(self.model_Xy_dir, file_name_prefix + "y.csv"))

        logging.debug(f"Save X to {Xpath},y to {ypath}")
        X_last.to_csv(Xpath, header=not Path(Xpath).exists(), mode='a')
        y_last.to_csv(ypath, header=not Path(ypath).exists(), mode='a')

    def create_pipe(self, X: pd.DataFrame, y: pd.DataFrame, epochs: int, batch_size: int) -> Pipeline:
        # Fit the model
        estimator = KerasClassifier(model=self.create_model(X_size=len(X.columns), y_size=len(y.columns)),
                                    epochs=epochs, batch_size=batch_size, verbose=1)

        # estimator = KerasClassifier(build_fn=self.create_model, X_size=len(X.columns), y_size=len(y.columns),
        #                             epochs=epochs, batch_size=batch_size, verbose=1)
        column_transformer = self.fe.column_transformer(X, y)

        return Pipeline([("column_transformer", column_transformer), ('model', estimator)])

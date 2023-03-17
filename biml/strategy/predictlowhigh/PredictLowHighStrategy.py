import logging
from pathlib import Path
from typing import Dict, List
import pandas as pd
from keras import Sequential, Input
from keras.layers import Dense, Dropout
from scikeras.wrappers import KerasRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from feed.BaseFeed import BaseFeed
from feed.BinanceWebsocketFeed import BinanceWebsocketFeed
from strategy.PeriodicalLearnStrategy import PeriodicalLearnStrategy
from strategy.PersistableModelStrategy import PersistableModelStrategy
from strategy.StrategyBase import StrategyBase
from strategy.predictlowhigh.PredictLowHighFeatures import PredictLowHighFeatures


class PredictLowHighStrategy(StrategyBase, PeriodicalLearnStrategy, PersistableModelStrategy):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, broker, config: Dict):
        StrategyBase.__init__(self, broker, config)
        PeriodicalLearnStrategy.__init__(self, config)
        self._log = logging.getLogger(self.__class__.__name__)

        self.config = config
        self.tickers = self.config["biml.tickers"].split(",")
        self.model_dir = self.config["biml.model.dir"]
        self.min_history_interval = pd.Timedelta("2 seconds")

        if self.model_dir:
            self.model_weights_dir = str(Path(self.model_dir, self.__class__.__name__, "weights"))
            self.model_Xy_dir = str(Path(self.model_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger(self.__class__.__name__)
        self.ticker = pd.DataFrame(columns=BaseFeed.bid_ask_columns).set_index("datetime")
        self.buffer = []

        self.bid_ask: pd.DataFrame = pd.DataFrame()
        self.level2: pd.DataFrame = pd.DataFrame()
        self.is_learning = False
        self.is_processing = False
        self.model = None

    def run(self, client):
        """
        Attach to the feed and listen
        """
        feed = BinanceWebsocketFeed(tickers=self.tickers)
        feed.consumers.append(self)
        feed.run()

    def on_level2(self, level2: List[Dict]):
        """
        Got new order book items event
        """
        new_df = pd.DataFrame(level2, columns=BaseFeed.bid_ask_columns).set_index("datetime", drop=False)
        self.level2 = self.level2.append(new_df)
        # self.level2 = pd.concat([self.level2, new_df]) if self.level2 is not None else new_df
        self.learn_or_skip()
        self.process_new_data()

    def on_ticker(self, ticker: dict):
        new_df = pd.DataFrame([ticker], columns=ticker.keys()).set_index("datetime", drop=False)
        self.bid_ask = self.bid_ask.append(new_df)
        # self.bid_ask = self.bid_ask if self.bid_ask is not None else new_df
        self.learn_or_skip()
        self.process_new_data()

    def process_new_data(self):
        if not self.bid_ask.empty and not self.level2.empty and self.model and not self.is_processing:
            self.is_processing = True
            y = self.predict_low_high()
            self.is_processing = False

    def predict_low_high(self):
        X = PredictLowHighFeatures.features_of(self.bid_ask, self.level2)
        y = self.model.predict(X) if not X.empty else pd.DataFrame.empty
        return y

    def can_learn(self) -> bool:
        """ Check preconditions for leraning"""
        # Check learn conditions
        if self.is_learning:
            #self._log.info("Cannot learn: previous learn is still in progress")
            return False
        if self.bid_ask.empty or self.level2.empty:
            # self._log.info(
            #     f"Cannot learn. bid_ask empty: {self.bid_ask.empty}, level2 empty: {self.level2.empty}")
            return False
        interval = self.bid_ask.index.max() - self.bid_ask.index.min()
        if interval < self.min_history_interval:
            # self._log.info(
            #     f"Cannot learn, need {self.min_history_interval} of data. Existing data contains only {interval} "
            #     f"from {self.bid_ask.index.min()} to {self.bid_ask.index.max()}")
            return False
        return True

    def learn(self):
        if self.is_learning:
            return
        self._log.info("Learning")
        self.is_learning = True
        try:
            train_X, train_y = PredictLowHighFeatures.features_targets_of(self.bid_ask, self.level2)
            model = self.create_pipe(train_X, train_y, 1, 1) if not self.model else self.model
            self._log.info(f"Train data len: {train_X.shape[0]}")
            if not train_X.empty:
                model.fit(train_X, train_y)
                self.model = model
        finally:
            self.is_learning = False
            self._log.info("Learning completed")

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

from keras import Sequential
from keras.layers import Dense, Dropout, LSTM

from pytrade2.strategy.common.LSTMBidAskRegressionStrategyBase import LSTMBidAskRegressionStrategyBase


class LSTMBidAskRegressionStrategy1(LSTMBidAskRegressionStrategyBase):
    """
    LSTM Neural network with minimum layers
    """

    def create_model(self, X_size, y_size):
        model = Sequential()
        model.add(LSTM(128, return_sequences=True, input_shape=(self.lstm_window_size, X_size)))
        model.add(Dropout(0.2))
        model.add(LSTM(32))
        model.add(Dropout(0.2))
        model.add(Dense(20, activation='relu'))
        model.add(Dense(y_size, activation='linear'))
        # model.add(Dense(y_size, activation='softmax'))

        model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['mean_squared_error'])
        # Load weights
        self.model_persister.load_last_model(model)
        model.summary()
        return model

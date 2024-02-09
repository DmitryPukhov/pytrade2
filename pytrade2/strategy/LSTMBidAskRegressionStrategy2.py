from keras import Sequential
from keras.layers import Dense, Dropout, LSTM

from strategy.common.LSTMBidAskRegressionStrategyBase import LSTMBidAskRegressionStrategyBase


class LSTMBidAskRegressionStrategy2(LSTMBidAskRegressionStrategyBase):
    """
    LSTM experimental with more layers
    """

    def create_model(self, X_size, y_size):
        model = Sequential()
        # LSTM2v1 was 10,825,588,190,59
        # LSTM2v2 now
        model.add(LSTM(320, return_sequences=True, input_shape=(self.lstm_window_size, X_size)))

        model.add(Dropout(0.2))
        model.add(LSTM(160))
        model.add(Dropout(0.2))
        model.add(Dense(40, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(16, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(y_size, activation='linear')) # linear for regression
        model.compile(optimizer='adam', loss='mae', metrics=['mse'])
        # Load weights
        self.model_persister.load_last_model(model)
        model.summary()
        return model

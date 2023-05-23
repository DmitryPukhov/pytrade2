from keras import Sequential
from keras.layers import Dense, Dropout, LSTM

from strategy.common.LSTMStrategyBase import LSTMStrategyBase


class LSTMStrategy2(LSTMStrategyBase):
    """
    LSTM experimental with more layers
    """

    def create_model(self, X_size, y_size):
        model = Sequential()
        # 10,825,588,190,59
        model.add(LSTM(825, return_sequences=True, input_shape=(self.lstm_window_size, X_size)))

        model.add(Dropout(0.1))
        model.add(LSTM(588))
        model.add(Dropout(0.2))
        model.add(Dense(190, activation='relu'))
        model.add(Dropout(0.1))
        model.add(Dense(59, activation='relu'))
        model.add(Dropout(0.2))
        model.add(Dense(y_size, activation='linear')) # linear for regression
        model.compile(optimizer='adam', loss='mae', metrics=['mse'])
        # Load weights
        self.load_last_model(model)
        model.summary()
        return model

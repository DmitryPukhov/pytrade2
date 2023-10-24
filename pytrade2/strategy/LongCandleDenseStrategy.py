from keras import Sequential, Input
from keras.layers import Dense, Dropout
from strategy.common.LongCandleStrategyBase import LongCandleStrategyBase
import tensorflow as tf
import tensorflow.python as tf

class LongCandleDenseStrategy(LongCandleStrategyBase):
    """ Predict long candle, NN with dense layers mainly """

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
        tf.keras.losses.CategoricalCrossentropy()

        # Load weights
        self.load_last_model(model)
        model.summary()
        return model

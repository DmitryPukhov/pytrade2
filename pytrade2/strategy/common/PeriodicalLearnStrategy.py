import gc
import logging
from datetime import datetime, timedelta
from threading import Thread

import pandas as pd
import tensorflow.python.keras.backend


class PeriodicalLearnStrategy:
    """ Each learn interval this strategy fits on new data """

    def __init__(self, config):
        self._log = logging.getLogger(self.__class__.__name__)
        self.learn_interval: timedelta = timedelta(seconds=config['pytrade2.strategy.learn.interval.sec'])
        self.last_learn_time: datetime = datetime.min

    def learn_or_skip(self):
        time1 = datetime.utcnow()
        if (time1 - self.last_learn_time >= self.learn_interval) and self.can_learn():
            self._log.debug(f"{self.learn_interval} elapsed from last learn time: {self.last_learn_time}")
            Thread(target=self.learn).start()
            tensorflow.keras.backend.clear_session() # To avoid OOM
            self.last_learn_time = time1

    def can_learn(self):
        return True

    def learn(self):
        raise NotImplementedError()

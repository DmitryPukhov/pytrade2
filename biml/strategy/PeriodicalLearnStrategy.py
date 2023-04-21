import logging
from datetime import datetime, timedelta
from threading import Thread


class PeriodicalLearnStrategy:
    def __init__(self, config):
        self._log = logging.getLogger(self.__class__.__name__)
        self.learn_interval: timedelta = timedelta(seconds=config['biml.strategy.learn.interval.sec'])
        self.last_learn_time: datetime = datetime.min

    def learn_or_skip(self):
        time1 = datetime.utcnow()
        if (time1 - self.last_learn_time >= self.learn_interval) and self.can_learn():
            self._log.debug(f"{self.learn_interval} elapsed from last learn time: {self.last_learn_time}")
            Thread(target=self.learn).start()
            self.last_learn_time = time1

    def can_learn(self):
        return True

    def learn(self):
        raise NotImplementedError()

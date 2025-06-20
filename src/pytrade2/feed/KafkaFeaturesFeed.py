import multiprocessing
from datetime import datetime
from typing import Dict

import pandas as pd

from pytrade2.exch.Exchange import Exchange


class KafkaFeaturesFeed:
    """" Provides precalculated features from Apache Kafka """

    kind = "features"

    def __init__(self, cfg: Dict[str, str], data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self.data_lock = data_lock
        self.new_data_event = new_data_event


    def run(self):
        ...


    def apply_buf(self):
        """ Add the buf to the data then clear the buf """

    def is_alive(self, maxdelta: pd.Timedelta):
        # todo: check
        return True

    def has_min_history(self):
        # todo: check if we have enough history
        return True

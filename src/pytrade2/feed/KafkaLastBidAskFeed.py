import multiprocessing


class KafkaLastBidAskFeed:
    """ Decorator for strategies. Reads candles from exchange """

    kind = "bidask"

    def __init__(self, config: dict[str, str], data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._features_topic = config['pytrade2.feed.bid_ask.kafka.topic']
        self._data_lock = data_lock
        self.last_bid_ask = None

    def apply_buf(self):
        """ Nothing to do"""
        ...

    def on_message(self, message):
        with self._data_lock:
            self.last_bid_ask = message


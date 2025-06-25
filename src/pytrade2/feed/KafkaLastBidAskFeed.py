import multiprocessing

from pytrade2.feed.KafkaFeedBase import KafkaFeedBase


class KafkaLastBidAskFeed(KafkaFeedBase):
    """ Decorator for strategies. Reads candles from exchange """

    kind = "bidask"

    def __init__(self, config: dict[str, str], data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._bid_ask_topic = config['pytrade2.feed.bid_ask.kafka.topic']
        super().__init__(self._bid_ask_topic, config, data_lock, new_data_event)

        self.last_bid_ask = None

    def apply_buf(self):
        """ Nothing to do"""
        ...

    def on_message(self, message):
        with self.data_lock:
            self.last_bid_ask = message


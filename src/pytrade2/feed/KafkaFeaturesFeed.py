import multiprocessing

from pytrade2.feed.KafkaFeedBase import KafkaFeedBase


class KafkaFeaturesFeed(KafkaFeedBase):
    """" Provides precalculated features from Apache Kafka """

    kind = "features"

    def __init__(self, config: dict[str, str], data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._features_topic = config['pytrade2.feed.features.kafka.topic']
        super().__init__(self._features_topic, config, data_lock, new_data_event)

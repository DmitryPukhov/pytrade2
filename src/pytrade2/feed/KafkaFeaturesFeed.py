import json
import logging
import multiprocessing
from threading import Thread
from typing import Dict

import pandas as pd
from confluent_kafka.cimpl import Consumer


class KafkaFeaturesFeed:
    """" Provides precalculated features from Apache Kafka """

    kind = "features"

    def __init__(self, config: dict[str, str], data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_lock = data_lock
        self.new_data_event = new_data_event
        kafka_conf = self._create_kafka_conf(config)
        self._consumer = Consumer(kafka_conf)
        self._features_topic = config['pytrade2.feed.features.kafka.topic']
        self._buf = []
        self.data = pd.DataFrame()
        self._logger.info(f"Features Feed created. Kafka topic: {self._features_topic}, bootstrap.servers: {kafka_conf.get('bootstrap.servers')}")

    def _create_kafka_conf(self, config: dict[str, str]):
        # kafka_conf = {
        #     'bootstrap.servers': cfg.get('KAFKA_BOOTSTRAP_SERVERS') or 'localhost:9092',  # Kafka broker(s)
        #     'group.id': cfg.get('pytrade2.feed.features.kafka.group_id') or self.__class__.__name__,  # Consumer group ID
        #     'auto.offset.reset': 'latest',  # Start from earliest if no offset
        # }

        kafka_conf = {
            'bootstrap.servers': config['pytrade2.feed.features.kafka.bootstrap.servers'],
            'group.id': self.__class__.__name__,  # Consumer group ID
            'auto.offset.reset': 'earliest',  # todo: change to latest
        }
        return kafka_conf

    def process_loop(self):
        self._logger.info(f"Subscribing to Kafka topic:{self._features_topic}")
        self._consumer.subscribe([self._features_topic])

        self._logger.info("Starting Kafka consumer loop")
        try:
            while True:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self._logger.error(f"Error: {msg.error()}")
                    continue
                else:
                    msg = json.loads(msg.value().decode('utf-8'))
                    self.on_message(msg)
                    self.new_data_event.set()

        except KeyboardInterrupt:
            self._consumer.close()

    def on_message(self, message):
        self._logger.debug(f"Received message: {message}")
        self._buf.append(message)

    def run(self):
        Thread(target=self.process_loop).start()

    def apply_buf(self):
        """ Add the buf to the data then clear the buf """
        with self.data_lock:
            self.data = self.data.append(self._buf)
            self._buf = []
            self.new_data_event.set()

    def is_alive(self, maxdelta: pd.Timedelta):

        return True

    def has_min_history(self):
        # Feature does not require history
        return True

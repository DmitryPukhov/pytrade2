import json
import logging
import multiprocessing
from threading import Thread
from typing import Optional

import pandas as pd
from confluent_kafka.cimpl import Consumer


class KafkaFeedBase:
    """" Read and accumulate messages from Kafka topic """

    def __init__(self, topic: str, config: dict[str, str], data_lock: multiprocessing.RLock,
                 new_data_event: multiprocessing.Event):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_lock = data_lock
        self._new_data_event = new_data_event
        self._kafka_conf = self._create_kafka_conf(config)
        self._consumer: Optional[Consumer] = None
        self._topic = topic
        self._buf = []
        self.data = pd.DataFrame()
        self._logger.info(
            f"Kafka feed created for topic: {self._topic}, bootstrap.servers: {self._kafka_conf.get('bootstrap.servers')}")

    def _create_kafka_conf(self, config: dict[str, str]):

        kafka_conf = {
            'bootstrap.servers': config.get('pytrade2.feed.kafka.bootstrap.servers', 'localhost:9092'),
            'group.id': self.__class__.__name__,  # Consumer group ID
            'auto.offset.reset': 'earliest',  # todo: change to latest
        }
        return kafka_conf

    def process_loop(self):
        self._consumer = Consumer(self._kafka_conf)

        self._logger.info(f"Subscribing to Kafka topic:{self._topic}")
        self._consumer.subscribe([self._topic])

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
                    self._on_message(msg)
                    self._new_data_event.set()

        except KeyboardInterrupt:
            self._consumer.close()

    def _on_message(self, message):
        self._logger.debug(f"Received message: {message}")
        if not message:
            return
        self._buf.append(message)
        self._new_data_event.set()

    def run(self):
        Thread(target=self.process_loop).start()

    def apply_buf(self):
        """ Add the buf to the data then clear the buf """
        with self.data_lock:
            new_df = pd.DataFrame(self._buf)
            self.data = pd.concat([self.data, new_df]) if not self.data.empty else new_df
            self._buf = []

    def is_alive(self, maxdelta: pd.Timedelta):
        # Children will overwrite
        return True

    def has_min_history(self):
        # Children will overwrite
        return True

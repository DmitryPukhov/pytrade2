import importlib
import logging

from collections import defaultdict
from typing import Dict

from exch.BrokerBase import BrokerBase


class Exchange:
    """ Provides binance or huobi exchange with broker and feed support"""

    def __init__(self, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.exchanges = defaultdict()

    def exchange(self, exch_name: str):
        """ Get or create given exchange with broker and feed """

        if exch_name not in self.exchanges:
            # Create exchange
            exchange_class_name = exch_name.split(".")[-1]
            exchange_file = f"exch.{exch_name}"
            self._log.info(f"Providing exchange: {exchange_file}")
            module = importlib.import_module(exchange_file, exchange_class_name)
            exchange_object = getattr(module, exchange_class_name)(config=self.config)
            self.exchanges[exch_name] = exchange_object

        # Get old or created exchange
        return self.exchanges[exch_name]

    def broker(self, exch_name: str) -> BrokerBase:
        """ Get or create broker for given exchange"""
        return self.exchange(exch_name).broker()

    def websocket_feed(self, exch_name: str):
        """ Get or create streaming feed for given exchange"""
        return self.exchange(exch_name).websocket_feed()

    def candles_feed(self, exch_name: str):
        """ Get or create candles feed for given exchange"""
        return self.exchange(exch_name).candles_feed()

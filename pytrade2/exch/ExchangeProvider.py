import importlib
import logging

from collections import defaultdict
from typing import Dict


class ExchangeProvider:
    """ Provides binance or huobi exchange with broker and feed support"""

    def __init__(self, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.exchanges = defaultdict()

    def exchange(self, name):
        """ Get or create given exchange with broker and feed """

        if name not in self.exchanges:
            # Create exchange
            exchange_class_name = "Exchange"
            exchange_file = f"exch.{name}.{exchange_class_name}"
            self._log.info(f"Providing exchange: {exchange_file}")
            module = importlib.import_module(exchange_file, exchange_class_name)
            exchange_object = getattr(module, exchange_class_name)(config=self.config)
            self.exchanges[name] = exchange_object

        # Get old or created exchange
        return self.exchanges[name]

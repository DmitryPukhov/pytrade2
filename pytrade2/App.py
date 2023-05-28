import importlib
import logging.config
import argparse
import os

import sys
from collections import defaultdict
from typing import Dict

import pandas as pd
import yaml
from binance.lib.utils import config_logging
from binance.spot import Spot as Client
from broker.BinanceBroker import BinanceBroker


class App:
    """
    Main application. Build strategy and run.
    """

    def __init__(self):
        # Suppress tensorflow log rubbish
        os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "1")
        config_logging(logging, "INFO")

        self._log = logging.getLogger(self.__class__.__name__)
        # For pandas printing to log
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)

        # Load config, set up logging
        self.config = self._load_config()

        # Init logging
        loglevel = self.config["log.level"]
        config_logging(logging, loglevel)
        self._log.info(f"Set log level to {loglevel}")

        # Create binance client
        self._init_client()

        # Init binance feed
        self.broker, self.strategy = None, None
        self._log.info("App initialized")

    def _read_config(self, path: str, required=False):
        if os.path.exists(path):
            with open(path, "r") as file:
                self._log.info(f"Reading config from {path}")
                conf = yaml.safe_load(file)
        elif required:
            sys.exit(f"Obligatory config {path} not found.")
        else:
            self._log.info(f"Config {path} not found, that could be ok.")
            conf = {}
        return conf

    def _load_config(self):
        """
        Load config from cfg folder respecting the order: defaults, app.yaml, environment vars
        """
        args = App._parse_args()
        strategy_key = "pytrade2.strategy"
        config: Dict[str, str] = defaultdict()

        # App configs
        app_config = self._read_config("cfg/app-defaults.yaml")
        app_config.update(self._read_config("cfg/app.yaml"))
        app_config.update(self._read_config("cfg/app-dev.yaml"))

        # Extra config if passed in command line
        extra_config = self._read_config(f"cfg/{args['config']}", required=True) \
            if args["config"] else defaultdict()

        # Determine strategy name
        final_strategy = args[strategy_key] or extra_config[strategy_key] or config[strategy_key]
        if not final_strategy:
            sys.exit("Please set pytrate2.strategy")
        # Read strategy config
        strategy_config = self._read_config(f"cfg/{final_strategy.lower()}.yaml")

        # Create final config in priority order
        config.update(app_config)
        config.update(strategy_config)
        config.update(extra_config)
        config.update(os.environ)
        config["pytrade2.strategy"] = final_strategy
        return config

    @staticmethod
    def _parse_args() -> Dict[str, str]:
        """ Parse command line arguments"""
        parser = argparse.ArgumentParser()
        parser.add_argument('--pytrade2.strategy',
                            help='Strategy class, example: --pytrade2.strategy SimpleKerasStraategy')
        parser.add_argument('--config', help='Additional config file')
        return vars(parser.parse_args())

    def _init_client(self):
        """ Binance spot client creation. Each strategy can be configured at it's own account"""
        strategy = self.config["pytrade2.strategy"].lower()
        self._log.info(f"Looking config for {strategy} key and secret")
        key, secret = self.config["pytrade2.connector.key"], self.config["pytrade2.connector.secret"]

        url = self.config["pytrade2.connector.url"]
        self._log.info(
            f"Init binance client for strategy: {strategy}, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
        self.client: Client = Client(key=key, secret=secret, base_url=url, timeout=10)

    def _create_strategy(self):
        """ Create strategy class"""

        strategy_file = f"strategy." + self.config["pytrade2.strategy"]
        strategy_class_name = strategy_file.split(".")[-1]
        self._log.info(f"Running the app with strategy from {strategy_file} import {strategy_class_name}")
        module = importlib.import_module(strategy_file, strategy_class_name)
        strategy = getattr(module, strategy_class_name)(broker=self.broker, config=self.config)
        return strategy

    def run(self):
        """
        Application entry point
        """
        self.broker = BinanceBroker(client=self.client, config=self.config)
        self.strategy = self._create_strategy()

        # Run and wait until the end
        self.strategy.run(self.client)

        self._log.info("The end")


if __name__ == "__main__":
    App().run()

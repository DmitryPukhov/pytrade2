import importlib
import logging.config
import argparse
import os

import sys
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
        self.config: Dict[str, str] = self._load_config()

        # Init logging
        loglevel = self.config["log.level"]
        config_logging(logging, loglevel)
        self._log.info(f"Set log level to {loglevel}")

        # Create binance client
        self._init_client()

        # Init binance feed
        self.broker, self.strategy = None, None
        self._log.info("App initialized")

    def _load_config_file(self, path: str):
        """ Update config with another from path"""
        if os.path.exists(path):
            with open(path, "r") as file:
                self._log.info(f"Reading config from {path}")
                conf = yaml.safe_load(file)
        else:
            self._log.info(f"Config {path} not found, that could be ok.")
            conf = {}
        return conf

    def _load_config(self):
        """
        Load config from cfg folder respecting the order: defaults, app.yaml, environment vars
        """
        args = App._parse_args()
        config = {}

        # Fill config files list in order
        files = ["cfg/app-defaults.yaml", "cfg/app.yaml", "cfg/app-dev.yaml"]

        # If strategy parameter set in args, add its config to files
        strategy_key = "biml.strategy"
        if args[strategy_key]:
            config[strategy_key] = args[strategy_key]
        path = f"cfg/{ config[strategy_key].lower()}.yaml"
        if os.path.exists(path):
            files.append(path)
        else:
            self._log.info(f"Strategy config {path} not found. Maybe the strategy does not require it.")

        # Read config files
        for file in files:
            config.update(self._load_config_file(file))

        # Enviroment variables
        config.update(os.environ)

        # Args
        config.update([(key, value) for (key, value) in args.items() if value])

        return config

    @staticmethod
    def _parse_args() -> Dict[str, str]:
        """ Parse command line arguments"""
        parser = argparse.ArgumentParser()
        parser.add_argument('--biml.strategy', help='Strategy class, example: --biml.strategy SimpleKerasStraategy')
        return vars(parser.parse_args())

    def _init_client(self):
        """ Binance spot client creation. Each strategy can be configured at it's own account"""
        strategy = self.config["biml.strategy"].lower()
        self._log.info(f"Looking config for {strategy} key and secret")
        key, secret = self.config["biml.connector.key"], self.config["biml.connector.secret"]

        url = self.config["biml.connector.url"]
        self._log.info(f"Init binance client for strategy: {strategy}, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
        self.client: Client = Client(key=key, secret=secret, base_url=url, timeout=10)

    def _create_strategy(self):
        """ Create strategy class"""

        strategy_file = f"strategy." + self.config["biml.strategy"]
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

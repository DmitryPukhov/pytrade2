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

    @staticmethod
    def _load_config():
        """
        Load config from cfg folder respecting the order: defaults, app.yaml, environment vars
        """
        # Defaults
        default_cfg_path = "cfg/app-defaults.yaml"
        with open(default_cfg_path, "r") as appdefaults:
            config = yaml.safe_load(appdefaults)

        # Custom config, should contain custom information,
        cfg_path = "cfg/app.yaml"
        if os.path.exists(cfg_path):
            with open(cfg_path) as app:
                config.update(yaml.safe_load(app))
        else:
            sys.exit(
                f"Config {cfg_path} not found. Please copy cfg/app-defaults.yaml to {cfg_path} "
                f"and update connection info there.")

        # Dev debugging config if needed
        dev_cfg_path = "cfg/app-dev.yaml"
        if os.path.exists(dev_cfg_path):
            with open(dev_cfg_path) as app:
                config.update(yaml.safe_load(app))
        else:
            print(f"{dev_cfg_path} not found, maybe it is not developer's run")

        # Enviroment variabless
        config.update(os.environ)

        # Parse arguments
        config.update(App._parse_args())
        return config

    @staticmethod
    def _parse_args() -> Dict[str, str]:
        """ Parse command line arguments"""
        parser = argparse.ArgumentParser()
        parser.add_argument('--biml.strategy', help='Strategy class name')
        return vars(parser.parse_args())

    def _init_client(self):
        """ Binance spot client creation. Each strategy can be configured at it's own account"""
        strategy = self.config["biml.strategy"].lower()
        self._log.info(f"Looking config for {strategy} key and secret")
        strategy_key_param = f"biml.connector.{strategy}.key"
        strategy_secret_param = f"biml.connector.{strategy}.secret"
        key_param = f"biml.connector.key"
        secret_param = f"biml.connector.secret"
        key, secret = None, None
        if {strategy_key_param, strategy_secret_param} <= self.config.keys():
            self._log.info(f"Found {strategy_key_param} and {strategy_secret_param} in config")
            key, secret = self.config[strategy_key_param], self.config[strategy_secret_param]
        else:
            self._log.info(f"{strategy_key_param} or {strategy_secret_param} not found in config. "
                           f"Using {key_param}, {secret_param}")
            key, secret = self.config[key_param], self.config[secret_param]

        url = self.config["biml.connector.url"]
        self._log.info(f"Init binance client for strategy: {strategy}, url: {url}")
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

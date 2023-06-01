import argparse
import importlib
import logging.config
import os
import sys
from collections import defaultdict
from typing import Dict

import pandas as pd
import yaml

from exch.Exchange import Exchange


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
        self.config = self._load_config()

        # Init logging
        loglevel = self.config["log.level"]
        logging.basicConfig(level=loglevel)
        self._log.info(f"Set log level to {loglevel}")
        self._log.info("App initialized")

    def _read_config_file(self, path: str, required=False):
        if os.path.exists(path):
            with open(path, "r") as file:
                print(f"Reading config from {path}")
                conf = yaml.safe_load(file)
        elif required:
            sys.exit(f"Obligatory config {path} not found.")
        else:
            print(f"Config {path} not found, that could be ok.")
            conf = {}
        return conf

    def _load_config(self)->dict:
        """
        Load config from cfg folder respecting the order: defaults, app.yaml, environment vars
        """
        args = App._parse_args()
        strategy_key = "pytrade2.strategy"
        config: Dict[str, str] = defaultdict()

        # App configs
        app_config = self._read_config_file("cfg/app-defaults.yaml")
        app_config.update(self._read_config_file("cfg/app.yaml"))
        app_config.update(self._read_config_file("cfg/app-dev.yaml"))

        # Extra config if passed in command line
        extra_config = self._read_config_file(f"cfg/{args['config']}", required=True) \
            if args["config"] else defaultdict()

        # Determine strategy name
        final_strategy = args.get(strategy_key) or extra_config.get(strategy_key) or app_config.get(strategy_key)
        if not final_strategy:
            sys.exit("Please set pytrade2.strategy")
        # Read strategy config
        strategy_config = self._read_config_file(f"cfg/{final_strategy.lower()}.yaml")

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

    def _create_strategy(self):
        """ Create strategy class"""
        exchange = Exchange(self.config)

        strategy_file = f"strategy." + self.config["pytrade2.strategy"]
        strategy_class_name = strategy_file.split(".")[-1]
        self._log.info(f"Running the strategy: {strategy_file}")
        module = importlib.import_module(strategy_file, strategy_class_name)
        strategy = getattr(module, strategy_class_name)(config = self.config, exchange_provider = exchange)
        return strategy

    def run(self):
        """
        Application entry point
        """
        self.strategy = self._create_strategy()

        # Run and wait until the end
        self.strategy.run()

        self._log.info("The end")


if __name__ == "__main__":
    App().run()

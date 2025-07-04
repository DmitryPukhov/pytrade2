import argparse
import importlib
import logging.config
import os
import signal
import sys
import threading
from collections import defaultdict
import time
from datetime import datetime
from typing import Dict

import pandas as pd
import yaml

from pytrade2.metrics.MetricServer import MetricServer

from pytrade2.exch.Exchange import Exchange
from pytrade2.metrics.Metrics import Metrics
from pytrade2.strategy.common.StrategyBase import StrategyBase


class App:
    """
    Main application. Build strategy and run.
    """

    def __init__(self):
        pd.options.mode.copy_on_write = True

        # Suppress tensorflow log rubbish
        os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "1")

        os.environ['TZ'] = 'UTC'
        time.tzset()
        self._init_logger()

        self._logger.info("\n--------------------------------------------------------------"
                          "\n--------------   Starting pytrade2 App   ---------------------"
                          "\n--------------------------------------------------------------")
        # For pandas printing to log
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)

        # Load config, set up logging
        self.config = {key:val for key, val in os.environ.items()}
        self._logger.info(self._config_msg(self.config))
        self.metrics_type = self.config.get("pytrade2.metrics.type", "push_to_gateway")

        self._logger.info("App initialized")

    def _init_logger(self):
        # Ensure logging directory exists
        # os.makedirs(logdir, exist_ok=True)
        cfgpaths = ["cfg/log.yaml", "cfg/log-dev.yaml"]
        for cfgpath in cfgpaths:
            cfgdict = self._read_config_file(cfgpath)
            print(f"Logging config: {cfgdict}")
            if cfgdict:
                logging.config.dictConfig(cfgdict)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info("Logging configured")

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

    def _load_config(self) -> dict:
        """
        Load config from cfg folder respecting the order: defaults, app.yaml, environment vars
        """
        args = self._parse_args()
        strategy_key = "pytrade2.strategy"
        config: Dict[str, str] = defaultdict()

        # App configs
        app_config = self._read_config_file("cfg/app-defaults.yaml")
        app_config.update(self._read_config_file("cfg/secret.yaml"))
        app_config.update(self._read_config_file("cfg/app.yaml"))
        app_config.update(self._read_config_file("cfg/app-dev.yaml"))

        # Extra config if passed in command line
        extra_config = self._read_config_file(f"cfg/{args['config']}", required=True) \
            if args.get("config") else defaultdict()

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
        config.update(args)
        config["pytrade2.strategy"] = final_strategy

        self._logger.info(self._config_msg(config))

        return config

    @staticmethod
    def _config_msg(config):
        """ Print config parameters to log
        """

        # def secured_key_val(key, value):
        #     if any([key.endswith(suffix) for suffix in [".secret", ".key", ".access_key", ".secret_key"]]):
        #         value = "***" + value[-3:]
        #     return (key, value)

        # secured_conf = [secured_key_val(key, config[key]) for key in sorted(config) if key.startswith("pytrade2")]
        secured_conf = App.secured_config(config)
        msg = "\n" + "\n".join([f"{key}: {val}" for key, val in secured_conf])
        return "Pytrade2 configuration: \n" + msg

    @staticmethod
    def secured_config(config):

        def secured_key_val(key, value):
            if any([key.endswith(suffix) for suffix in [".secret", ".key", ".access_key", ".secret_key"]]):
                value = "***" + value[-3:]
            return (key, value)

        return [secured_key_val(key, config[key]) for key in sorted(config) if key.startswith("pytrade2")]

    def _parse_args(self) -> Dict[str, str]:
        """ Parse command line arguments"""
        parser = argparse.ArgumentParser()
        parser.add_argument('--pytrade2.strategy',
                            help='Strategy class, example: --pytrade2.strategy SimpleKerasStraategy')
        parser.add_argument('--config', help='Additional config file')
        return vars(parser.parse_args())

    def _create_strategy(self) -> StrategyBase:
        """ Create strategy class"""
        exchange = Exchange(self.config)

        strategy_file = "strategy." + self.config["pytrade2.strategy"]
        strategy_class_name = strategy_file.split(".")[-1]
        self._logger.info(f"Running the strategy: {strategy_file}")
        module = importlib.import_module(strategy_file, strategy_class_name)
        strategy = getattr(module, strategy_class_name)(config=self.config, exchange_provider=exchange)
        return strategy

    def run(self):
        """
        Application entry point
        """
        self.strategy = self._create_strategy()

        # Create prometheus metrics endpoint
        MetricServer.metrics = Metrics("pytrade2", self.strategy.__class__.__name__)

        # Start metrics server or periodical push to gateway
        if self.metrics_type == "push_to_gateway":
            threading.Thread(target=MetricServer.metrics.push_to_gateway_periodical).start()
        else:
            MetricServer.app_config = {key: val for key, val in self.secured_config(self.config) if
                                       key.startswith("pytrade2")}
            MetricServer.auth_token = self.config.get("pytrade2.prometheus.token")
            MetricServer.start_http_server()

        # Watchdog starts after initial interval
        threading.Timer(60, self.watchdog_check).start()

        # Run and wait until the end
        self.strategy.run()

        self._logger.info("Started the app")

    def watchdog_check(self):
        """ If not alive, reset websocket feed"""
        if not self.strategy.is_alive():
            self._logger.error("Strategy seems to be dead, exiting")
            os.kill(os.getpid(), signal.SIGINT)
        else:
            self._logger.debug("Strategy is alive")
            # Schedule next check
            threading.Timer(60, self.watchdog_check).start()


if __name__ == "__main__":
    App().run()

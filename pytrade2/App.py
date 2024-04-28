import argparse
import importlib
import logging.config
import os
import signal
import sys
import threading
from collections import defaultdict
import time
from typing import Dict

import pandas as pd
import yaml

from metrics.Metrics import Metrics
from exch.Exchange import Exchange


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

        self._logger.info(f"\n--------------------------------------------------------------"
                       f"\n--------------   Starting pytrade2 App   ---------------------"
                       f"\n--------------------------------------------------------------")
        # For pandas printing to log
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)

        # Load config, set up logging
        self.config = self._load_config()

        self._log_report_interval_sec = 300
        self._logger.info("App initialized")

    def log_report(self):
        """ Periodically report to log"""

        # Header
        exchange_name = self.config["pytrade2.exchange"].split(".")[-1]
        strategy_name = self.config["pytrade2.strategy"]
        header = f"\n--------- {exchange_name} {strategy_name} report --------------"
        status = f"Strategy is alive: {self.strategy.is_alive()}"
        report = self.strategy.get_report()

        # Footer
        footer = "\n".rjust(len(header), "-")

        # Write to log
        self._logger.info("\n".join([header, status, report, footer]))

        # Schedule next report
        threading.Timer(self._log_report_interval_sec, self.log_report).start()

    def _init_logger(self):
        # Ensure logging directory exists
        # os.makedirs(logdir, exist_ok=True)
        cfgpaths = ["cfg/log.yaml", "cfg/log-dev.yaml"]
        for cfgpath in cfgpaths:
            cfgdict = self._read_config_file(cfgpath)
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
        """ Pring config parameters to log
        """
        def secured_key_val(key, value):
            if any([key.endswith(suffix) for suffix in [".secret", ".key", ".access_key", ".secret_key"]]):
                value = "***" + value[-3:]
            return (key, value)

        secured_conf = [secured_key_val(key, config[key]) for key in sorted(config) if key.startswith("pytrade2")]
        msg = "\n" + "\n".join([f"{key}: {val}" for key, val in secured_conf])
        return msg

    def _parse_args(self) -> Dict[str, str]:
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
        self._logger.info(f"Running the strategy: {strategy_file}")
        module = importlib.import_module(strategy_file, strategy_class_name)
        strategy = getattr(module, strategy_class_name)(config=self.config, exchange_provider=exchange)
        return strategy

    def run(self):
        """
        Application entry point
        """
        self.strategy = self._create_strategy()

        # Create metrics
        Metrics.auth_token = self.config.get("pytrade2.prometheus.token")
        Metrics.app_name = self.strategy.__class__.__name__
        Metrics.start_http_server()



        # Watchdog starts after initial interval
        threading.Timer(60, self.watchdog_check).start()
        threading.Timer(60, self.log_report).start()

        # Run and wait until the end
        self.strategy.run()

        self._logger.info("Started the app")

    def watchdog_check(self):
        """ If not alive, reset websocket feed"""
        if not self.strategy.is_alive():
            self._logger.error(f"Strategy seems to be dead, exiting")
            os.kill(os.getpid(), signal.SIGINT)
        else:
            self._logger.debug(f"Strategy is alive")
            # Schedule next check
            threading.Timer(60, self.watchdog_check).start()


if __name__ == "__main__":
    App().run()

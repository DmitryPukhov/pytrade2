import argparse
import importlib
import logging.config
import os
import signal
import sys
import threading
from collections import defaultdict
import time
from pprint import pprint
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

        os.environ['TZ'] = 'UTC'
        time.tzset()
        self._init_logger()
        
        logging.info(f"\n--------------------------------------------------------------"
                       f"\n--------------   Starting pytrade2 App   ---------------------"
                       f"\n--------------------------------------------------------------")
        # For pandas printing to log
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)

        # Load config, set up logging
        self.config = self._load_config()

        self._log_report_interval_sec = 60
        logging.info("App initialized")

    def log_report(self):
        """ Periodically report to log"""

        # Header
        exchange_name = self.config["pytrade2.exchange"].split(".")[-1]
        strategy_name = self.config["pytrade2.strategy"]
        header = f"\n--------- {exchange_name} {strategy_name} report --------------\n"

        report = self.strategy.get_report()

        # Footer
        footer = "\n".ljust(len(header), "-") + "\n"

        # Write to log
        logging.info(header + report + footer)

        # Schedule next report
        threading.Timer(self._log_report_interval_sec, self.log_report).start()

    def _init_logger(self):
        # Ensure logging directory exists
        # os.makedirs(logdir, exist_ok=True)
        cfgpaths = ["cfg/log.cfg", "cfg/log-dev.cfg"]
        for cfgpath in cfgpaths:
            if os.path.exists(cfgpath):
                logging.config.fileConfig(cfgpath)
                self._logger = logging.getLogger(__name__)
                self._logger.info(f"Logging configured from {cfgpath}")

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
        args = App._parse_args()
        strategy_key = "pytrade2.strategy"
        config: Dict[str, str] = defaultdict()

        # App configs
        app_config = self._read_config_file("cfg/app-defaults.yaml")
        app_config.update(self._read_config_file("cfg/secret.yaml"))
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

        logging.info(self._config_msg(config))

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
        logging.info(f"Running the strategy: {strategy_file}")
        module = importlib.import_module(strategy_file, strategy_class_name)
        strategy = getattr(module, strategy_class_name)(config=self.config, exchange_provider=exchange)
        return strategy

    def run(self):
        """
        Application entry point
        """
        self.strategy = self._create_strategy()

        self.log_report()

        # Watchdog starts after initial interval
        threading.Timer(60, self.watchdog_check).start()

        # Run and wait until the end
        self.strategy.run()

        logging.info("Started the app")

    def watchdog_check(self):
        """ If not alive, reset websocket feed"""
        if not self.strategy.is_alive():
            logging.error(f"Strategy seems to be dead, exiting")
            os.kill(os.getpid(), signal.SIGINT)
        else:
            logging.info(f"Strategy is alive")
            # Schedule next check
            threading.Timer(60, self.watchdog_check).start()


if __name__ == "__main__":
    App().run()

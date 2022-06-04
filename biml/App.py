import logging.config
import os
import sys
from typing import List
import yaml
from binance.lib.utils import config_logging
from binance.spot import Spot as Client
from feed.BinanceFeed import BinanceFeed
from feed.TickerInfo import TickerInfo


class App:
    """
    Main application. Build strategy and run.
    """

    def __init__(self):
        # Load config, set up logging
        self.config = self._load_config()

        # Init logging
        loglevel = self.config["log.level"]
        config_logging(logging, loglevel)  # self._init_logger(self.config['log.dir'])
        logging.info(f"Set log level to {loglevel}")

        # Create spot client
        key, url = self.config["biml.connector.key"], self.config["biml.connector.url"]
        logging.info(f"Init binance client, url: {url}")
        self.spot_client: Client = Client(key=key, base_url=url, timeout=10)

        # Init binance feed
        self.tickers = list(App.read_candle_config(self.config))
        self.feed = BinanceFeed(spot_client=self.spot_client, tickers=self.tickers)
        logging.info("App initialized")

    @staticmethod
    def read_candle_config(conf) -> List[TickerInfo]:
        """
        Read ticker infos from config
        """
        tickers = conf["biml.tickers"].split(',')
        for ticker in tickers:
            # biml.feed.BTCUSDT.candle.intervals: 1m,15m
            intervals = conf[f"biml.feed.{ticker}.candle.intervals"].split(",")
            limits = [int(limit) for limit in str(conf[f"biml.feed.{ticker}.candle.limits"]).split(",")]
            yield TickerInfo(ticker, intervals, limits)

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

        # Enviroment variabless
        config.update(os.environ)
        return config

    def run(self):
        """
        Application entry point
        """
        logging.info("Starting the app")

        # Read feed from binance
        self.feed.run()

        logging.info("The end")


if __name__ == "__main__":
    App().run()

import logging.config
import os
import sys
from typing import List
import pandas as pd
import yaml
from binance.lib.utils import config_logging
from binance.spot import Spot as Client

from broker.BinanceBroker import BinanceBroker
from feed.BinanceFeed import BinanceFeed
from feed.TickerInfo import TickerInfo
from strategy.FutureLowHigh import FutureLowHigh


class App:
    """
    Main application. Build strategy and run.
    """

    def __init__(self):
        # For pandas printing to log
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)

        # Load config, set up logging
        self.config = self._load_config()

        # Init logging
        loglevel = self.config["log.level"]
        config_logging(logging, loglevel)  # self._init_logger(self.config['log.dir'])
        logging.info(f"Set log level to {loglevel}")

        # Create spot client
        key, secret, url = self.config["biml.connector.key"], self.config["biml.connector.secret"], self.config[
            "biml.connector.url"]
        logging.info(f"Init binance client, url: {url}")
        self.client: Client = Client(key=key, secret=secret, base_url=url, timeout=10)

        # Init binance feed
        self.tickers = list(App.read_candle_config(self.config))
        self.feed, self.broker, self.strategy = None, None, None
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

        # Dev debugging config if needed
        dev_cfg_path="cfg/app-dev.yaml"
        if os.path.exists(dev_cfg_path):
            with open(dev_cfg_path) as app:
                config.update(yaml.safe_load(app))
        else:
            print(f"{dev_cfg_path} not found, maybe it is not developer's run")

        # Enviroment variabless
        config.update(os.environ)
        return config

    def run(self):
        """
        Application entry point
        """
        logging.info("Starting the app")
        self.feed = BinanceFeed(spot_client=self.client, tickers=self.tickers)
        self.broker = BinanceBroker(client = self.client)

        # Strategy
        self.strategy = FutureLowHigh(broker = self.broker, ticker=self.tickers[-1].ticker,
                                      model_dir=self.config["biml.model.dir"])
        self.feed.consumers.append(self.strategy)
        # Read feed from binance
        self.feed.run()

        # ticker = self.tickers[-1]
        # self.feed.emulate_feed(ticker.ticker, ticker.candle_intervals[-1], datetime.min, datetime.max)

        logging.info("The end")


if __name__ == "__main__":
    App().run()

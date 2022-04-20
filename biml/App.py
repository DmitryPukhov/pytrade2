import logging.config
import os
import sys
import yaml
from binance.lib.utils import config_logging
from binance.spot import Spot as Client

from biml.feed.BinanceFeed import BinanceFeed


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

        # Set ticker
        self.ticker = self.config["biml.ticker"]
        logging.info(f"Main ticker: {self.ticker}")

        # Create spot client
        key, url = self.config["biml.connector.key"], self.config["biml.connector.url"]
        logging.info(f"Init binance client, url: {url}")
        self.spot_client: Client = Client(key=key, base_url=url)

        # Create feed
        self.feed = BinanceFeed(self.spot_client, self.ticker,
                                self.config["biml.feed.read.interval"],
                                self.config["biml.feed.candle.fast.interval"],
                                self.config["biml.feed.candle.fast.limit"],
                                self.config["biml.feed.candle.medium.interval"],
                                self.config["biml.feed.candle.medium.limit"])
        logging.info("App initialized")

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
        logging.info(f"Fast candles:\n{self.feed.candles_fast.head()}")
        logging.info(f"Medium candles:\n{self.feed.candles_medium.head()}")

        logging.info("The end")


if __name__ == "__main__":
    App().run()

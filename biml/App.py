import logging.config
import os
import sys
import yaml
from binance.lib.utils import config_logging
from binance.spot import Spot as Client


class App:
    """
    Main application. Build strategy and run.
    """

    def __init__(self):
        # Load config, set up logging
        config = self._load_config()

        # Init logging
        loglevel = config["log.level"]
        config_logging(logging, loglevel)  # self._init_logger(self.config['log.dir'])
        logging.info(f"Set log level to {loglevel}")

        # Create spot client
        key, url = config["biml.connector.key"], config["biml.connector.url"]
        logging.info(f"Init binance client, url: {url}")
        self.spot_client: Client = Client(key=key, base_url=url)

        # Set ticker
        self.ticker = config["biml.ticker"]
        logging.info(f"Main ticker: {self.ticker}")

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

    def feed(self):
        """
        Get price data for configured ticker
        """
        # Get price history for configured ticker
        logging.info(f"Getting history for {self.ticker}")
        history = self.spot_client.historical_trades(symbol=self.ticker, limit=10)
        logging.info(history)
        # todo: save to csv for future use

    def main(self):
        """
        Application entry point
        """
        logging.info("Starting the app")
        self.feed()


if __name__ == "__main__":
    App().main()

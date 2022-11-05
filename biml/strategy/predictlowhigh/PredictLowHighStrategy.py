import logging
from pathlib import Path
from typing import Dict
from feed.BinanceWebsocketFeed import BinanceWebsocketFeed
from strategy.StrategyBase import StrategyBase


class PredictLowHighStrategy(StrategyBase):
    """
    Listen price data from web socket, predict future low/high
    """

    def __init__(self, broker, config: Dict):
        super().__init__(broker)
        self.config = config
        self.tickers = self.config["biml.tickers"].split(",")
        self.model_dir = self.config["biml.model.dir"]

        if self.model_dir:
            self.model_weights_dir = str(Path(self.model_dir, self.__class__.__name__, "weights"))
            self.model_Xy_dir = str(Path(self.model_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, client):
        """
        Attach to the feed and listen
        """
        feed = BinanceWebsocketFeed(tickers=self.tickers)
        feed.consumers.append(self)
        feed.run()

    def on_bid_ask(self, bid_ask: dict):
        logging.info(f"Got bid_ask {bid_ask}")
        pass



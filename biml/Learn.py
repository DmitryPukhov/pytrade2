import datetime
import logging
from App import App
from feed.LocalFeed import LocalFeed
from feed.LocalWriter import LocalWriter
from strategy.FutureLowHigh import FutureLowHigh


class Learn(App):
    """
    Learn on history
    """

    def __init__(self):
        super().__init__()
        self.data_dir = self.config["biml.data.dir"]

    def learn(self):
        logging.info(f"Learn, data dir: {self.data_dir}")
        # Run saved csv data from local folder
        history_feed = LocalFeed(self.data_dir, self.tickers)
        data = history_feed.read_intervals(None, None)

        strategy = FutureLowHigh()
        strategy.learn(data)


if __name__ == "__main__":
    Learn().learn()

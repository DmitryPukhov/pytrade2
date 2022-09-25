import logging
from App import App
from feed.BinanceFeed import BinanceFeed
from feed.LocalWriter import LocalWriter


class Downloader(App):
    """
    Download prices from binance to data folder
    """

    def __init__(self):
        super().__init__()
        self.data_dir = self.config["biml.data.dir"]

    def run(self):
        logging.info(f"Run downloader, data dir: {self.data_dir}")
        self.feed = BinanceFeed(spot_client=self.client, tickers=self.tickers)

        # Run binance feed with local writer consumer
        self.feed.consumers.append(LocalWriter(self.data_dir))
        self.feed.run()


if __name__ == "__main__":
    Downloader().run()

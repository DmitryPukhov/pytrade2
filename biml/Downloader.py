import logging

from biml.App import App


class Downloader(App):
    """
    Download prices from binance to data folder
    """

    def __init__(self):
        super().__init__()
        self.data_dir = self.config["biml.data.dir"]

    # def run(self, binance_feed: BinanceFeed, local_feed:LocalFeed,  ticker: str):
    def run(self):
        logging.info("Run downloader")
        self.f


if __name__ == "__main__":
    Downloader().run()

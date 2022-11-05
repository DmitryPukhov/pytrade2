import datetime
import logging
from App import App
from AppTools import AppTools
from feed.LocalFeed import LocalFeed
from strategy.predictlowhighcandles.PredictLowHighCandlesStrategy import PredictLowHighCandlesStrategy


class Learn(App):
    """
    Learn on history
    """

    def __init__(self):
        super().__init__()
        self.data_dir = self.config["biml.data.dir"]
        self.model_dir = self.config["biml.model.dir"]

    def learn(self):
        logging.info(f"Learn, data dir: {self.data_dir}")
        # Run saved csv data from local folder
        tickers = AppTools.read_candles_tickers(self.config)
        history_feed = LocalFeed(self.data_dir, tickers)

        start_time = datetime.datetime.now()-datetime.timedelta(days=7)
        data = history_feed.read_intervals(start_time, None)

        strategy = PredictLowHighCandlesStrategy(broker=None, config=self.config)
        strategy.learn(data)


if __name__ == "__main__":
    Learn().learn()

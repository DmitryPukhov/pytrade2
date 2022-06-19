import logging.config
import os
import sys
from datetime import datetime
from typing import List

import pandas as pd
import yaml
from binance.lib.utils import config_logging
from binance.spot import Spot as Client

from feed.BinanceFeed import BinanceFeed
from feed.LocalFeed import LocalFeed
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
        key, secret, url = self.config["biml.connector.key"], self.config["biml.connector.secred"], self.config[
            "biml.connector.url"]
        logging.info(f"Init binance client, url: {url}")
        self.client: Client = Client(key=key, secret=secret, base_url=url, timeout=10)

        # Init binance feed
        self.tickers = list(App.read_candle_config(self.config))
        self.feed = BinanceFeed(spot_client=self.client, tickers=self.tickers)
        #self.data_dir = self.config["biml.data.dir"]
        #self.feed = LocalFeed(self.data_dir, self.tickers)

        # Strategy
        self.strategy = FutureLowHigh(client=self.client, ticker=self.tickers[-1].ticker)
        self.feed.consumers.append(self.strategy)
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

        # ticker = self.tickers[-1]
        # self.feed.emulate_feed(ticker.ticker, ticker.candle_intervals[-1], datetime.min, datetime.max)

        logging.info("The end")
    #
    # def order(self, side: str, price: float):
    #     quantity = 0.001
    #     symbol = "BTCUSDT"
    #     stop_loss = .02
    #     trailing_delta = int(100 * stop_loss*100)  # trailing delta in points
    #
    #     if side == "BUY":
    #         stop_loss_price = price * (1 - stop_loss)
    #     elif side == "SELL":
    #         stop_loss_price = price * (1 + stop_loss)
    #     logging.info(f"Creating {side} order, symbol={symbol}, price={price}, stop_loss_price={stop_loss_price}, trailing_delta={trailing_delta}")
    #
    #     # Main order
    #     res = self.client.new_order_test(
    #         symbol=symbol,
    #         side=side,
    #         type="LIMIT",
    #         price=price,
    #         quantity=quantity,
    #         timeInForce="GTC")
    #     print(res)
    #     # Trailing stop loss order
    #     res = self.client.new_order_test(
    #         symbol=symbol,
    #         side=side,
    #         type='STOP_LOSS_LIMIT',
    #         quantity=quantity,
    #         price=stop_loss_price,
    #         stopPrice=price,
    #         trailingDelta=trailing_delta,
    #         timeInForce="GTC")
    #     print(res)
    #
    # def test(self):
    #     side = "BUY"
    #     quantity = 0.001
    #     symbol = "BTCUSDT"
    #     price = 21900
    #     trailing_delta = 100 * 2  # 2%
    #     self.order("BUY", 21900)
    #     # Buy
    #     # res = self.client.new_order(
    #     #     symbol="BTCUSDT",
    #     #     side="BUY",
    #     #     type='MARKET',
    #     #     quantity=0.001)
    #     # res = self.client.new_order_test(
    #     #     symbol=symbol,
    #     #     side=side,
    #     #     type='STOP_LOSS_LIMIT',
    #     #     quantity=quantity,
    #     #     price=price,
    #     #     stopPrice=price,
    #     #     trailingDelta=trailing_delta,
    #     #     timeInForce="GTC")
    #     #print(res)
    #     # print(res)
    #     # res = self.client.new_oco_order(
    #     #     symbol='BTCUSDT',
    #     #     side='SELL',
    #     #     quantity=0.001,
    #     #     price=22000,
    #     #
    #     #     stopPrice=21800,
    #     #     stopLimitPrice=21800,
    #     #     stopLimitTimeInForce='GTC')
    #     # print(self.client.exchange_info("BTCUSDT"))
    #     # print(self.client.my_trades(symbol='BTCUSDT'))
    #     # info = self.client.asset_detail(1)
    #     # self.client.account()
    #     # print(self.client.account())
    #     # print(self.client.account_status())
    #     # print(self.client.account_snapshot())
    #     # print(self.client.get_orders(symbol='BTCUSDT'))
    #     # print(self.client.cancel_order(symbol='BTCUSDT', orderId =1957295))
    #     # self.client.cancel_oco_order(symbol='BTCUSDT', orderId='1957295', clientOrderId = 'cFRgG8FNWl0a08vyHDAjv7')
    #     # self.client.cancel_oco_order(symbol='BTCUSDT', orderId= 1957295)


if __name__ == "__main__":
    App().run()

from typing import List

from feed.TickerInfo import TickerInfo


class AppTools:
    @staticmethod
    def read_candles_tickers(conf) -> List[TickerInfo]:
        """
        Read ticker infos from config
        """
        tickernames = conf["biml.tickers"].split(',')
        tickers = []
        for ticker in tickernames:
            # biml.feed.BTCUSDT.candle.intervals: 1m,15m
            intervals = conf[f"biml.feed.{ticker}.candle.intervals"].split(",")
            limits = [int(limit) for limit in str(conf[f"biml.feed.{ticker}.candle.limits"]).split(",")]
            tickers.append(TickerInfo(ticker, intervals, limits))
        return tickers

    @staticmethod
    def close_opened_posisions(broker, tickers):
        if broker:
            for ticker in tickers.strip().split("\\s*,\\s*"):
                broker.close_opened_positions(ticker)
                # Raise exception if we are in trade for this ticker
                AppTools.assert_out_of_market(broker, ticker)

    @staticmethod
    def assert_out_of_market(broker, ticker: str):
        """
        Raise exception if we have opened positions for the symbol
        """
        if broker:
            opened_quantity, opened_orders = broker.get_opened_positions(ticker)
            if opened_quantity or opened_orders:
                raise AssertionError(
                    f"Fatal: cannot trade. We have opened positions: {opened_quantity} {ticker} "
                    f"and {len(opened_orders)} opened {ticker} orders.")

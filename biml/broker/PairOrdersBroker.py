import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker

from broker.model.Trade import Trade


class PairOrdersBroker:
    """ Broker with idea of open/close orders and profit of each pair.
      Example: buy at 100, sell at 110, profit=10
      """

    def __init__(self):
        self._log = logging.getLogger(self.__class__.__name__)
        self.__init_db__()
        self.log_last_trades()
        self.cur_trade = self.get_cur_trade()

    def __init_db__(self):
        # Create database
        db_path = "../data/biml.db"
        self._log.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()
        self.cur_trade = self.get_cur_trade()

    def log_last_trades(self):
        self._log.info("Last trades:")
        self._log.info(self.db_session.query(Trade).order_by(Trade.open_time.desc()).limit(10).all())

    def get_cur_trade(self) -> Trade:
        """ Returns current opened trade, stored in db or none """
        return self.db_session \
            .query(Trade) \
            .where(Trade.close_time.is_(None)) \
            .order_by(Trade.open_time.desc()).first()

    def end_cur_trade(self, symbol: str) -> Trade:
        """
        If current opened trade exists,  close it, error otherwise
        """
        self._log.info(f"Close trade for {symbol}")
        assert self.cur_trade
        side = self.order_sides[-self.cur_trade.side]
        close_price = self.create_order(symbol=symbol, order_type=side, quantity=self.cur_trade.quantity, price=None,
                                        stop_loss=None)
        # Update stat db
        self.cur_trade.close_time = datetime.now()
        self.cur_trade.close_price = close_price
        self.db_session.flush()
        self.cur_trade, closed_trade = None, self.cur_trade
        return closed_trade

    def new_trade(self, symbol: str, side: int, quantity: float, price: float, stop_loss: float, take_profit: float):
        """
        Start new trade with new order
        """
        assert (not self.cur_trade)
        # Create order on stock exchange
        filled_price = self.create_order(symbol=symbol, order_type=side, quantity=quantity, price=price,
                                         stop_loss=stop_loss)
        # Insert trade info into db
        trade = Trade(ticker=symbol, side=side, open_time=datetime.datetime(), open_price=filled_price,
                      quantity=quantity)
        self._log.debug(f"Insert new trade into stat db: {trade}")
        self.db_session.execute(insert(trade))
        self.db_session.flush()
        self.cur_trade = trade

    # def close_trade(self, symbol: str):
    #     assert self.cur_trade
    #     filled_price = self.create_order(symbol=symbol, order_type=side, quantity=quantity, price=price,
    #                                      stop_loss=stop_loss)
    #     # Inserting new trade into db
    #     trade = Trade(ticker=symbol, side=side, open_time=datetime.datetime(), open_price=filled_price,
    #                   quantity=quantity)
    #     self._log.debug(f"Insert new trade into stat db: {trade}")
    #     self.db_session.execute(insert(trade))
    #     self.db_session.flush()

    def create_order(self, symbol: str, order_type: int, quantity: float, price: Optional[float],
                     stop_loss: Optional[float]) -> float:
        pass

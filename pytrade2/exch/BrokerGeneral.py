import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.Trade import Trade


class BrokerGeneral:
    """
    Orders management: buy, sell etc
    """

    def __init__(self, broker, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.allow_trade = config.get("pytrade2.broker.trade.allow", False)
        self.exch_broker = broker

        # Database
        self.__init_db__(config)

        # Load saved opened trade
        if self.allow_trade:
            self.cur_trade = self.read_last_opened_trade()
            if self.cur_trade:
                self._log.info(f"Loaded previously opened current trade: {self.cur_trade}")
                self.update_cur_trade_status()
        else:
            self.cur_trade = None
        self.price_precision = 2
        self.min_trade_interval = timedelta(seconds=10)
        self.last_trade_time = datetime.utcnow() - self.min_trade_interval
        self._log.info(f"Completed init broker. Allow trade: {self.allow_trade}")

    def __init_db__(self, config: Dict[str, str]):
        # Create database
        strategy = config["pytrade2.strategy"]
        data_dir = config["pytrade2.data.dir"] + "/" + strategy
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        db_path = f"{data_dir}/{strategy}.db"
        self._log.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        """
        Buy or sell with take profit and stop loss
        Binance does not support that in single order, so make 2 orders: main and stoploss/takeprofit
        """

        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return

        if (direction not in {1, -1}) or ((datetime.utcnow() - self.last_trade_time) <= self.min_trade_interval):
            # If signal is out of market or interval is not elapsed
            return

        # Prepare vars
        self.last_trade_time = datetime.utcnow()
        # side = self.order_side_names[direction]
        price = round(price, self.price_precision)
        stop_loss_price = round(stop_loss_price, self.price_precision)
        take_profit_price = round(take_profit_price, self.price_precision)

        # Main order
        try:
            self._log.info(
                f"Creating order. Asset:{symbol}  direction:{direction}, price: {price}  quantity: {quantity},"
                f" stop loss: {stop_loss_price}, take profit: {take_profit_price}")
            self.cur_trade = self.create_main_order(symbol=symbol,
                                                    direction=direction,
                                                    price=price,
                                                    quantity=quantity)
            if not self.cur_trade:
                return None
            # Adjust sl/tp to order filled price
            stop_loss_price_adj, take_profit_price_adj = self.adjusted_sl_tp(direction=direction,
                                                                             orig_price=price,
                                                                             orig_sl_price=stop_loss_price,
                                                                             orig_tp_price=take_profit_price,
                                                                             filled_price=self.cur_trade.open_price)
            if take_profit_price:
                # Stop loss and take profit
                sl_tp_trade = self.create_sl_tp_order(base_trade=self.cur_trade,
                                                         stop_loss_price=stop_loss_price_adj,
                                                         take_profit_price=take_profit_price_adj)
            else:
                # Stop loss without take profit
                sl_tp_trade = self.create_sl_order(base_trade=self.cur_trade,
                                                      stop_loss_price=stop_loss_price_adj)
            if not sl_tp_trade:
                raise "sl/tp not created"
            self.cur_trade = sl_tp_trade
        except Exception as e:
            # If sl/tp order exception, close main order
            logging.error(f"Order creation error: {e} ")
            if self.cur_trade:
                self.close_cur_trade()

        # Persist order to db
        self.db_session.add(self.cur_trade)
        self.db_session.commit()

        if not self.cur_trade.close_order_id:
            # If no error, order is not closed yet
            self._log.info(f"Created new trade: {self.cur_trade}")
        else:
            self.cur_trade = None
        return self.cur_trade

    def close_cur_trade(self):
        self.cur_trade = self.exch_broker.close_order(self.cur_trade)
        self._log.info(f"Closed current trade:{self.cur_trade}")

    def adjusted_sl_tp(self, direction, orig_price: float, orig_sl_price: float, orig_tp_price: float,
                       filled_price: float):
        """ Main order filled price can differ from original, so change original sl/tp to filled base price"""
        stop_loss_price_adj = round(float(filled_price - direction * abs(orig_price - orig_sl_price)),
                                    self.price_precision)
        take_profit_price_adj = round(float(filled_price + direction * abs(orig_tp_price - orig_price)),
                                      self.price_precision)
        return stop_loss_price_adj, take_profit_price_adj

    def create_main_order(self, symbol: str, direction: int, price: float, quantity: float) -> Trade:
        """ Main buy or sell order. Stop loss and take profit will be set later. """
        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return None

        trade = self.exch_broker.create_order(
            symbol=symbol,
            direction=direction,
            price=price,
            quantity=quantity
        )
        if trade:
            self._log.info(f"Created main order {trade}")
        else:
            self._log.info(f"Cannot create main {symbol} direction: {direction}, price: {price}, that's ok.")
        return trade

    def create_sl_tp_order(self, base_trade: Trade, stop_loss_price: float, take_profit_price: float) -> \
            Optional[Trade]:
        """
         After base order filled, create sl/tp oco order
         @:return order list id for sl/tp orders
         """
        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return None

        # Adjust stop loss, calc stop loss limit pric
        limit_ratio = 0.01  # 1# slippage to set stop loss limit
        stop_loss_limit_price = round(stop_loss_price - base_trade.direction() * base_trade.open_price * limit_ratio,
                                      self.price_precision)
        self._log.info(
            f"Creating stop loss and take profit order, base order: {base_trade.side} {base_trade.ticker}, base_price={base_trade.open_price}, stop_loss_adj={stop_loss_price},"
            f" stop_loss_limit_price={stop_loss_limit_price}, take_profit_adj={take_profit_price}")
        return self.exch_broker.create_sl_tp_order(base_trade=base_trade,
                                                   stop_loss_price=stop_loss_price,
                                                   stop_loss_limit_price=stop_loss_limit_price,
                                                   take_profit_price=take_profit_price)

    def create_sl_order(self, base_trade: Trade, stop_loss_price: float) -> Optional[Trade]:
        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return None

        # stop_loss_price = round(stop_loss_price, self.price_precision)
        stop_loss_limit_price = round(stop_loss_price - (base_trade.open_price - stop_loss_price), self.price_precision)

        self._log.info(
            f"Creating stop loss order. Base order: {base_trade.side} {base_trade.ticker}, "
            f"stop_loss={stop_loss_price}, stop_loss_limit_price={stop_loss_limit_price}")

        return self.exch_broker.create_sl_order(base_trade=base_trade,
                                                stop_loss_price=stop_loss_price,
                                                stop_loss_limit_price=stop_loss_limit_price)

    def close_opened_trades(self):
        self._log.info("Closing all opened trades if exist")
        # Query opened trades
        trades = self.db_session \
            .query(Trade) \
            .where(Trade.close_time.is_(None)) \
            .order_by(Trade.open_time.desc())
        # Close opened trades
        for trade in trades:
            self.close_trade(trade)

        # If current trade was already closed,
        if self.cur_trade and self.cur_trade.close_time:
            self._log.info(f"Current trade was closed: {self.cur_trade}")
            self.cur_trade = None

    def read_last_opened_trade(self) -> Trade:
        """ Returns current opened trade, stored in db or none """
        return self.db_session \
            .query(Trade) \
            .where(Trade.close_time.is_(None)) \
            .order_by(Trade.open_time.desc()).first()

    def update_cur_trade_status(self):
        if not self.cur_trade:
            return
        # Check if closed by sl/tp
        self.exch_broker.update_trade_status(self.cur_trade)

        if self.cur_trade.close_time:
            self._log.info(f"Current trade found closed, probably by stop loss or take profit: {self.cur_trade}")
            # If closed, save to db and clear current trade
            self.db_session.commit()
            self.cur_trade = None

    def close_trade(self, trade: Trade) -> Trade:
        """
        If current opened trade exists,  close it, error otherwise
        """
        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return trade

        assert trade
        self._log.info(f"Closing trade: {trade}")

        # Read trade orders from exchange, maybe closed by sl/tp
        trade = self.exch_broker.update_trade_status(trade)

        if not trade.close_time:
            # Close the trade
            trade = self.exch_broker.close_cur_trade(trade)
            self._log.info(f"Trade is already closed, probably by stop loss or take profit: {trade}")
        else:
            self._log.info(f"Trade is already closed, probably by stop loss or take profit: {trade}")
        self.db_session.commit()
        return trade

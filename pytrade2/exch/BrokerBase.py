import logging
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.Trade import Trade
from model.TradeStatus import TradeStatus


class BrokerBase:
    """
    Orders management: buy, sell etc
    """

    def __init__(self, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.price_precision = 2
        self.amount_precision = 2

        self.min_trade_interval = timedelta(seconds=10)
        self.last_trade_time = datetime.utcnow() - self.min_trade_interval

        self.allow_trade = config.get("pytrade2.broker.trade.allow", False)
        self._log.info(f"Allow trade: {self.allow_trade}")
        self.trade_lock: RLock = RLock()

        # Database
        self.__init_db__(config)

        # Load saved opened trade
        if self.allow_trade:
            self.cur_trade = self.read_last_opened_trade()
            if self.cur_trade:
                self._log.info(f"Loaded previously opened current trade: {self.cur_trade}")
                self.fix_cur_trade()
        else:
            self._log.info("Opened trades not found")
            self.cur_trade: Optional[Trade] = None
        self._log.info(f"Completed init broker.")

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
            self._log.debug("Trading is not allowed")
            return None

        with self.trade_lock:

            if self.cur_trade:
                self._log.error(f"Cannot create current trade: another current trade already exists: {self.cur_trade}")
                return None

            if not self.allow_trade:
                # self._log.debug(f"Trading is not allowed. "
                #                 f"{symbol} {Trade.order_side_names[direction]} order at {price} will not be executed.")
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
                self.cur_trade = self.create_main_trade_part(symbol=symbol,
                                                             direction=direction,
                                                             price=price,
                                                             quantity=quantity)
                if not self.cur_trade:
                    return None
                self.cur_trade.status = TradeStatus.opening
                # Adjust sl/tp to order filled price
                stop_loss_price_adj, take_profit_price_adj = self.adjusted_sl_tp(direction=direction,
                                                                                 orig_price=price,
                                                                                 orig_sl_price=stop_loss_price,
                                                                                 orig_tp_price=take_profit_price,
                                                                                 filled_price=self.cur_trade.open_price)
                if take_profit_price:
                    # Stop loss and take profit
                    sl_tp_trade = self.create_sl_tp_trade_part(base_trade=self.cur_trade,
                                                               stop_loss_price=stop_loss_price_adj,
                                                               take_profit_price=take_profit_price_adj)
                else:
                    # Stop loss without take profit
                    sl_tp_trade = self.create_sl_trade_part(base_trade=self.cur_trade,
                                                            stop_loss_price=stop_loss_price_adj)
                if not sl_tp_trade:
                    raise "sl/tp not created"
                sl_tp_trade.status = TradeStatus.opened
                self.cur_trade = sl_tp_trade
            except Exception as e:
                # If sl/tp order exception, close main order
                logging.error(f"Order creation error: {e} ")
                if self.cur_trade:
                    self.close_cur_trade()

            if self.cur_trade:
                # Persist created order to db
                self.db_session.add(self.cur_trade)
                self.db_session.commit()

                if not self.cur_trade.close_order_id:
                    # If order is not closed by error
                    self._log.info(f"Created new trade: {self.cur_trade}")
                # else:
                #     # If order is closed by error
                #     self.cur_trade = None

            return self.cur_trade

    def close_cur_trade(self):
        if not self.allow_trade:
            self._log.debug("Trading is not allowed")

        with self.trade_lock:
            try:
                self._log.info(f"Closing current trade:{self.cur_trade}")
                self.cur_trade.status = TradeStatus.closing

                # Call exchange broker to close the order
                self.cur_trade = self.create_closing_order(self.cur_trade)
                self.db_session.commit()
                if self.cur_trade.status == TradeStatus.closed:
                    self._log.info(f"Closed current trade:{self.cur_trade}")
                    self.cur_trade = None
                else:
                    self._log.debug(f"Current trade not closed immediately: {self.cur_trade}")

            except Exception as e:
                self._log.error(f"Cannot close cur trade {self.cur_trade}. Error: {e}")

    def adjusted_sl_tp(self, direction, orig_price: float, orig_sl_price: float, orig_tp_price: float,
                       filled_price: float):
        """ Main order filled price can differ from original, so change original sl/tp to filled base price"""
        stop_loss_price_adj = round(float(filled_price - direction * abs(orig_price - orig_sl_price)),
                                    self.price_precision)
        take_profit_price_adj = round(float(filled_price + direction * abs(orig_tp_price - orig_price)),
                                      self.price_precision)
        return stop_loss_price_adj, take_profit_price_adj

    def create_main_trade_part(self, symbol: str, direction: int, price: float, quantity: float) -> Optional[Trade]:
        """ Main buy or sell order. Stop loss and take profit will be set later. """
        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return None
        with self.trade_lock:
            trade = self.create_order(
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

    def create_sl_tp_trade_part(self, base_trade: Trade, stop_loss_price: float, take_profit_price: float) -> \
            Optional[Trade]:
        """
         After base order filled, create sl/tp oco order
         @:return order list id for sl/tp orders
         """
        if not self.allow_trade:
            self._log.info("Trading in not allowed")
            return None

        with self.trade_lock:
            # Adjust stop loss, calc stop loss limit pric
            limit_ratio = 0.01  # 1# slippage to set stop loss limit
            stop_loss_limit_price = round(
                stop_loss_price - base_trade.direction() * base_trade.open_price * limit_ratio,
                self.price_precision)
            self._log.info(
                f"Creating stop loss and take profit order, base order: {base_trade.side} {base_trade.ticker}, base_price={base_trade.open_price}, stop_loss_adj={stop_loss_price},"
                f" stop_loss_limit_price={stop_loss_limit_price}, take_profit_adj={take_profit_price}")
            return self.create_sl_tp_order(base_trade=base_trade,
                                           stop_loss_price=stop_loss_price,
                                           stop_loss_limit_price=stop_loss_limit_price,
                                           take_profit_price=take_profit_price)

    def create_sl_trade_part(self, base_trade: Trade, stop_loss_price: float) -> Optional[Trade]:
        if not self.allow_trade:
            self._log.debug("Trading is not allowed")
            return None

        with self.trade_lock:
            # stop_loss_price = round(stop_loss_price, self.price_precision)
            stop_loss_limit_price = round(stop_loss_price - (base_trade.open_price - stop_loss_price),
                                          self.price_precision)

            self._log.info(
                f"Creating stop loss order. Base order: {base_trade.side} {base_trade.ticker}, "
                f"stop_loss={stop_loss_price}, stop_loss_limit_price={stop_loss_limit_price}")

            return self.create_sl_order(base_trade=base_trade,
                                        stop_loss_price=stop_loss_price,
                                        stop_loss_limit_price=stop_loss_limit_price)

    def read_last_opened_trade(self) -> Trade:
        """ Returns current opened trade, stored in db or none """
        return self.db_session \
            .query(Trade) \
            .where(Trade.status.isnot(TradeStatus.closed)) \
            .order_by(Trade.open_time.desc()).first()

    def fix_cur_trade(self):
        """ Fix hung trade, cancel suborders, close the trade"""
        if not self.cur_trade:
            return
        with self.trade_lock:
            self.update_cur_trade_status()
            if self.cur_trade:
                if self.cur_trade.status != TradeStatus.opened:
                    self._log.info(f"Fixing bad trade: {self.cur_trade}")
                    if self.cur_trade.status == TradeStatus.closed:
                        self._log.info(f"Bad trade was already closed, maybe final update not received. "
                                       f"Just set closed status to the order.")
                        self.cur_trade.status = TradeStatus.closed
                        self.db_session.commit()
                        self.cur_trade = None
                    else:
                        self._log.info(f"Bad trade will be closed: {self.cur_trade}")
                        self.close_cur_trade()
                if self.cur_trade and self.cur_trade.status != TradeStatus.opened:
                    self._log.error(f"Cannot fix bad trade: {self.cur_trade}")

    def update_cur_trade_status(self):
        """ Update current trade sl/tp status from exchange.
        This func is called to check what's happened on exchange when the bot was turned off or disconnected.
        """
        raise NotImplementedError()

    def create_order(self, symbol, direction, price, quantity):
        raise NotImplementedError()

    def update_trade_status(self, trade):
        raise NotImplementedError()

    def create_sl_order(self, base_trade, stop_loss_price, stop_loss_limit_price):
        raise NotImplementedError()

    def create_sl_tp_order(self, base_trade, stop_loss_price, stop_loss_limit_price, take_profit_price):
        raise NotImplementedError()

    def create_closing_order(self, cur_trade):
        raise NotImplementedError()

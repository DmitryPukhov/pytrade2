import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from threading import RLock
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.Trade import Trade
from model.TradeStatus import TradeStatus


class Broker:
    def __init__(self, config: dict):
        self.price_precision = config["pytrade2.price.precision"]
        self.cur_trade: Optional[Trade] = None
        self.prev_trade: Optional[Trade] = None
        self.trade_lock: RLock = RLock()
        self.config = config
        self.amount_precision = config["pytrade2.amount.precision"]
        self._log = logging.getLogger(self.__class__.__name__)
        self.min_trade_interval = timedelta(seconds=10)
        self.last_trade_time = datetime.utcnow() - self.min_trade_interval
        self.allow_trade = config.get("pytrade2.broker.trade.allow", False)
        self.__init_db__(config)

        # Load saved opened trade
        self.cur_trade = self.read_last_opened_trade()
        if self.cur_trade:
            self._log.info(f"Loaded previously opened current trade: {self.cur_trade}")
            if self.allow_trade:
                self.fix_cur_trade()
        else:
            self._log.info("Opened trades not found")
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

    def adjusted_sl_tp(self, direction, orig_price: float, orig_sl_price: float, orig_tp_price: float,
                       filled_price: float):
        """ Main order filled price can differ from original, so change original sl/tp to filled base price"""
        stop_loss_price_adj = round(float(filled_price - direction * abs(orig_price - orig_sl_price)),
                                    self.price_precision)
        take_profit_price_adj = round(float(filled_price + direction * abs(orig_tp_price - orig_price)),
                                      self.price_precision)
        return stop_loss_price_adj, take_profit_price_adj

    def run(self):
        pass

    def get_report(self):
        """ Short info for report """

        # Form message string
        msg = StringIO()
        msg.write(f"Allow trade: {self.allow_trade}\n")

        # Opened trade
        msg.write(f"Current trade: {self.cur_trade}\n")
        return msg.getvalue()

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        raise NotImplementedError()

    def fix_cur_trade(self):
        pass

    def read_last_opened_trade(self) -> Trade:
        """ Returns current opened trade, stored in db or none """
        return self.db_session \
            .query(Trade) \
            .where(Trade.status.isnot(TradeStatus.closed)) \
            .order_by(Trade.open_time.desc()).first()

    def update_cur_trade_status(self):
        """ Update current trade sl/tp status from exchange.
        This func is called to check what's happened on exchange when the bot was turned off or disconnected.
        """
        raise NotImplementedError()

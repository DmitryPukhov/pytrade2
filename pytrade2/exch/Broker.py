import logging
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datamodel.Trade import Trade
from datamodel.TradeStatus import TradeStatus
from metrics.MetricServer import MetricServer


class Broker:
    def __init__(self, config: dict):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.price_precision = int(config["pytrade2.price.precision"])
        self.fee: float = 0
        self.cur_trade: Optional[Trade] = None
        self.prev_trade: Optional[Trade] = None
        self.trade_lock: RLock = RLock()
        self.config = config
        self.amount_precision = int(config["pytrade2.amount.precision"])

        self.min_trade_interval = timedelta(seconds=10)
        self.last_trade_time = datetime.utcnow() - self.min_trade_interval
        self.allow_trade = bool(str(config.get("pytrade2.broker.trade.allow", "False")).lower() == "true")
        self.__init_db__(config)

        # Load saved opened trade
        self.cur_trade = self.read_last_opened_trade()
        if self.cur_trade:
            self._logger.info(f"Loaded previously opened current trade: {self.cur_trade}")
            if self.allow_trade:
                self.fix_cur_trade()
            MetricServer.metrics.broker.trade.in_trade.set(self.cur_trade.direction())
        else:
            MetricServer.metrics.broker.trade.in_trade.set(0)
            self._logger.info("Opened trades not found")
        MetricServer.metrics.broker.trade.in_trade.set(bool(self.cur_trade))

        self._logger.info("Completed init broker.")

    def __init_db__(self, config: Dict[str, str]):
        # Create database
        strategy = config["pytrade2.strategy"]
        data_dir = config["pytrade2.data.dir"] + "/" + strategy
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        db_path = f"{data_dir}/{strategy}.db"
        self._logger.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()

    def run(self):
        pass

    def get_report(self):
        """ Short info for report """
        prefix = self.__class__.__name__.lower()
        return {
            f"{prefix}_trade_allow": {self.allow_trade},
            # Opened trade
            f"{prefix}_trade_current": {self.cur_trade},
            f"{prefix}_trade_prev": {self.prev_trade}}

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float],
                         trailing_delta: Optional[float]) -> Optional[Trade]:
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

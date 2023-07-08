import threading
from unittest.mock import MagicMock

from exch import AccountManagerBase
from exch.Broker import Broker
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm


class BrokerMocks:

    @staticmethod
    def init_db(self, config):
        self.db_session = MagicMock()

    @staticmethod
    def init(self, config):
        self.db_session = MagicMock()

    @staticmethod
    def broker_mock()-> HuobiBrokerHbdm:
        threading.Timer = MagicMock()
        # No database
        Broker.__init_db__ = BrokerMocks.init_db
        Broker.__init__ = BrokerMocks.init

        broker = HuobiBrokerHbdm(
            conf=MagicMock(),
            rest_client=MagicMock(),
            ws_feed=MagicMock(),
            ws_client=MagicMock()
        )
        broker.trade_lock = MagicMock()
        broker.account_manager = MagicMock()
        return broker

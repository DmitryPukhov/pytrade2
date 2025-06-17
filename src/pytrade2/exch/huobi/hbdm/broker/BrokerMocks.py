import threading
from unittest.mock import MagicMock, patch

from exch import AccountManagerBase
from pytrade2.exch.Broker import Broker
from pytrade2.exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from pytrade2.exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm


class BrokerMocks:

    @staticmethod
    def init_db(self, config):
        self.db_session = MagicMock()

    @staticmethod
    def init(self, config):
        self.db_session = MagicMock()
        self.allow_trade = True
        self.cur_trade = None

    @staticmethod
    def broker_mock()-> HuobiBrokerHbdm:
        threading.Timer = MagicMock()
        # No database
        Broker.__init_db__ = BrokerMocks.init_db
        patch("exch.Broker.__init__", new=BrokerMocks.init).start()

        broker = HuobiBrokerHbdm(
            conf=MagicMock(),
            rest_client=MagicMock(),
            ws_feed=MagicMock(),
            ws_client=MagicMock()
        )
        broker.trade_lock = MagicMock()
        broker.account_manager = MagicMock()
        return broker

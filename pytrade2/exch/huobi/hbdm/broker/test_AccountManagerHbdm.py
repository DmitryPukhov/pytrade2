from unittest import TestCase
from datetime import datetime, timezone

import pandas as pd

from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm
from datamodel.Trade import Trade


class TestHuobiBrokerHbdm(TestCase):

    def test_response_to_list(self):
        # Get balance response
        res = {'status': 'ok', 'data': [
            {'valuation_asset': 'asset1', 'balance': '1'},
            {'valuation_asset': 'asset2', 'balance': '2'}
        ], 'ts': 1687069745771}
        actual = list(AccountManagerHbdm.response_to_list(res))

        self.assertEqual(["asset1", "asset2"], [a["asset"] for a in actual])
        self.assertEqual([1, 2], [a["balance"] for a in actual])
        # Should be the same time for each record
        self.assertEqual(1, len(set([a["time"] for a in actual])))

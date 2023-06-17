import re


class HuobiFeedBase:

    @staticmethod
    def ticker_of_ch(ch):
        return re.match("market\\.([\\w\\-]*)\\..*", ch).group(1)

    @staticmethod
    def interval_of_ch(ch):
        return re.match("market\\.[\\w\\-]*\\.kline\\.(.*)", ch).group(1)

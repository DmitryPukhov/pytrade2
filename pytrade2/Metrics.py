from typing import Dict

from prometheus_client import start_http_server, Summary, Gauge, Counter


class Metrics:
    gauges: Dict[str, Gauge] = {}
    counters: Dict[str, Counter] = {}

    @staticmethod
    def start_http_server(port=8000):
        start_http_server(port)

    @classmethod
    def name_of(cls, source, suffix):
        return f'pytrade2_{source.__class__.__name__.lower()}_{suffix}'

    @classmethod
    def gauge(cls, source, suffix) -> Gauge:
        name = cls.name_of(source, suffix)
        if name not in cls.gauges:
            cls.gauges[name] = Gauge(name, name)
        return cls.gauges[name]

    @classmethod
    def counter(cls, source, suffix) -> Counter:
        name = cls.name_of(source, suffix)
        if name not in cls.counters:
            cls.counters[name] = Counter(name, name)
        return cls.counters[name]

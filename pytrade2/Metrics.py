import logging
import threading
from functools import wraps
from typing import Dict

from prometheus_client import Gauge, Counter
from flask import Flask, request, abort
from prometheus_client import make_wsgi_app


class Metrics:
    """Work with Prometheus metrics"""

    # token value to expect in header Authorization: Bearer <auth_token>
    auth_token = None

    # Metrics
    gauges: Dict[str, Gauge] = {}
    counters: Dict[str, Counter] = {}

    # Flask app to expose metrics endpoint
    app = Flask("pytrade2")
    prometheus_wsgi_app = make_wsgi_app()

    @staticmethod
    def require_api_token(func):
        """ Authorization to secure metrics endpoind. """

        @wraps(func)
        def check_token(*args, **kwargs):
            auth_token = request.headers.get("Authorization", "").lstrip("Bearer ")
            if (Metrics.auth_token != auth_token):
                # Kick off who not authorized
                abort(401)

            # Otherwise just send them where they wanted to go
            return func(*args, **kwargs)

        return check_token

    @staticmethod
    @app.route("/metrics")
    @require_api_token
    def metrics():
        """ Flask endpoint for metrics"""
        return Metrics.prometheus_wsgi_app

    @staticmethod
    def start_http_server():
        """ Start Flask thread to expose metrics to prometheus server."""
        threading.Thread(target=Metrics.app.run, kwargs={"host": "0.0.0.0", "port": 5000}).start()
        logging.info("Prometheus started")

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

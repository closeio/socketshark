import asyncio
from prometheus_async.aio.web import start_http_server
from prometheus_client import Gauge

class PrometheusMetrics:
    """
    Prometheus metrics provider.
    """
    def __init__(self, shark, config):
        self.ready_gauge = Gauge('service_state', 'Service status')
        self.connection_count = Gauge('connection_count', 'Connection count')
        self.event_success_counter = Gauge('event_success_counter', 'Event success counter')
        self.event_error_counter = Gauge('event_error_counter', 'Event error counter', ['event'])
        self.config = config
        assert 'port' in self.config

    def initialize(self):
        # Run Prometheus but don't fail hard if it doesn't start.
        asyncio.ensure_future(start_http_server(
            addr=self.config.get('host', ''),
            port=self.config['port']))

    def set_ready(self, ready):
        self.ready_gauge.set(int(ready))

    def set_connection_count(self, count):
        self.connection_count.set(count)

    def log_event(self, event, success):
        if success:
            self.event_success_counter.labels(event=event).inc()
        else:
            self.event_error_counter.labels(event=event).inc()

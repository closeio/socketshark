import asyncio

from prometheus_async.aio.web import start_http_server
from prometheus_client import Counter, Gauge


class PrometheusMetrics:
    """
    Prometheus metrics provider.
    """
    def __init__(self, shark, config):
        self.ready_gauge = Gauge('socketshark_service_state', 'Service status')
        self.active_connections_gauge = Gauge('socketshark_connection_count',
                                              'Active connections')
        self.connection_counter = Counter('socketshark_connection_total',
                                          'Connection total')
        self.event_counter = Gauge('socketshark_event_success_counter',
                                   'Event success counter', ['event', 'status'])

        self.config = config
        self.active_connections = 0
        assert 'port' in self.config

    def initialize(self):
        # Run Prometheus but don't fail hard if it doesn't start.
        asyncio.ensure_future(start_http_server(
            addr=self.config.get('host', ''),
            port=self.config['port']))

    def decrease_connection_count(self):
        self.active_connections -= 1
        self.active_connections_gauge.set(self.active_connections)

    def increase_connection_count(self):
        self.connection_counter.inc()
        self.active_connections += 1
        self.active_connections_gauge.set(self.active_connections)

    def set_ready(self, ready):
        self.ready_gauge.set(int(ready))

    def log_event(self, event, success):
        if success:
            self.event_counter.labels(event=event, status='success').inc()
        else:
            self.event_counter.labels(event=event, status='error').inc()

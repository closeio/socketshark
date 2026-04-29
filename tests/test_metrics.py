from unittest import mock

import pytest
from structlog.testing import capture_logs

from socketshark import SocketShark, config_defaults
from socketshark.metrics import Metrics
from socketshark.metrics.log import LogMetrics

TEST_CONFIG = {
    "BACKEND": "websockets",
    "WS_HOST": "127.0.0.1",
    "WS_PORT": 9001,
    "LOG": config_defaults.LOG,
    "METRICS": {},
    "REDIS": {
        "host": "127.0.0.1",
        "port": 6379,
        "channel_prefix": "",
    },
    "HTTP": {
        "timeout": 1,
        "tries": 1,
        "wait": 1,
    },
    "AUTHENTICATION": {},
    "SERVICES": {},
}


class TestMetrics:
    def test_it_supports_a_log_provider(self):
        config = TEST_CONFIG.copy()
        config["METRICS"] = {"log": {}}
        shark = SocketShark(config)
        assert shark.metrics.providers == {"log": mock.ANY}
        assert isinstance(shark.metrics.providers["log"], LogMetrics)

    def test_it_raises_for_an_unknown_provider(self):
        config = TEST_CONFIG.copy()
        config["METRICS"] = {"nonexistent": {}}
        with pytest.raises(ModuleNotFoundError):
            SocketShark(config)

    def test_it_works_if_no_providers_specified(self):
        shark = SocketShark(TEST_CONFIG)
        metrics = Metrics(shark)
        assert metrics.providers == {}


class TestLogMetrics:
    @pytest.fixture
    def log_metrics(self):
        config = TEST_CONFIG.copy()
        config["METRICS"] = {"log": {}}
        shark = SocketShark(config)
        return LogMetrics(shark, {})

    def test_it_logs_on_connection_count_increase(self, log_metrics):
        with capture_logs() as structlog_logs:
            log_metrics.increase_connection_count()
        assert structlog_logs == [
            {
                "log_level": "debug",
                "event": "metrics",
                "active_connections": 1,
                "total_connections": 1,
                "pid": mock.ANY,
            }
        ]

    def test_it_logs_on_connection_count_decrease(self, log_metrics):
        log_metrics.connection_count = 2
        log_metrics.active_connections = 2

        with capture_logs() as structlog_logs:
            log_metrics.decrease_connection_count()

        assert structlog_logs == [
            {
                "log_level": "debug",
                "event": "metrics",
                "active_connections": 1,
                "total_connections": 2,
                "pid": mock.ANY,
            }
        ]

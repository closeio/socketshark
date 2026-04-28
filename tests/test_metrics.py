from unittest import mock

import pytest

from socketshark import SocketShark, config_defaults
from socketshark.metrics import Metrics
from socketshark.metrics.log import LogMetrics

TEST_CONFIG = {
    'BACKEND': 'websockets',
    'WS_HOST': '127.0.0.1',
    'WS_PORT': 9001,
    'LOG': config_defaults.LOG,
    'METRICS': {},
    'REDIS': {
        'host': '127.0.0.1',
        'port': 6379,
        'channel_prefix': '',
    },
    'HTTP': {
        'timeout': 1,
        'tries': 1,
        'wait': 1,
    },
    'AUTHENTICATION': {},
    'SERVICES': {},
}


class TestMetrics:
    def test_it_supports_a_log_provider(self):
        config = TEST_CONFIG.copy()
        config['METRICS'] = {'log': {}}
        shark = SocketShark(config)
        assert shark.metrics.providers == {'log': mock.ANY}
        assert isinstance(shark.metrics.providers['log'], LogMetrics)

    def test_it_raises_for_an_unknown_provider(self):
        config = TEST_CONFIG.copy()
        config['METRICS'] = {'nonexistent': {}}
        with pytest.raises(ModuleNotFoundError):
            SocketShark(config)

    def test_it_works_if_no_providers_specified(self):
        shark = SocketShark(TEST_CONFIG)
        metrics = Metrics(shark)
        assert metrics.providers == {}

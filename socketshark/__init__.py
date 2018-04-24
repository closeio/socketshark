import asyncio
import importlib
import logging
import os
import signal
import ssl
import sys

import aioredis
from aioredis.pubsub import Receiver
import click
import structlog

from . import config_defaults
from .metrics import Metrics
from .receiver import ServiceReceiver


def setup_logging(log_config):
    # Configure root logger if logging level is specified in config
    if log_config['level']:
        level = getattr(logging, log_config['level'])
        formatter = logging.Formatter(log_config['format'])
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)

        logger = logging.getLogger()
        logger.setLevel(level)
        logger.addHandler(sh)

        trace_level = getattr(logging, log_config['trace_level'])
        trace_logger = logging.getLogger(log_config['trace_logger_prefix'])
        trace_logger.setLevel(trace_level)

    if log_config['setup_structlog']:
        setup_structlog(sys.stdout.isatty())


def setup_structlog(tty=False):
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt='iso', utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    if tty:
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def load_backend(config):
    """
    Returns the backend module from the given SocketShark configuration.
    """
    backend_name = config.get('BACKEND', 'websockets')
    backend_module = 'socketshark.backend.{}'.format(backend_name)
    return importlib.import_module(backend_module)


class SocketShark:
    def __init__(self, config):
        self.config = config
        backend_module = load_backend(config)
        backend_cls = backend_module.Backend
        self.backend = backend_cls(self)
        self._init_logging()
        self._task = None
        self._shutdown = False
        self.sessions = set()
        self.metrics = Metrics(self)
        self.metrics.initialize()
        self.metrics.set_ready(False)

    def _init_logging(self):
        logger_name = self.config['LOG']['logger_name']
        trace_logger_prefix = self.config['LOG']['trace_logger_prefix']
        trace_logger_name = '{}.{}'.format(trace_logger_prefix, logger_name)
        pid = os.getpid()
        self.log = structlog.get_logger(logger_name).bind(pid=pid)
        self.trace_log = structlog.get_logger(trace_logger_name).bind(pid=pid)
        self.trace_log.debug('trace')

    def signal_ready(self):
        """
        Called by the backend to notify that the backend is ready.
        """
        self.log.info('🦈  ready',
                      host=self.config['WS_HOST'],
                      port=self.config['WS_PORT'],
                      secure=bool(self.config.get('WS_SSL')))
        self.metrics.set_ready(True)

    def signal_shutdown(self):
        """
        Called by the backend to notify that the backend shut down.
        """
        self.log.info('done')
        self.metrics.set_ready(False)

    async def _redis_connection_handler(self):
        await self.redis.wait_closed()

        self.log.error('redis unexpectedly closed')
        self.metrics.set_ready(False)

        # Since we rely on PUBSUB channels, we disconnect all clients when
        # Redis goes down so they can reconnect and restore subscriptions.
        asyncio.ensure_future(self.shutdown())

    async def prepare(self):
        """
        Called by the backend to prepare SocketShark (i.e. initialize Redis
        connection and the receiver class)
        """
        redis_receiver = Receiver(loop=asyncio.get_event_loop())
        redis_settings = self.config['REDIS']
        try:
            self.redis = await aioredis.create_redis(
                (redis_settings['host'], redis_settings['port']),
                db=redis_settings.get('db', 0))
        except (OSError, aioredis.RedisError):
            self.log.exception('could not connect to redis')
            raise

        # Some features (e.g. pinging) don't work on old Redis versions.
        info = await self.redis.info('server')
        version_info = info['server']['redis_version'].split('.')
        major, minor = int(version_info[0]), int(version_info[1])
        if not (major > 3 or major == 3 and minor >= 2):
            msg = 'Redis version must be at least 3.2'
            self.log.exception(msg, version_info=version_info)
            raise Exception(msg)

        self._redis_connection_handler_task = asyncio.ensure_future(
                self._redis_connection_handler())

        self.service_receiver = ServiceReceiver(self, redis_receiver)

    def _cleanup(self):
        self._redis_connection_handler_task.cancel()
        self.redis.close()

    async def shutdown(self):
        """
        Cleanly shutdown SocketShark.
        """
        if self._shutdown:
            return

        self.log.info('shutting down')

        self._shutdown = True

        # Stop accepting new connections.
        self.backend.close()

        for session in self.sessions:
            asyncio.ensure_future(session.close())

        # In many cases (e.g. test cases or few open connections) we need
        # little time to close the connections (but yielding once with sleep(0)
        # is not enough)
        await asyncio.sleep(0.01)

        # Wait for all sessions to close
        while self.sessions:
            self.log.info('waiting for sessions to close',
                          n_sessions=len(self.sessions))
            await asyncio.sleep(1)

        await self.service_receiver.stop()

        if self._task:
            await asyncio.wait([self._task])
            self._task = None
            asyncio.get_event_loop().stop()

        self._cleanup()

        self._uninstall_signal_handlers()
        self._shutdown = False

    async def run_service_receiver(self, once=False):
        return await self.service_receiver.reader(once=once)

    def start(self):
        """
        Main entrypoint into SocketShark.
        """
        self.backend.start()

    async def _run(self, once=False):
        await self.run_service_receiver()
        asyncio.ensure_future(self.shutdown())

    async def run(self, once=False):
        """
        SocketShark main coroutine, invoked by the backend.
        """
        self._install_signal_handlers()
        self._task = asyncio.ensure_future(self._run())

    def _install_signal_handlers(self):
        """
        Sets up signal handlers for safely stopping the worker.
        """
        def request_stop():
            self.log.info('stop requested')
            asyncio.ensure_future(self.shutdown())

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, request_stop)
        loop.add_signal_handler(signal.SIGTERM, request_stop)

    def _uninstall_signal_handlers(self):
        """
        Restores default signal handlers.
        """
        loop = asyncio.get_event_loop()
        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)

    def get_ssl_context(self):
        ssl_settings = self.config.get('WS_SSL')
        if ssl_settings:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(certfile=ssl_settings['cert'],
                                        keyfile=ssl_settings['key'])
            return ssl_context


def load_config(config_name):
    config = {}

    # Get config defaults
    for key in dir(config_defaults):
        if key.isupper():
            config[key] = getattr(config_defaults, key)

    # Merge given config with defaults
    obj = importlib.import_module(config_name)
    for key in dir(obj):
        if key in config:
            value = getattr(obj, key)
            if isinstance(config[key], dict):
                config[key].update(value)
            else:
                config[key] = value

    return config


@click.command()
@click.option('-c', '--config', required=True, help='dotted path to config')
@click.pass_context
def run(context, config):
    config_obj = load_config(config)

    setup_logging(config_obj['LOG'])

    shark = SocketShark(config_obj)
    try:
        shark.start()
    except Exception:
        shark.log.exception('unhandled exception')
        raise

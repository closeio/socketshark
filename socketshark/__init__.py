import asyncio
import importlib
import logging
import os
import signal
import sys

import aioredis
from aioredis.pubsub import Receiver
import click
import structlog

from .receiver import ServiceReceiver


def setup_structlog(tty=False):
    processors = [
        structlog.stdlib.filter_by_level,
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


class SocketShark:
    def __init__(self, config):
        self.config = config
        self.log = structlog.get_logger().bind(pid=os.getpid())
        self._task = None
        self._shutdown = False
        self.sessions = set()

    def signal_ready(self):
        """
        Called by the backend to notify that the backend is ready.
        """
        self.log.info('🦈  ready', host=self.config['WS_HOST'],
                      port=self.config['WS_PORT'])

    def signal_shutdown(self):
        """
        Called by the backend to notify that the backend shut down.
        """
        self.log.info('done')

    async def _redis_connection_handler(self):
        await self.redis.wait_closed()

        self.log.error('redis unexpectedly closed')

        # Since we rely on PUBSUB channels, we disconnect all clients when
        # Redis goes down so they can reconnect and restore subscriptions.
        asyncio.ensure_future(self.shutdown())

    async def prepare(self):
        redis_receiver = Receiver(loop=asyncio.get_event_loop())
        redis_settings = self.config['REDIS']
        try:
            self.redis = await aioredis.create_redis((
                redis_settings['host'], redis_settings['port']))
        except (OSError, aioredis.RedisError):
            self.log.exception('could not connect to redis', exc_info=True)
            raise

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

        self._shutdown = True

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

        # If we're running the run loop, stopping the service receiver will
        # trigger a shutdown. Otherwise we need clean up explicitly.
        if self._task:
            await asyncio.wait([self._task])
            self._task = None
            asyncio.get_event_loop().stop()
        else:
            self._cleanup()

        self._uninstall_signal_handlers()
        self._shutdown = False

    async def run_service_receiver(self, once=False):
        return await self.service_receiver.reader(once=once)

    async def _run(self, once=False):
        await self.run_service_receiver()
        self._cleanup()

    async def run(self, once=False):
        """
        SocketShark main coroutine.
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


def load_config(config_name):
    obj = importlib.import_module(config_name)
    config = {}
    for key in dir(obj):
        if key.isupper():
            config[key] = getattr(obj, key)
    return config


def load_backend(config):
    backend_name = config.get('BACKEND', 'websockets')
    backend_module = 'socketshark.backend.{}'.format(backend_name)
    return importlib.import_module(backend_module)


@click.command()
@click.option('-c', '--config', required=True, help='dotted path to config')
@click.pass_context
def run(context, config):
    config_obj = load_config(config)
    backend = load_backend(config_obj)
    log_config = config_obj['LOG']
    level = getattr(logging, log_config['level'])
    logging.basicConfig(format=log_config['format'], level=level)
    setup_structlog(sys.stdout.isatty())
    shark = SocketShark(config_obj)
    backend.run(shark)

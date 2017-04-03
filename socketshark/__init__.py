import asyncio
import importlib
import logging
import os
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
        self.log.info('🦈  ready')

    async def prepare(self):
        redis_receiver = Receiver(loop=asyncio.get_event_loop())
        redis_settings = self.config['REDIS']
        self.redis = await aioredis.create_redis((
            redis_settings['host'], redis_settings['port']))

        self.service_receiver = ServiceReceiver(self.config, self.redis,
                                                redis_receiver)

    async def shutdown(self):
        self.redis.close()

    async def run_service_receiver(self):
        await self.service_receiver.reader()


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

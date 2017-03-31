import asyncio
import importlib

import aioredis
from aioredis.pubsub import Receiver
import click

from .receiver import ServiceReceiver


class SocketShark:
    def __init__(self, config):
        self.config = config

    async def prepare(self):
        redis_receiver = Receiver(loop=asyncio.get_event_loop())
        redis_settings = self.config['REDIS']
        self.redis = await aioredis.create_redis((
            redis_settings['host'], redis_settings['port']))

        self.service_receiver = ServiceReceiver(self.config, self.redis,
                redis_receiver)

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
    return importlib.import_module('socketshark.backend.{}'.format(backend_name))


@click.command()
@click.option('-c', '--config', help='dotted path to config')
@click.pass_context
def run(context, config):
    config_obj = load_config(config)
    backend = load_backend(config_obj)
    shark = SocketShark(config_obj)
    backend.run(shark)

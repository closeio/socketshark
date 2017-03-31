import aioredis
from aioredis.pubsub import Receiver
import asyncio
import json

import websockets

from ..session import Session


class Client:
    def __init__(self, shark, websocket):
        self.websocket = websocket
        self.session = Session(self, shark)

    async def consumer_handler(self):
        try:
            while True:
                event = await self.websocket.recv()
                print('EVENT', event)
                await self.session.on_client_event(json.loads(event))
        except websockets.ConnectionClosed:
            print('closed')
            # TODO: handle this

    async def send(self, event):
        await self.websocket.send(json.dumps(event))


def run(shark):
    async def serve(websocket, path):
        client = Client(shark, websocket)
        await client.consumer_handler()

    config = shark.config
    loop = asyncio.get_event_loop()
    loop.run_until_complete(shark.prepare())
    start_server = websockets.serve(serve, config['WS_HOST'],
            config['WS_PORT'])
    loop.run_until_complete(start_server)
    loop.run_until_complete(shark.run_service_receiver())
    loop.run_forever()

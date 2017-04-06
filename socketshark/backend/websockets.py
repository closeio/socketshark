import asyncio
import json

import websockets

from .. import constants as c
from ..session import Session


class Client:
    def __init__(self, shark, websocket):
        self.websocket = websocket
        self.session = Session(shark, self, info={
            'remote': websocket.remote_address,
        })

    async def consumer_handler(self):
        try:
            while True:
                event = await self.websocket.recv()
                try:
                    data = json.loads(event)
                except json.decoder.JSONDecodeError:
                    self.session.log.warn('received invalid json')
                    await self.send({
                        "status": "error",
                        "error": c.ERR_INVALID_EVENT,
                    })
                else:
                    await self.session.on_client_event(data)
        except websockets.ConnectionClosed:
            await self.session.on_close()

    async def send(self, event):
        try:
            await self.websocket.send(json.dumps(event))
        except websockets.ConnectionClosed:
            self.session.log.warn('attempted to send to closed socket')


def run(shark):
    async def serve(websocket, path):
        client = Client(shark, websocket)
        await client.consumer_handler()

    config = shark.config
    loop = asyncio.get_event_loop()
    loop.run_until_complete(shark.prepare())
    start_server = websockets.serve(serve, config['WS_HOST'], config['WS_PORT'])
    loop.run_until_complete(start_server)
    loop.run_until_complete(shark.run_service_receiver())
    loop.run_forever()

import asyncio
import json
import time

import websockets

from .. import constants as c
from ..session import Session


class Client:
    def __init__(self, shark, websocket):
        self.websocket = websocket
        self.session = Session(shark, self, info={
            'remote': websocket.remote_address,
        })
        self.shark = shark

    async def ping_timeout_handler(self, ping):
        ping_timeout = self.shark.config['WS_PING']['timeout']
        await asyncio.sleep(ping_timeout)
        if not ping.done():
            self.session.log.warn('ping timeout')
            await self.close()
            return True
        return False

    async def ping_handler(self):
        ping_interval = self.shark.config['WS_PING']['interval']
        if not ping_interval:
            return
        latency = 0
        while True:
            await asyncio.sleep(ping_interval - latency)
            self.session.log.debug('ping')
            start_time = time.time()
            try:
                ping = await self.websocket.ping()
            except websockets.ConnectionClosed:
                return
            timeout_handler = asyncio.ensure_future(
                    self.ping_timeout_handler(ping))
            await ping
            latency = time.time() - start_time
            self.session.log.debug('pong', latency=round(latency, 3))
            # Return immediately if a ping timeout occurred.
            if not timeout_handler.cancel() and timeout_handler.result():
                return

    async def consumer_handler(self):
        try:
            ping_handler = asyncio.ensure_future(self.ping_handler())
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

            ping_handler.cancel()
        except Exception:
            self.session.log.exception('unhandled error in consumer handler')

    async def send(self, event):
        try:
            await self.websocket.send(json.dumps(event))
        except websockets.ConnectionClosed:
            self.session.log.warn('attempted to send to closed socket')

    async def close(self):
        await self.websocket.close()


def run(shark):
    async def serve(websocket, path):
        client = Client(shark, websocket)
        await client.consumer_handler()

    async def shutdown_server():
        server.close()
        await server.wait_closed()

    config = shark.config
    loop = asyncio.get_event_loop()
    loop.run_until_complete(shark.prepare())
    ssl_context = shark.get_ssl_context()
    start_server = websockets.serve(serve,
                                    config['WS_HOST'],
                                    config['WS_PORT'],
                                    ssl=ssl_context)
    server = loop.run_until_complete(start_server)
    shark.signal_ready()
    loop.run_until_complete(shark.run())
    loop.run_forever()
    loop.run_until_complete(shutdown_server())
    shark.signal_shutdown()

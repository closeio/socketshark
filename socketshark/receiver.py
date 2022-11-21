import asyncio
import json
import time
from collections import defaultdict
from typing import Any, Optional


class ServiceReceiver:
    """
    Receives messages from services and forwards them to subscribing sessions.
    """

    def __init__(self, shark):
        self.shark = shark

        self.subscriptions = set()

        # {subscription: [sessions]}
        self.provisional_subscriptions = defaultdict(set)

        # {session: [msgs]}
        self.provisional_events = defaultdict(list)

        # {subscription: [sessions]}
        self.confirmed_subscriptions = defaultdict(set)

        self.redis_connections = shark.redis_connections
        self._stop = False  # Stop flag

    def _channel(self, redis_connection, name):
        return redis_connection.channel_prefix + name

    async def ping_handler(self):
        handlers = [self._ping_handler(c) for c in self.redis_connections]
        await asyncio.gather(*handlers)

    async def _ping_handler(self, redis_connection):
        ping_interval = redis_connection.ping_interval
        ping_timeout = redis_connection.ping_timeout
        if not ping_interval or not ping_timeout:
            return

        latency: float = 0
        ping: Optional[Any] = None
        wait: Optional[Any] = None

        try:
            while True:
                # Sleep before pings
                await asyncio.sleep(ping_interval - latency)

                self.shark.trace_log.debug('redis ping')

                start_time = time.time()

                ping = redis_connection.redis.ping()
                wait = asyncio.ensure_future(asyncio.sleep(ping_timeout))

                done, pending = await asyncio.wait(
                    [ping, wait], return_when=asyncio.FIRST_COMPLETED
                )

                if ping and ping in pending:
                    # Ping timeout
                    ping.cancel()
                    self.shark.log.warn('redis ping timeout')
                    self._stop = True
                    break

                latency = time.time() - start_time
                self.shark.trace_log.debug(
                    'redis pong', latency=round(latency, 3)
                )

        except asyncio.CancelledError:  # Cancelled by ping_handler.cancel()
            if ping:
                ping.cancel()
            if wait:
                wait.cancel()
            self.shark.log.debug('redis ping handler cancelled')
        except Exception:
            self.shark.log.exception('unhandled exception in ping handler')
            self._stop = True
        finally:
            if self._stop:
                await self.stop()

    async def reader(self, once=False):
        self._stop = False
        try:
            ping_handler = asyncio.ensure_future(self.ping_handler())
            tasks = [
                self._reader_for_connection(connection, once=once)
                for connection in self.redis_connections
            ]
            result = await asyncio.gather(*tasks)
            return result
        except Exception:
            self.shark.log.exception('unhandled exception in receiver')
        finally:
            ping_handler.cancel()

    async def _reader_for_connection(self, connection, once=False):
        prefix_length = len(connection.channel_prefix)
        if once and not connection.redis_receiver._queue.qsize():
            return False

        while True:
            data = await connection.redis_receiver.get()
            channel, msg = data
            if channel == connection.stop_channel:
                break
            subscription = channel.name.decode()[prefix_length:]
            try:
                data = json.loads(msg.decode())
                self.shark.trace_log.debug('service event', data=data)
                # The subscription arrays may change while executing
                # on_service_event. We therefore create a snapshot before
                # looping.
                confirmed_sessions = list(
                    self.confirmed_subscriptions[subscription]
                )
                provisional_sesssions = list(
                    self.provisional_subscriptions[subscription]
                )
                for session in confirmed_sessions:
                    await session.on_service_event(data)
                for session in provisional_sesssions:
                    self.provisional_events[session].append(data)
            except json.decoder.JSONDecodeError:
                self.shark.log.exception('JSONDecodeError')
            except Exception:
                self.shark.log.exception('unhandled exception in receiver')
            if once and not connection.redis_receiver._queue.qsize():
                return True

    async def add_provisional_subscription(self, session, subscription):
        if subscription not in self.subscriptions:
            self.subscriptions.add(subscription)
            await asyncio.gather(
                *[
                    c.redis.subscribe(
                        c.redis_receiver.channel(
                            self._channel(c, subscription)
                        )
                    )
                    for c in self.redis_connections
                ]
            )
        self.provisional_subscriptions[subscription].add(session)

    async def confirm_subscription(self, session, subscription):
        self.confirmed_subscriptions[subscription].add(session)
        self.provisional_subscriptions[subscription].remove(session)

        # Clear empty set
        if not self.provisional_subscriptions[subscription]:
            del self.provisional_subscriptions[subscription]

        # Flush provisional messages
        events = self.provisional_events.pop(session, [])
        for data in events:
            await session.on_service_event(data)

    async def delete_subscription(self, session, subscription):
        conf_set = self.confirmed_subscriptions[subscription]
        conf_set.discard(session)
        prov_set = self.provisional_subscriptions[subscription]
        prov_set.discard(session)

        # Clear empty set
        if not conf_set:
            del self.confirmed_subscriptions[subscription]
        if not prov_set:
            del self.provisional_subscriptions[subscription]

        if not conf_set and not prov_set:
            self.subscriptions.remove(subscription)
            await asyncio.gather(
                *[
                    c.redis.unsubscribe(
                        c.redis_receiver.channel(
                            self._channel(c, subscription)
                        )
                    )
                    for c in self.redis_connections
                    if not c.redis.closed
                ]
            )

    async def stop(self):
        self._stop = True
        for c in self.redis_connections:
            c.stop_channel.put_nowait(None)

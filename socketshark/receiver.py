import asyncio
from collections import defaultdict
import json
import time


class ServiceReceiver:
    """
    Receives messages from services and forwards them to subscribing sessions.
    """
    def __init__(self, shark, redis_receiver):
        self.shark = shark

        self.subscriptions = set()

        # {subscription: [sessions]}
        self.provisional_subscriptions = defaultdict(set)

        # {session: [msgs]}
        self.provisional_events = defaultdict(list)

        # {subscription: [sessions]}
        self.confirmed_subscriptions = defaultdict(set)

        self.redis_settings = shark.config['REDIS']
        self.redis_channel_prefix = self.redis_settings['channel_prefix']
        self.redis = shark.redis
        self.redis_receiver = redis_receiver

        # We use a special channel to pass the stop message to the reader.
        self._stop_channel = self.redis_receiver.channel('_internal')
        self._stop = False  # Stop flag

    def _channel(self, name):
        return self.redis_channel_prefix + name

    async def ping_handler(self):
        ping_interval = self.redis_settings['ping_interval']
        if not ping_interval:
            return
        latency = 0
        try:
            while True:
                self.shark.log.debug('redis ping')
                start_time = time.time()
                ping = self.redis.ping()
                timeout_handler = asyncio.ensure_future(
                        self.ping_timeout_handler(ping))
                try:
                    await ping
                except asyncio.CancelledError:
                    if self._stop:  # Cancelled by shutdown
                        raise  # Caught below
                    else:  # Cancelled by timeout handler
                        break  # Report timeout

                latency = time.time() - start_time
                self.shark.log.debug('redis pong', latency=round(latency, 3))

                # Return immediately if a ping timeout occurred.
                if not timeout_handler.cancel() and timeout_handler.result():
                    break

                # Sleep in between pings
                await asyncio.sleep(ping_interval - latency)

            # Ping timeout
            self.shark.log.warn('redis ping timeout')
        except asyncio.CancelledError:  # Cancelled by stop()
            if not self._stop:
                self.shark.log.exception('unhandled exception in ping handler')
        except Exception:
            self.shark.log.exception('unhandled exception in ping handler')

        await self.stop()

    async def ping_timeout_handler(self, ping):
        ping_timeout = self.redis_settings['ping_timeout']
        await asyncio.sleep(ping_timeout)
        if not ping.done():
            ping.cancel()
            return True
        return False

    async def reader(self, once=False):
        self._stop = False
        try:
            ping_handler = asyncio.ensure_future(self.ping_handler())
            result = await self._reader(once=once)
            ping_handler.cancel()
            return result
        except Exception:
            self.shark.log.exception('unhandled exception in receiver')
        else:
            self.redis_receiver.stop()

    async def _reader(self, once=False):
        prefix_length = len(self.redis_channel_prefix)
        if once and not self.redis_receiver._queue.qsize():
            return False

        while True:
            data = await self.redis_receiver.get()
            channel, msg = data
            if channel == self._stop_channel:
                break
            subscription = channel.name.decode()[prefix_length:]
            try:
                data = json.loads(msg.decode())
                for session in self.confirmed_subscriptions[subscription]:
                    await session.on_service_event(data)
                for session in self.provisional_subscriptions[subscription]:
                    self.provisional_events[session].append(data)
            except json.decoder.JSONDecodeError:
                self.shark.log.exception('JSONDecodeError')
            except Exception:
                self.shark.log.exception('unhandled exception in receiver')
            if once and not self.redis_receiver._queue.qsize():
                return True

    async def add_provisional_subscription(self, session, subscription):
        if subscription not in self.subscriptions:
            self.subscriptions.add(subscription)
            await self.redis.subscribe(self.redis_receiver.channel(
                self._channel(subscription)))
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
            if not self.redis.closed:
                await self.redis.unsubscribe(self._channel(subscription))

    async def stop(self):
        self._stop = True
        self._stop_channel.put_nowait(None)

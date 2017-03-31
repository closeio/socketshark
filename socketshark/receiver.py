from collections import defaultdict
import json


class ServiceReceiver:
    """
    Receives messages from services and forwards them to subscribing sessions.
    """
    def __init__(self, config, redis, redis_receiver):
        self.subscriptions = set()

        # {subscription: [sessions]}
        self.provisional_subscriptions = defaultdict(set)

        # {session: [msgs]}
        self.provisional_events = defaultdict(list)

        # {subscription: [sessions]}
        self.confirmed_subscriptions = defaultdict(set)

        redis_settings = config['REDIS']
        self.redis_channel_prefix = redis_settings['channel_prefix']
        self.redis = redis
        self.redis_receiver = redis_receiver

    def _channel(self, name):
        return self.redis_channel_prefix + name

    async def reader(self):
        prefix_length = len(self.redis_channel_prefix)
        while (await self.redis_receiver.wait_message()):
            channel, msg = await self.redis_receiver.get()
            subscription = channel.name.decode()[prefix_length:]
            try:
                data = json.loads(msg.decode())
                for session in self.confirmed_subscriptions[subscription]:
                    await session.on_service_event(data)
                for session in self.provisional_subscriptions[subscription]:
                    self.provisional_events[session].append(data)
            except json.decoder.JSONDecodeError:
                print('JSON decode error')
        # TODO: handle other exceptions here

    async def add_provisional_subscription(self, session, subscription):
        if subscription not in self.subscriptions:
            self.subscriptions.add(subscription)
            await self.redis.subscribe(self.redis_receiver.channel(
                self._channel(subscription)))
        self.provisional_subscriptions[subscription].add(session)

    async def confirm_subscription(self, session, subscription):
        self.confirmed_subscriptions[subscription].add(session)
        self.provisional_subscriptions[subscription].remove(session)

        # Flush provisional messages
        events = self.provisional_events.pop(session, [])
        for data in events:
            await session.on_service_event(data)

    async def delete_subscription(self, session, subscription):
        conf_set = self.confirmed_subscriptions[subscription]
        conf_set.discard(session)
        prov_set = self.provisional_subscriptions[subscription]
        prov_set.discard(session)

        if not conf_set and not prov_set:
            self.subscriptions.remove(subscription)
            await self.redis.unsubscribe(self._channel(subscription))

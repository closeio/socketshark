from . import constants as c
from .events import Event, InvalidEvent, UnknownEvent


class Session:
    """
    Represents a client session
    """
    def __init__(self, shark, client, info={}):
        """
        Initialize a session with
        - `shark`: a SocketShark instance,
        - `client`: a Websocket-backend-specific client object which implements
                    an async send() method that takes a JSON dict, and
        - `info`: a dict with any other information that should be logged (e.g.
                  the client's remote address).
        """
        self.auth_info = {}
        self.shark = shark
        self.config = shark.config
        self.client = client
        self.log = self.shark.log.bind(session=id(self))
        self.log.debug('new session', **info)
        self.subscriptions = {}  # dict of Subscription objects by name
        self.active = True
        shark.sessions.add(self)
        shark.metrics.increase_connection_count()

    async def on_client_event(self, data):
        """
        Called by the WebSocket backend when a new client messages comes in.o
        Expects a JSON dict.
        """
        assert self.active
        self.log.debug('client event', data=data)
        event = Event.from_data(self, data)
        try:
            result = await event.full_process()

            # Don't log invalid/unknown event names
            if isinstance(event, InvalidEvent):
                event_name = 'invalid'
            elif isinstance(event, UnknownEvent):
                event_name = 'unknown'
            else:
                event_name = event.event
            self.shark.metrics.log_event(event_name, result)
        except:
            self.shark.log.exception('unhandled event processing exception')
            await event.send_error(c.ERR_UNHANDLED_EXCEPTION)
            await self.close()

    async def on_service_event(self, data):
        """
        Called by the ServiceReceiver with a JSON dict on messages published by
        a service.
        """
        # Don't attempt to process messages to closed connections.
        if not self.active:
            return

        self.log.debug('service event', data=data)

        # Filter by comparing filter_fields to auth_info
        subscription_name = data['subscription']
        subscription = self.subscriptions.get(subscription_name)
        if not subscription:
            return

        filter_fields = subscription.service_config.get('filter_fields', [])
        for field in filter_fields:
            if field in data:
                if self.auth_info.get(field) != data[field]:
                    # Message filtered.
                    return

        msg = {
            'event': 'message',
            'subscription': subscription.name,
            'data': data['data'],
        }
        msg.update(subscription.extra_data)
        await self.send(msg)

    async def send(self, data):
        """
        Sends a JSON message to the client.
        """
        await self.client.send(data)

    async def close(self):
        if self.active:
            self.log.info('closing connection')
            self.active = False
            await self.client.close()
        else:
            self.log.info('connection already closing')

    async def on_close(self):
        """
        Called by the WebSocket backend to indicate the connection was closed.
        """
        self.active = False
        self.log.info('connection closed')
        await self.unsubscribe_all()
        self.shark.sessions.remove(self)
        self.shark.metrics.decrease_connection_count()

    async def unsubscribe_all(self):
        """
        Force-unsubscribe all subscriptions of the session.
        """
        while self.subscriptions:
            name, subscription = self.subscriptions.popitem()
            await subscription.force_unsubscribe()

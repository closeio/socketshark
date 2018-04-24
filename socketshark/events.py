from . import constants as c
from .exceptions import EventError
from .subscription import Subscription
from .utils import http_post


class Event:
    @classmethod
    def from_data(cls, session, data):
        if not isinstance(data, dict) or 'event' not in data:
            return InvalidEvent(session)

        event = data['event']

        # Make sure we don't echo back large messages.
        if not isinstance(event, str) or len(event) > c.MAX_EVENT_LENGTH:
            return InvalidEvent(session)

        cls = {
            'auth': AuthEvent,
            'message': MessageEvent,
            'subscribe': SubscribeEvent,
            'unsubscribe': UnsubscribeEvent,
        }.get(event)

        if cls:
            return cls(session, data)
        else:
            return UnknownEvent(session, data)

    def __init__(self, session, data):
        self.config = session.config
        self.data = data
        self.event = data['event']
        self.extra_data = {}
        self.session = session
        self.shark = session.shark

    async def send_error(self, error, data=None, extra_data={}):
        msg = {
            'event': self.event,
            'status': 'error',
            'error': error
        }
        msg.update(self.extra_data)
        if data is not None:
            msg['data'] = data
        msg.update(extra_data)
        await self.session.send(msg)

    async def send_ok(self, data=None, extra_data={}):
        msg = {
            'event': self.event,
            'status': 'ok',
        }
        msg.update(self.extra_data)
        if data is not None:
            msg['data'] = data
        msg.update(extra_data)
        await self.session.send(msg)

    async def full_process(self):
        """
        Fully process an event and return whether it was successful.
        """
        try:
            return await self.process()
        except EventError as e:
            await self.send_error(e.error, data=e.data)
            return False


class InvalidEvent(Event):
    def __init__(self, session):
        self.session = session
        self.event = None
        self.extra_data = {}

    async def full_process(self):
        msg = {
            'status': 'error',
            'error': c.ERR_INVALID_EVENT,
        }
        await self.session.send(msg)
        return False


class UnknownEvent(Event):
    async def process(self):
        raise EventError(c.ERR_EVENT_NOT_FOUND)


class AuthEvent(Event):
    def __init__(self, session, data):
        super().__init__(session, data)
        self.auth_config = self.config['AUTHENTICATION']
        self.method = data.get('method', c.DEFAULT_AUTH_METHOD)

    async def process(self):
        if self.method not in self.auth_config:
            raise EventError(c.ERR_AUTH_UNSUPPORTED)

        # The only supported method.
        assert self.method == 'ticket'

        auth_method_config = self.auth_config[self.method]

        ticket = self.data.get('ticket')
        if not ticket:
            raise EventError(c.ERR_NEEDS_TICKET)

        auth_url = auth_method_config['validation_url']
        auth_fields = auth_method_config['auth_fields']
        result = await http_post(self.shark, auth_url, {'ticket': ticket})
        if result.get('status') != 'ok':
            raise EventError(result.get('error', c.ERR_AUTH_FAILED))
        auth_info = {field: result[field] for field in auth_fields}
        self.session.auth_info = auth_info
        self.session.log.debug('auth info', auth_info=auth_info)
        await self.send_ok()
        return True


class SubscriptionEvent(Event):
    def __init__(self, session, data):
        super().__init__(session, data)
        subscription_name = data.get('subscription') or None
        self.subscription_name = subscription_name
        if subscription_name in self.session.subscriptions:
            self.subscription = self.session.subscriptions[subscription_name]
        else:
            self.subscription = Subscription(self.config, session, data)
        self.extra_data = self.subscription.extra_data

    async def send_error(self, error, data=None):
        await super().send_error(error, data=data, extra_data={
            'subscription': self.subscription_name,
        } if self.subscription_name else {})

    async def send_ok(self, data=None):
        await super().send_ok(data=data, extra_data={
            'subscription': self.subscription_name,
        } if self.subscription_name else {})

    async def process(self):
        self.subscription.validate()
        return True


class SubscribeEvent(SubscriptionEvent):
    async def process(self):
        await super().process()
        await self.subscription.subscribe(self)
        return True


class MessageEvent(SubscriptionEvent):
    async def process(self):
        await super().process()
        await self.subscription.message(self)
        return True


class UnsubscribeEvent(SubscriptionEvent):
    async def process(self):
        await super().process()
        await self.subscription.unsubscribe(self)
        return True

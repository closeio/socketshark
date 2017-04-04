from . import constants as c
from .utils import http_post


class EventError(Exception):
    pass


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

    async def send_error(self, error):
        msg = {
            'event': self.event,
            'status': 'error',
            'error': error
        }
        msg.update(self.extra_data)
        await self.session.send(msg)

    async def send_ok(self, data=None):
        msg = {
            'event': self.event,
            'status': 'ok',
        }
        msg.update(self.extra_data)
        if data is not None:
            msg['data'] = data
        await self.session.send(msg)

    async def full_process(self):
        try:
            await self.process()
        except EventError as e:
            await self.send_error(str(e))
        except:
            self.shark.log.exception('unhandled exception', exc_info=True)
            await self.send_error(c.ERR_UNHANDLED_EXCEPTION)
            raise


class InvalidEvent:
    def __init__(self, session):
        self.session = session

    async def full_process(self):
        msg = {
            'status': 'error',
            'error': c.ERR_INVALID_EVENT,
        }
        await self.session.send(msg)


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
        await self.send_ok()


class SubscriptionEvent(Event):
    def __init__(self, session, data):
        super().__init__(session, data)
        self.subscription = data.get('subscription') or ''
        if '.' in self.subscription:
            self.service, self.topic = self.subscription.split('.', 1)
        else:
            self.service, self.topic = (None, None)
        if self.service in self.config['SERVICES']:
            self.service_config = self.config['SERVICES'][self.service]
            extra_fields = self.service_config.get('extra_fields', [])
            self.extra_data = {field: data[field] for field in extra_fields}
        else:
            self.service_config = None
            self.extra_data = {}

    def prepare_service_data(self):
        """
        Returns a data dict to be sent to the service handler.
        """
        data = {'subscription': self.subscription}
        data.update(self.extra_data)
        data.update(self.session.auth_info)
        return data

    async def process(self):
        if not self.service or not self.topic:
            raise EventError(c.ERR_INVALID_SUBSCRIPTION_FORMAT)

        if not self.service_config:
            raise EventError(c.ERR_INVALID_SERVICE)

    async def perform_service_request(self, service_event, extra_data={},
                                      error_message=None):
        if service_event in self.service_config:
            url = self.service_config[service_event]
            data = self.prepare_service_data()
            data.update(extra_data)
            result = await http_post(self.shark, url, data)
            if result.get('status') != 'ok':
                raise EventError(result.get('error', error_message or
                                            c.ERR_UNHANDLED_EXCEPTION))
            return result
        return {'status': 'ok'}


class SubscribeEvent(SubscriptionEvent):
    async def authorize_subscription(self):
        await self.perform_service_request('authorizer',
                                           error_message=c.ERR_UNAUTHORIZED)

    async def before_subscribe(self):
        return await self.perform_service_request('before_subscribe')

    async def on_subscribe(self):
        return await self.perform_service_request('on_subscribe')

    async def process(self):
        await super().process()

        require_authentication = self.service_config.get(
            'require_authentication', True)

        if require_authentication and not self.session.auth_info:
            raise EventError(c.ERR_AUTH_REQUIRED)

        if self.subscription in self.session.subscriptions:
            raise EventError('Already subscribed.')

        await self.authorize_subscription()

        await self.shark.service_receiver.add_provisional_subscription(
            self.session, self.subscription)

        result = await self.before_subscribe()

        self.session.subscriptions.add(self.subscription)
        await self.send_ok(result.get('data'))

        await self.shark.service_receiver.confirm_subscription(
            self.session, self.subscription)

        await self.on_subscribe()


class UnsubscribeEvent(SubscriptionEvent):
    async def before_unsubscribe(self):
        return await self.perform_service_request('before_unsubscribe')

    async def on_unsubscribe(self):
        return await self.perform_service_request('on_unsubscribe')

    async def process(self):
        await super().process()

        if self.subscription not in self.session.subscriptions:
            raise EventError('Subscription does not exist.')

        result = await self.before_unsubscribe()
        if result.get('status') != 'ok':
            raise EventError(result['error'])

        self.session.subscriptions.remove(self.subscription)
        await self.shark.service_receiver.delete_subscription(
            self.session, self.subscription)
        await self.send_ok(result.get('data'))

        await self.on_unsubscribe()


class MessageEvent(SubscriptionEvent):
    async def on_message(self):
        return await self.perform_service_request('on_message', extra_data={
            # Message data
            'data': self.data['data']
        })

    async def process(self):
        await super().process()

        if self.subscription not in self.session.subscriptions:
            raise EventError('Subscription does not exist.')

        result = await self.on_message()
        if 'data' in result:
            await self.send_ok(result['data'])

from .utils import http_post


DEFAULT_AUTH_METHOD = 'ticket'


class EventError(Exception):
    pass


class Event:
    @classmethod
    def from_data(cls, session, data):
        event = data.get('event')

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
            import traceback
            traceback.print_exc()
            await self.send_error('Unhandled exception.')


class UnknownEvent(Event):
    async def process(self):
        if 'event' in self.data:
            raise EventError('Event not found.')
        else:
            raise EventError('Event not specified.')


class AuthEvent(Event):
    def __init__(self, session, data):
        super().__init__(session, data)
        self.auth_config = self.config['AUTHENTICATION']
        self.method = data.get('method', DEFAULT_AUTH_METHOD)

    async def process(self):
        if self.method not in self.auth_config:
            raise EventError('Authentication method unsupported.')

        # The only supported method.
        assert self.method == 'ticket'

        auth_method_config = self.auth_config[self.method]

        ticket = self.data.get('ticket')
        if not ticket:
            raise EventError('Must specify ticket.')

        auth_url = auth_method_config['validation_url']
        auth_fields = auth_method_config['auth_fields']
        result = await http_post(self.shark, auth_url, {'ticket': ticket})
        if result.get('status') != 'ok':
            raise EventError(result.get('error', 'Authentication failed'))
        auth_info = {field: result[field] for field in auth_fields}
        self.session.auth_info = auth_info
        await self.send_ok()


class SubscriptionEvent(Event):
    def __init__(self, session, data):
        super().__init__(session, data)
        self.subscription = data['subscription']
        self.service, self.topic = self.subscription.split('.', 1)
        self.service_config = self.config['SERVICES'][self.service]
        extra_fields = self.service_config.get('extra_fields', [])
        self.extra_data = {field: data[field] for field in extra_fields}

    def prepare_service_data(self):
        """
        Returns a data dict to be sent to the service handler.
        """
        data = {'subscription': self.subscription}
        data.update(self.extra_data)
        data.update(self.session.auth_info)
        return data

    async def perform_service_request(self, service_event, extra_data={},
                                      error_message=None):
        if service_event in self.service_config:
            url = self.service_config[service_event]
            data = self.prepare_service_data()
            data.update(extra_data)
            result = await http_post(self.shark, url, data)
            if result.get('status') != 'ok':
                raise EventError(result.get('error', error_message or
                                            'Unhandled exception.'))
            return result
        return {'status': 'ok'}


class SubscribeEvent(SubscriptionEvent):
    async def authorize_subscription(self):
        await self.perform_service_request('authorizer',
                                           error_message='Unauthorized.')

    async def before_subscribe(self):
        return await self.perform_service_request('before_subscribe')

    async def on_subscribe(self):
        return await self.perform_service_request('on_subscribe')

    async def process(self):
        require_authentication = self.service_config.get(
            'require_authentication', True)

        if require_authentication and not self.session.auth_info:
            raise EventError('Authentication required.')

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
        if self.subscription not in self.session.subscriptions:
            raise EventError('Subscription does not exist.')

        result = await self.on_message()
        if 'data' in result:
            await self.send_ok(result['data'])

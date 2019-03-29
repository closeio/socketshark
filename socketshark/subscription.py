import asyncio
import time

from . import constants as c
from .exceptions import EventError
from .utils import http_post


def _get_options(data):
    """
    Returns a dict of parsed message options.
    """
    raw_options = data.get('options', {})

    options = {
        'order': None,
        'order_key': None,
        'throttle': None,
        'throttle_key': None,
    }

    if 'order' in raw_options:
        try:
            options['order'] = float(raw_options['order'])
        except (TypeError, ValueError):
            pass
        else:
            options['order_key'] = raw_options.get('order_key')

    if 'throttle' in raw_options:
        try:
            options['throttle'] = float(raw_options['throttle'])
        except (TypeError, ValueError):
            pass
        else:
            options['throttle_key'] = raw_options.get('throttle_key')

    return options


class Subscription:
    """
    A subscription of a session to a service and topic.
    """
    def __init__(self, config, session, data):
        self.config = config
        self.session = session
        self.shark = session.shark
        self.name = data.get('subscription') or ''
        if '.' in self.name:
            self.service, self.topic = self.name.split('.', 1)
        else:
            self.service = self.topic = None
        if self.service in config['SERVICES']:
            self.service_config = config['SERVICES'][self.service]
            self.extra_fields = self.service_config.get('extra_fields', [])
            self.extra_data = {field: data[field] for field in self.extra_fields
                               if field in data}
            self.authorizer_fields = \
                self.service_config.get('authorizer_fields', [])
        else:
            self.service_config = None
            self.extra_data = {}
            self.authorizer_fields = []
        self.authorizer_data = None

        # order key -> numeric order (the default order key is None)
        self.order_state = {}

        # throttle key -> (last message sent timestamp, latest message, task)
        # If this is set, the possible states are:
        # (ts, None, None) -- No messages are pending.
        # (ts, msg,  task) -- The given message is scheduled to be sent by the
        #                     given asyncio task.
        # (ts, None, task) -- A message is currently being sent by the given
        #                     asyncio task.
        self.throttle_state = {}

        self._periodic_authorizer_task = None

    def validate(self):
        if not self.service or not self.topic:
            raise EventError(c.ERR_INVALID_SUBSCRIPTION_FORMAT)

        if self.service_config is None:
            raise EventError(c.ERR_INVALID_SERVICE)

    def prepare_service_data(self):
        """
        Returns a data dict to be sent to the service handler.
        """
        data = {'subscription': self.name}
        data.update(self.extra_data)
        if self.authorizer_data:
            data.update(self.authorizer_data)
        data.update(self.session.auth_info)
        return data

    async def perform_service_request(self, service_event, extra_data={},
                                      error_message=None, raise_error=True):
        if service_event in self.service_config:
            url = self.service_config[service_event]
            data = self.prepare_service_data()
            data.update(extra_data)
            result = await http_post(self.shark, url, data)
            if raise_error and result.get('status') != 'ok':
                raise EventError(result.get('error', error_message or
                                            c.ERR_UNHANDLED_EXCEPTION),
                                 data=result.get('data'))
            return result
        return {'status': 'ok'}

    async def authorize_subscription(self):
        data = await self.perform_service_request(
            'authorizer', error_message=c.ERR_UNAUTHORIZED)

        authorizer_data = {field: data[field] for field in
                           self.authorizer_fields if field in data}

        # If authorizer fields changed during periodic authorization, invoke
        # a special callback.
        fields_changed = (
            self.authorizer_data is not None and
            authorizer_data != self.authorizer_data
        )

        self.authorizer_data = authorizer_data

        if fields_changed:
            await self.perform_service_request('on_authorization_change')

    async def periodic_authorizer(self):
        period = self.service_config['authorization_renewal_period']
        self.session.trace_log.debug('initializing periodic authorizer',
                                     subscription=self.name,
                                     period=period)
        while True:
            await asyncio.sleep(period)
            try:
                self.session.log.debug('verifying authorization',
                                       subscription=self.name)
                await self.authorize_subscription()
                self.session.log.debug('authorization verified',
                                       subscription=self.name)
            except EventError as e:
                self.session.log.info('authorization expired',
                                      subscription=self.name,
                                      error=e.error)
                await self.self_unsubscribe(e.error)

    async def before_subscribe(self):
        return await self.perform_service_request('before_subscribe')

    async def on_subscribe(self):
        return await self.perform_service_request('on_subscribe',
                                                  raise_error=False)

    async def on_message(self, message_data):
        return await self.perform_service_request('on_message', extra_data={
            'data': message_data,
        })

    async def before_unsubscribe(self, raise_error=True):
        return await self.perform_service_request('before_unsubscribe',
                                                  raise_error=raise_error)

    async def on_unsubscribe(self):
        return await self.perform_service_request('on_unsubscribe',
                                                  raise_error=False)

    def _should_deliver_message_filter_fields(self, data):
        """
        Returns whether to deliver the given message based on filter feilds.
        """
        # Check whether the message is filtered by comparing any defined
        # filter_fields to auth_info and extra_fields.
        filter_fields = self.service_config.get('filter_fields', [])
        for field in filter_fields:
            if field in data:
                if field in self.extra_fields:
                    if self.extra_data.get(field) != data[field]:
                        # Message doesn't match extra fields.
                        return False
                elif self.session.auth_info.get(field) != data[field]:
                    # Message doesn't match auth fields.
                    return False
        return True

    def _should_deliver_message_order(self, data, options):
        """
        Returns whether to deliver the given message based on order.
        """
        order = options['order']
        if order is None:
            return True

        # Check whether the message is out-of-order.
        key = options['order_key']
        last_order = self.order_state.get(key)

        if last_order is not None and order <= last_order:
            return False  # Message out-of-order.

        self.order_state[key] = order

        return True

    def _should_deliver_message_throttle(self, data, options):
        """
        Returns whether to deliver the given message based on throttling.
        """
        throttle = options['throttle']
        if throttle is None:
            return True

        key = options['throttle_key']
        last_throttle = self.throttle_state.get(key)
        now = time.time()
        if last_throttle:
            ts_last_msg_sent, pending_msg, task = last_throttle
            if task:  # We'll update the message and let the task send it.
                self.throttle_state[key] = (ts_last_msg_sent, data, task)
                return False
            elif now - ts_last_msg_sent < throttle:
                # Schedule a task to send the message.
                self._schedule_throttled_message_task(key, ts_last_msg_sent,
                                                      data)
                return False

        # Send current message and store time.
        self.throttle_state[key] = (now, None, None)
        return True

    def _schedule_throttled_message_task(self, key, ts_last_msg_sent, data):
        options = _get_options(data)
        # This should succeed since we parsed it previously
        when = ts_last_msg_sent + options['throttle']
        task = asyncio.ensure_future(self._schedule_throttled_message(when,
                                                                      key))
        self.throttle_state[key] = (ts_last_msg_sent, data, task)

    def should_deliver_message(self, data):
        """
        Returns whether to deliver the given message.
        """
        options = _get_options(data)

        if not self._should_deliver_message_filter_fields(data):
            self.session.trace_log.debug('message filtered', data=data,
                                         reason='fields')
            return False

        if not self._should_deliver_message_order(data, options):
            self.session.trace_log.debug('message filtered', data=data,
                                         reason='order')
            return False

        if not self._should_deliver_message_throttle(data, options):
            self.session.trace_log.debug('message filtered', data=data,
                                         reason='throttle')
            return False

        return True

    async def _schedule_throttled_message(self, when, throttle_key):
        delay = when - time.time()
        self.session.trace_log.debug('throttled message scheduled',
                                     throttle_key=throttle_key, delay=delay)
        try:
            await asyncio.sleep(delay)
            await self._send_throttled_message(throttle_key)
        except asyncio.CancelledError:  # Cancelled by unsubscribe
            self.session.trace_log.debug('throttled message canceled',
                                         throttle_key=throttle_key)
        except Exception:
            self.session.log.exception('unhandled exception when sending '
                                       'throttled message')

    async def _send_throttled_message(self, throttle_key):
        # We've unsubscribed meanwhile.
        if self.name not in self.session.subscriptions:
            self.session.trace_log.debug('throttled message subscription '
                                         'invalid', throttle_key=throttle_key)
            return

        last_throttle = self.throttle_state[throttle_key]
        ts_last_msg_sent, pending_msg, task = last_throttle
        now = time.time()
        self.throttle_state[throttle_key] = (now, None, task)
        self.session.trace_log.debug('sending throttled message',
                                     throttle_key=throttle_key)
        await self.session.send_message(self, pending_msg['data'])

        ts_last_msg_sent, pending_msg, task = self.throttle_state[throttle_key]
        # A throttled message was submitted while we were sending.
        # Schedule another task.
        if pending_msg:
            self.session.trace_log.debug('throttled message submitted while '
                                         'sending', throttle_key=throttle_key)
            self._schedule_throttled_message_task(throttle_key,
                                                  ts_last_msg_sent,
                                                  pending_msg)
        else:
            self.throttle_state[throttle_key] = (now, None, None)

    async def subscribe(self, event):
        """
        Subscribes to the subscription.
        """
        require_authentication = self.service_config.get(
            'require_authentication', True)

        if require_authentication and not self.session.auth_info:
            raise EventError(c.ERR_AUTH_REQUIRED)

        if self.name in self.session.subscriptions:
            raise EventError(c.ERR_ALREADY_SUBSCRIBED)

        await self.authorize_subscription()

        await self.shark.service_receiver.add_provisional_subscription(
            self.session, self.name)

        result = await self.before_subscribe()

        self.session.subscriptions[self.name] = self

        if self.should_deliver_message(result):
            await event.send_ok(result.get('data'))

        await self.shark.service_receiver.confirm_subscription(
            self.session, self.name)

        if 'authorization_renewal_period' in self.service_config:
            self._periodic_authorizer_task = asyncio.ensure_future(
                self.periodic_authorizer())

        await self.on_subscribe()

    async def message(self, event):
        """
        Sends a message to the subscription.
        """
        if self.name not in self.session.subscriptions:
            raise EventError(c.ERR_SUBSCRIPTION_NOT_FOUND)

        message_data = event.data.get('data')

        result = await self.on_message(message_data)
        if 'data' in result:
            if event:
                await event.send_ok(result['data'])

    async def cleanup_subscription(self):
        await self.shark.service_receiver.delete_subscription(
            self.session, self.name)

        for key, throttle in self.throttle_state.items():
            ts_last_msg_sent, pending_msg, task = throttle
            if task:
                task.cancel()

        if self._periodic_authorizer_task:
            self._periodic_authorizer_task.cancel()

    async def unsubscribe(self, event):
        """
        Unsubscribes from the subscription.
        """
        if self.name not in self.session.subscriptions:
            raise EventError(c.ERR_SUBSCRIPTION_NOT_FOUND)

        result = await self.before_unsubscribe()

        del self.session.subscriptions[self.name]
        await self.cleanup_subscription()

        await event.send_ok(result.get('data'))

        await self.on_unsubscribe()

    async def self_unsubscribe(self, error):
        """
        Unsubscribes from the subscription (not triggered by the user).
        """
        del self.session.subscriptions[self.name]
        await self.cleanup_subscription()

        result = await self.before_unsubscribe(raise_error=False)
        await self.on_unsubscribe()
        await self.session.send_unsubscribe(self, result.get('data'), error)

    async def force_unsubscribe(self):
        """
        Force-unsubscribes from the subscription. Caller is responsible for
        deleting the subscription from the session's subscriptions array.
        This method is called when a session is disconnected.
        """
        await self.cleanup_subscription()

        await self.before_unsubscribe(raise_error=False)
        await self.on_unsubscribe()

import asyncio

import aioredis
from aioresponses import aioresponses
import pytest

from socketshark import constants as c, SocketShark
from socketshark.session import Session


TEST_CONFIG = {
    'REDIS': {
        'host': 'localhost',
        'port': 6379,
        'channel_prefix': '',
    },
    'HTTP': {
        'timeout': 1,
        'tries': 1,
        'wait': 1,
    },
    'AUTHENTICATION': {
        'ticket': {
            'validation_url': 'http://auth-service/auth/ticket/',
            'auth_fields': ['session_id'],
        }
    },
    'SERVICES': {
        'empty': {},
        'simple': {
            'require_authentication': False,
            'filter_fields': ['session_id'],
        }
    },
}


class MockClient:
    def __init__(self):
        self.log = []

    async def send(self, event):
        self.log.append(event)


class TestShark:
    def test_shark_init(self):
        shark = SocketShark(TEST_CONFIG)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(shark.prepare())
        loop.run_until_complete(shark.shutdown())


class TestSession:
    @pytest.mark.asyncio
    async def test_invalid_message(self):
        """
        Test basic validation of event messages.
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event(None)
        assert client.log.pop() == {
            'status': 'error',
            'error': c.ERR_INVALID_EVENT,
        }

        await session.on_client_event('hello')
        assert client.log.pop() == {
            'status': 'error',
            'error': c.ERR_INVALID_EVENT,
        }

        await session.on_client_event({})
        assert client.log.pop() == {
            'status': 'error',
            'error': c.ERR_INVALID_EVENT,
        }

        await session.on_client_event({'event': None})
        assert client.log.pop() == {
            'status': 'error',
            'error': c.ERR_INVALID_EVENT,
        }

        await session.on_client_event({'event': ''})
        assert client.log.pop() == {
            'status': 'error',
            'event': '',
            'error': c.ERR_EVENT_NOT_FOUND,
        }

        await session.on_client_event({'event': 'hello'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'hello',
            'error': c.ERR_EVENT_NOT_FOUND,
        }

        assert not client.log

    @pytest.mark.asyncio
    async def test_auth_invalid(self):
        """
        Test invalid authentication method
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({'event': 'auth', 'method': 'x'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'auth',
            'error': c.ERR_AUTH_UNSUPPORTED,
        }

        no_auth_config = TEST_CONFIG.copy()
        no_auth_config['AUTHENTICATION'] = {}
        shark = SocketShark(no_auth_config)
        session = Session(shark, client)

        await session.on_client_event({'event': 'auth', 'method': 'ticket'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'auth',
            'error': c.ERR_AUTH_UNSUPPORTED,
        }

        assert not client.log

    @pytest.mark.asyncio
    async def test_auth_ticket(self):
        """
        Test ticket authentication.
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({'event': 'auth'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'auth',
            'error': c.ERR_NEEDS_TICKET,
        }

        await session.on_client_event({'event': 'auth', 'method': 'ticket'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'auth',
            'error': c.ERR_NEEDS_TICKET,
        }

        with aioresponses() as mock:
            # Auth endpoint unreachable
            await session.on_client_event({
                'event': 'auth',
                'method': 'ticket',
                'ticket': 'the_ticket',
            })
            assert client.log.pop() == {
                'status': 'error',
                'event': 'auth',
                'error': c.ERR_SERVICE_UNAVAILABLE,
            }

        with aioresponses() as mock:
            # Mock auth endpoint
            auth_url = 'http://auth-service/auth/ticket/'

            # First request fails, second one succeeds.
            mock.post(auth_url, payload={
                'status': 'error',

                # these should be ignored
                'session_id': 'sess_invalid',
                'foo': 'bar',
            })

            mock.post(auth_url, payload={
                'status': 'ok',
                'session_id': 'sess_123',

                # this should be ignored
                'foo': 'bar',
            })

            await session.on_client_event({
                'event': 'auth',
                'method': 'ticket',
                'ticket': 'invalid_ticket',
            })
            assert client.log.pop() == {
                'status': 'error',
                'event': 'auth',
                'error': c.ERR_AUTH_FAILED,
            }

            assert session.auth_info == {}

            await session.on_client_event({
                'event': 'auth',
                'method': 'ticket',
                'ticket': 'valid_ticket',
            })
            assert client.log.pop() == {
                'status': 'ok',
                'event': 'auth',
            }

            assert session.auth_info == {
                'session_id': 'sess_123',
            }

            # Ensure we passed the right arguments to the mock endpoint.
            requests = mock.requests[('POST', auth_url)]

            invalid_request, valid_request = requests

            assert invalid_request.kwargs['json'] == {
                'ticket': 'invalid_ticket',
            }

            assert valid_request.kwargs['json'] == {
                'ticket': 'valid_ticket',
            }

        assert not client.log

    @pytest.mark.asyncio
    async def test_subscription_invalid(self):
        """
        Test subscription validation
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({
            'event': 'subscribe',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'status': 'error',
            'error': c.ERR_INVALID_SUBSCRIPTION_FORMAT,
        }

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'invalid'
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'status': 'error',
            'error': c.ERR_INVALID_SUBSCRIPTION_FORMAT,
        }

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'invalid.topic'
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'status': 'error',
            'error': c.ERR_INVALID_SERVICE,
        }

        assert not client.log

    @pytest.mark.asyncio
    async def test_subscription_needs_auth(self):
        """
        For safety reasons, subscriptions require authentication by default.
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'empty.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'status': 'error',
            'error': c.ERR_AUTH_REQUIRED,
        }

        assert not client.log

    @pytest.mark.asyncio
    async def test_subscription_validation(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'status': 'ok',
            'event': 'subscribe',
        }

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'status': 'error',
            'event': 'subscribe',
            'error': c.ERR_ALREADY_SUBSCRIBED,
        }

        await session.on_client_event({
            'event': 'message',
            'subscription': 'simple.invalid',
            'data': {'foo': 'bar'},
        })
        assert client.log.pop() == {
            'status': 'error',
            'event': 'message',
            'error': c.ERR_SUBSCRIPTION_NOT_FOUND,
        }

        await session.on_client_event({
            'event': 'unsubscribe',
            'subscription': 'simple.invalid',
        })
        assert client.log.pop() == {
            'status': 'error',
            'event': 'unsubscribe',
            'error': c.ERR_SUBSCRIPTION_NOT_FOUND,
        }

    @pytest.mark.asyncio
    async def test_subscription_simple(self):
        """
        Test subscription to an unauthenticated service.

        Messages are not captured because this service doesn't have any
        callbacks.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'status': 'ok',
            'event': 'subscribe',
        }

        prov_subs = shark.service_receiver.provisional_subscriptions
        assert prov_subs == {}

        conf_subs = shark.service_receiver.confirmed_subscriptions
        sessions = conf_subs['simple.topic']
        assert sessions == set([session])

        # Test message from client to server
        await session.on_client_event({
            'event': 'message',
            'subscription': 'simple.topic',
            'data': {'foo': 'bar'},
        })

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis((
            redis_settings['host'], redis_settings['port']))
        redis_topic = redis_settings['channel_prefix'] + 'simple.topic'

        # This message has no filters and will arrive.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple.topic',
            'data': {'baz': 'foo'}
        })

        # This message has an invalid subscription.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple.othertopic',
            'data': {'never': 'arrives'}
        })

        # This message has incorrect JSON
        await redis.publish(redis_topic, '{')

        # This message will arrive on anonymous sessions only.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple.topic',
            'session_id': None,
            'data': {'arrives': True},
        })

        # This message will not arrive on sessions with a session_id.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple.topic',
            'session_id': 'sess_123',
            'data': {'arrives': False},
        })

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [{
            'event': 'message',
            'subscription': 'simple.topic',
            'data': {'baz': 'foo'},
        }, {
            'event': 'message',
            'subscription': 'simple.topic',
            'data': {'arrives': True},
        }]

        client.log = []

        await session.on_client_event({
            'event': 'unsubscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'status': 'ok',
            'event': 'unsubscribe',
        }

        prov_subs = shark.service_receiver.provisional_subscriptions
        assert prov_subs == {}

        conf_subs = shark.service_receiver.confirmed_subscriptions
        assert conf_subs == {}

        assert not client.log

        await shark.shutdown()

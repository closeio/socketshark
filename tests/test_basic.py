import asyncio
import json

import aiohttp
import aioredis
from aioresponses import aioresponses
import pytest

from socketshark import constants as c, load_backend, SocketShark
from socketshark.session import Session


TEST_CONFIG = {
    'BACKEND': 'websockets',
    'WS_HOST': '127.0.0.1',
    'WS_PORT': 9001,
    'WS_PING': {'interval': 0.1, 'timeout': 0.1},
    'METRICS': {},
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
        },
        'simple_auth': {
            'require_authentication': True,
            'filter_fields': ['session_id'],
        },
        'authorizer': {
            'require_authentication': True,
            'authorizer': 'http://auth-service/auth/authorizer/',
            'extra_fields': ['extra'],
        },
        'complex': {
            'require_authentication': True,
            'authorizer': 'http://auth-service/auth/authorizer/',
            'extra_fields': ['extra'],
            'before_subscribe': 'http://my-service/subscribe/',
            'before_unsubscribe': 'http://my-service/unsubscribe/',
            'on_subscribe': 'http://my-service/on_subscribe/',
            'on_unsubscribe': 'http://my-service/on_unsubscribe/',
            'on_message': 'http://my-service/on_message/',
        },
        'ws_test': {
            'require_authentication': False,
            'on_unsubscribe': 'http://my-service/on_unsubscribe/',
        },
    },
}


class MockClient:
    def __init__(self, shark):
        self.log = []
        self.session = Session(shark, self)

    async def send(self, event):
        self.log.append(event)

    async def close(self):
        await self.session.on_close()


class TestShark:
    def test_shark_init(self):
        shark = SocketShark(TEST_CONFIG)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(shark.prepare())
        loop.run_until_complete(shark.shutdown())


class TestSession:
    """
    Main test cases involving the Session class.
    """
    async def _auth_session(self, session):
        with aioresponses() as mock:
            # Mock auth endpoint
            auth_url = 'http://auth-service/auth/ticket/'

            mock.post(auth_url, payload={
                'status': 'ok',
                'session_id': 'sess_123',
            })

            await session.on_client_event({
                'event': 'auth',
                'method': 'ticket',
                'ticket': 'valid_ticket',
            })
            assert session.client.log.pop() == {
                'status': 'ok',
                'event': 'auth',
            }

            assert session.auth_info == {
                'session_id': 'sess_123',
            }

    @pytest.mark.asyncio
    async def test_invalid_message(self):
        """
        Test basic validation of event messages.
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient(shark)
        session = client.session

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
        client = MockClient(shark)
        session = client.session

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
        client = MockClient(shark)
        session = client.session

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

            # Ensure we passed the right arguments to the auth endpoint.
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
        client = MockClient(shark)
        session = client.session

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
            'subscription': 'invalid',
            'status': 'error',
            'error': c.ERR_INVALID_SUBSCRIPTION_FORMAT,
        }

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'invalid.topic'
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'invalid.topic',
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
        client = MockClient(shark)
        session = client.session

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'empty.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'empty.topic',
            'status': 'error',
            'error': c.ERR_AUTH_REQUIRED,
        }

        assert not client.log

    @pytest.mark.asyncio
    async def test_subscription_validation(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'ok',
        }

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'error',
            'error': c.ERR_ALREADY_SUBSCRIBED,
        }

        await session.on_client_event({
            'event': 'message',
            'subscription': 'simple.invalid',
            'data': {'foo': 'bar'},
        })
        assert client.log.pop() == {
            'status': 'error',
            'subscription': 'simple.invalid',
            'event': 'message',
            'error': c.ERR_SUBSCRIPTION_NOT_FOUND,
        }

        await session.on_client_event({
            'event': 'unsubscribe',
            'subscription': 'simple.invalid',
        })
        assert client.log.pop() == {
            'event': 'unsubscribe',
            'subscription': 'simple.invalid',
            'status': 'error',
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
        client = MockClient(shark)
        session = client.session

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'ok',
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
            'event': 'unsubscribe',
            'subscription': 'simple.topic',
            'status': 'ok',
        }

        prov_subs = shark.service_receiver.provisional_subscriptions
        assert prov_subs == {}

        conf_subs = shark.service_receiver.confirmed_subscriptions
        assert conf_subs == {}

        assert not client.log

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_auth(self):
        """
        Test subscription to an authenticated service.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple_auth.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple_auth.topic',
            'status': 'error',
            'error': c.ERR_AUTH_REQUIRED,
        }

        await self._auth_session(session)

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple_auth.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple_auth.topic',
            'status': 'ok',
        }

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis((
            redis_settings['host'], redis_settings['port']))
        redis_topic = redis_settings['channel_prefix'] + 'simple_auth.topic'

        # This message has no filters and will arrive.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple_auth.topic',
            'data': {'foo': 'bar'}
        })

        # This message will arrive on an invalid session only.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple_auth.topic',
            'session_id': 'sess_invalid',
            'data': {'arrives': False},
        })

        # This message will arrive in the current session.
        await redis.publish_json(redis_topic, {
            'subscription': 'simple_auth.topic',
            'session_id': 'sess_123',
            'data': {'arrives': True},
        })

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [{
            'event': 'message',
            'subscription': 'simple_auth.topic',
            'data': {'foo': 'bar'},
        }, {
            'event': 'message',
            'subscription': 'simple_auth.topic',
            'data': {'arrives': True},
        }]

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_authorizer(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'authorizer.topic',
        })
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'authorizer.topic',
            'status': 'error',
            'error': c.ERR_AUTH_REQUIRED,
        }

        await self._auth_session(session)

        with aioresponses() as mock:
            # Authorizer is unavailable.
            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'error',
                'error': c.ERR_SERVICE_UNAVAILABLE,
            }

        with aioresponses() as mock:
            # Mock authorizer
            authorizer_url = 'http://auth-service/auth/authorizer/'

            mock.post(authorizer_url, payload={
                'status': 'error',
            })

            mock.post(authorizer_url, payload={
                'status': 'error',
                'error': 'test error',
            })

            mock.post(authorizer_url, payload={
                'status': 'ok',
            })

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'error',
                'error': c.ERR_UNAUTHORIZED,
            }

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'error',
                'error': 'test error',
            }

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'extra': 'foo',
                'other': 'bar',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'ok',
                'extra': 'foo',
            }

            # Ensure we passed the right arguments to the authorizer endpoint.
            requests = mock.requests[('POST', authorizer_url)]
            r1, r2, r3 = requests
            assert r1.kwargs['json'] == r2.kwargs['json'] == {
                'subscription': 'authorizer.topic',
                'session_id': 'sess_123',
            }
            assert r3.kwargs['json'] == {
                'subscription': 'authorizer.topic',
                'session_id': 'sess_123',
                'extra': 'foo',
            }

        assert client.log == []

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_complex(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session
        await self._auth_session(session)

        conf = TEST_CONFIG['SERVICES']['complex']

        # Test unsuccessful subscriptions
        with aioresponses() as mock:
            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(conf['before_subscribe'], payload={'status': 'error'})

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'complex.topic',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'error': c.ERR_UNHANDLED_EXCEPTION,
            }

            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(conf['before_subscribe'], payload={
                'status': 'error',
                'error': 'before subscribe error',
            })

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'complex.topic',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'error': 'before subscribe error',
            }

            req_list_1 = mock.requests[('POST', conf['authorizer'])]
            req_list_2 = mock.requests[('POST', conf['before_subscribe'])]
            for request in req_list_1 + req_list_2:
                assert request.kwargs['json'] == {
                    'session_id': 'sess_123',
                    'subscription': 'complex.topic'
                }

        # Test successful subscription with extra field and messages
        with aioresponses() as mock:
            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(conf['before_subscribe'], payload={'status': 'ok'})
            mock.post(conf['on_subscribe'], payload={'doesnt': 'matter'})
            mock.post(conf['on_message'], payload={'status': 'ok'})
            mock.post(conf['on_message'], payload={
                'status': 'error',
                'error': 'on message error',
            })
            mock.post(conf['on_message'], payload={
                'status': 'ok',
                'data': None,
            })
            mock.post(conf['on_message'], payload={
                'status': 'ok',
                'data': {'reply': True}
            })

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'extra': 'hello',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
            }

            # Successful but no reply
            await session.on_client_event({
                'event': 'message',
                'subscription': 'complex.topic',
                'extra': 'irrelevant',
            })

            await session.on_client_event({
                'event': 'message',
                'subscription': 'complex.topic',
                'extra': 'irrelevant',
                'data': {'foo': 'bar', 'baz': 123},
            })
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': 'on message error',
            }

            await session.on_client_event({
                'event': 'message',
                'subscription': 'complex.topic',
                'extra': 'irrelevant',
            })
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
            }

            await session.on_client_event({
                'event': 'message',
                'subscription': 'complex.topic',
                'extra': 'irrelevant',
            })
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
                'data': {'reply': True},
            }

            req_list_1 = mock.requests[('POST', conf['authorizer'])]
            req_list_2 = mock.requests[('POST', conf['before_subscribe'])]
            req_list_3 = mock.requests[('POST', conf['on_subscribe'])]
            for request in req_list_1 + req_list_2 + req_list_3:
                assert request.kwargs['json'] == {
                    'session_id': 'sess_123',
                    'subscription': 'complex.topic',
                    'extra': 'hello',
                }

            msg_reqs = mock.requests[('POST', conf['on_message'])]
            assert msg_reqs[0].kwargs['json'] == {
                'session_id': 'sess_123',
                'subscription': 'complex.topic',
                'extra': 'hello',
                'data': None,
            }
            assert msg_reqs[1].kwargs['json'] == {
                'session_id': 'sess_123',
                'subscription': 'complex.topic',
                'extra': 'hello',
                'data': {'foo': 'bar', 'baz': 123},
            }

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis((
            redis_settings['host'], redis_settings['port']))
        redis_topic = redis_settings['channel_prefix'] + 'complex.topic'

        await redis.publish_json(redis_topic, {
            'subscription': 'complex.topic',
            'data': {'foo': 'bar'}
        })

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log.pop() == {
            'event': 'message',
            'subscription': 'complex.topic',
            'data': {'foo': 'bar'},
            'extra': 'hello',
        }

        # Test unsubscribe callbacks
        with aioresponses() as mock:
            mock.post(conf['before_unsubscribe'], payload={'status': 'error'})
            mock.post(conf['before_unsubscribe'], payload={
                'status': 'error',
                'error': 'before unsubscribe error',
            })
            mock.post(conf['before_unsubscribe'], payload={'status': 'ok'})
            mock.post(conf['on_unsubscribe'], payload={'doesnt': 'matter'})

            await session.on_client_event({
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
            })
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': c.ERR_UNHANDLED_EXCEPTION,
            }

            await session.on_client_event({
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
            })
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': 'before unsubscribe error',
            }

            await session.on_client_event({
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
            })
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
            }

            req_list_1 = mock.requests[('POST', conf['before_unsubscribe'])]
            req_list_2 = mock.requests[('POST', conf['on_unsubscribe'])]
            for request in req_list_1 + req_list_2:
                assert request.kwargs['json'] == {
                    'session_id': 'sess_123',
                    'subscription': 'complex.topic',
                    'extra': 'hello',
                }

        # Test extra data in subscribe/unsubscribe callbacks
        with aioresponses() as mock:
            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(conf['before_subscribe'], payload={
                'status': 'ok',
                'data': {'foo': 'subscribe'},
            })
            mock.post(conf['on_subscribe'], payload={})
            mock.post(conf['before_unsubscribe'], payload={
                'status': 'ok',
                'data': {'foo': 'unsubscribe'},
            })
            mock.post(conf['on_unsubscribe'], payload={})

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'complex.extra_data',
            })
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.extra_data',
                'status': 'ok',
                'data': {'foo': 'subscribe'},
            }

            await session.on_client_event({
                'event': 'unsubscribe',
                'subscription': 'complex.extra_data',
            })
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.extra_data',
                'status': 'ok',
                'data': {'foo': 'unsubscribe'},
            }

        assert client.log == []

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_unsubscribe_on_close(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session
        await self._auth_session(session)

        await session.on_client_event({
            'event': 'subscribe',
            'subscription': 'simple.one',
        })
        assert session.client.log.pop() == {
            'status': 'ok',
            'event': 'subscribe',
            'subscription': 'simple.one',
        }

        conf = TEST_CONFIG['SERVICES']['complex']

        with aioresponses() as mock:
            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(conf['before_subscribe'], payload={'status': 'ok'})
            mock.post(conf['on_subscribe'], payload={})

            await session.on_client_event({
                'event': 'subscribe',
                'subscription': 'complex.two',
            })
            assert session.client.log.pop() == {
                'status': 'ok',
                'event': 'subscribe',
                'subscription': 'complex.two',
            }

        subscription_names = set(session.subscriptions.keys())
        assert subscription_names == set(['simple.one', 'complex.two'])

        await session.on_close()

        assert session.subscriptions == {}

        await shark.shutdown()


class TestWebsocket:
    """
    Test an actual WebSocket connection.
    """

    @property
    def ws_url(self):
        return 'http://{}:{}'.format(TEST_CONFIG['WS_HOST'],
                                     TEST_CONFIG['WS_PORT'])

    def test_websocket(self):
        shark = SocketShark(TEST_CONFIG)

        done = False

        async def task():
            nonlocal done

            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()
            mock = aioresponses()
            conf = TEST_CONFIG['SERVICES']['ws_test']

            async with aiosession.ws_connect(self.ws_url) as ws:
                await ws.send_str(json.dumps({
                    'event': 'subscribe',
                    'subscription': 'ws_test.hello',
                }))
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data == {
                    'event': 'subscribe',
                    'subscription': 'ws_test.hello',
                    'status': 'ok',
                }

                # Start mocking here (if we started mocking earlier we wouldn't
                # be able to use aiohttp to connect to the WebSocket).
                mock.start()
                mock.post(conf['on_unsubscribe'], payload={})

            await aiosession.close()

            # Wait until backend learns about the disconnected WebSocket.
            await asyncio.sleep(0.1)
            mock.stop()
            requests = mock.requests[('POST', conf['on_unsubscribe'])]
            assert len(requests) == 1
            assert requests[0].kwargs['json'] == {
                'subscription': 'ws_test.hello'}

            await shark.shutdown()

            done = True

        shark = SocketShark(TEST_CONFIG)
        backend = load_backend(TEST_CONFIG)
        asyncio.ensure_future(task())
        backend.run(shark)

        assert done

    def test_shutdown(self):
        """
        Make sure we call unsubscribe callbacks when shutting down.
        """
        mock = aioresponses()
        conf = TEST_CONFIG['SERVICES']['ws_test']

        async def task():
            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()

            async with aiosession.ws_connect(self.ws_url) as ws:
                await ws.send_str(json.dumps({
                    'event': 'subscribe',
                    'subscription': 'ws_test.hello',
                }))
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data == {
                    'event': 'subscribe',
                    'subscription': 'ws_test.hello',
                    'status': 'ok',
                }

                mock.start()
                mock.post(conf['on_unsubscribe'], payload={})

                # Shutdown
                asyncio.ensure_future(shark.shutdown())

                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE
                await ws.close()

            await aiosession.close()

        shark = SocketShark(TEST_CONFIG)
        backend = load_backend(TEST_CONFIG)
        asyncio.ensure_future(task())
        backend.run(shark)
        mock.stop()

        requests = mock.requests[('POST', conf['on_unsubscribe'])]
        assert len(requests) == 1
        assert requests[0].kwargs['json'] == {
                'subscription': 'ws_test.hello'}

    def test_ping(self):
        """
        Ensure server sends periodic pings and disconnects timed out clients.
        """
        async def task():
            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()

            async with aiosession.ws_connect(self.ws_url,
                                             autoping=False) as ws:

                # Respond to ping in time
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.PING
                ws.pong(msg.data)

                # Respond to ping late
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.PING
                await asyncio.sleep(0.1)
                ws.pong(msg.data)

                # Ensure we get disconnected
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE

                await ws.close()

            await aiosession.close()
            asyncio.ensure_future(shark.shutdown())

        shark = SocketShark(TEST_CONFIG)
        backend = load_backend(TEST_CONFIG)
        asyncio.ensure_future(task())
        backend.run(shark)

    def test_redis_disconnect(self):
        """
        Ensure server disconnects clients when Redis connection closes.
        """
        async def task():
            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()

            ws1 = await aiosession.ws_connect(self.ws_url)
            ws2 = await aiosession.ws_connect(self.ws_url)
            await ws2.send_str(json.dumps({
                'event': 'subscribe',
                'subscription': 'simple.hello',
            }))
            msg = await ws2.receive()
            assert msg.type == aiohttp.WSMsgType.TEXT
            data = json.loads(msg.data)
            assert data == {
                'event': 'subscribe',
                'subscription': 'simple.hello',
                'status': 'ok',
            }

            shark.redis.close()

            msg = await ws1.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE

            msg = await ws2.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE

            await aiosession.close()

        shark = SocketShark(TEST_CONFIG)
        backend = load_backend(TEST_CONFIG)
        asyncio.ensure_future(task())
        backend.run(shark)

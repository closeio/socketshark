import asyncio
import json
import os
import time
from unittest.mock import patch

import aiohttp
import aioredis
import pytest
from aioresponses import aioresponses
from yarl import URL

from socketshark import (
    SocketShark,
    config_defaults,
    constants as c,
    setup_logging,
)
from socketshark.session import Session

LOCAL_REDIS_HOST = os.environ.get('LOCAL_REDIS_HOST')
if not LOCAL_REDIS_HOST:
    LOCAL_REDIS_HOST = '127.0.0.1'

TEST_CONFIG = {
    'BACKEND': 'websockets',
    'WS_HOST': '127.0.0.1',
    'WS_PORT': 9001,
    'WS_PING': {'interval': 0.1, 'timeout': 0.1},
    'LOG': config_defaults.LOG,
    'METRICS': {},
    'REDIS': {
        'host': LOCAL_REDIS_HOST,
        'port': 6379,
        'channel_prefix': '',
        'ping_interval': 0.1,
        'ping_timeout': 0.1,
    },
    'HTTP': {
        'timeout': 1,
        'tries': 1,
        'wait': 1,
        'rate_limit_reset_header_name': 'X-Rate-Limit-Reset',
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
            'filter_fields': ['session_id', 'extra'],
            'extra_fields': ['extra'],
        },
        'simple_auth': {
            'require_authentication': True,
            'filter_fields': ['session_id'],
        },
        'simple_before_subscribe': {
            'require_authentication': False,
            'before_subscribe': 'http://my-service/subscribe/',
        },
        'authorizer': {
            'require_authentication': True,
            'authorizer': 'http://auth-service/auth/authorizer/',
            'extra_fields': ['extra'],
        },
        'periodic_authorizer': {
            'require_authentication': True,
            'authorizer': 'http://auth-service/auth/authorizer/',
            'authorization_renewal_period': 0.2,
        },
        'periodic_authorizer_with_fields': {
            'require_authentication': True,
            'authorizer': 'http://auth-service/auth/authorizer/',
            'authorizer_fields': ['capabilities'],
            'authorization_renewal_period': 0.2,
            'on_subscribe': 'http://my-service/on_subscribe/',
            'on_authorization_change': 'http://my-service/on_auth_change/',
        },
        'complex': {
            'require_authentication': True,
            'authorizer': 'http://auth-service/auth/authorizer/',
            'authorizer_fields': ['capabilities'],
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

TEST_CONFIG_WITH_ALT_REDIS = TEST_CONFIG.copy()
TEST_CONFIG_WITH_ALT_REDIS['REDIS_ALT'] = TEST_CONFIG['REDIS'].copy()
TEST_CONFIG_WITH_ALT_REDIS['REDIS_ALT']['channel_prefix'] = 'alt:'

setup_logging(TEST_CONFIG['LOG'])


class aioresponses_delayed(aioresponses):  # noqa
    """
    Just like aioresponses, but slightly delays POST requests.
    """

    async def _request_mock(self, orig_self, method, url, *args, **kwargs):
        result = await super()._request_mock(
            orig_self, method, url, *args, **kwargs
        )
        if method == 'POST':
            await asyncio.sleep(0.2)
        return result


class MockClient:  # noqa SIM119
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

            mock.post(
                auth_url,
                payload={
                    'status': 'ok',
                    'session_id': 'sess_123',
                },
            )

            await session.on_client_event(
                {
                    'event': 'auth',
                    'method': 'ticket',
                    'ticket': 'valid_ticket',
                }
            )
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
        Test invalid authentication method.
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
            await session.on_client_event(
                {
                    'event': 'auth',
                    'method': 'ticket',
                    'ticket': 'the_ticket',
                }
            )
            assert client.log.pop() == {
                'status': 'error',
                'event': 'auth',
                'error': c.ERR_SERVICE_UNAVAILABLE,
            }

        with aioresponses() as mock:
            # Mock auth endpoint
            auth_url = 'http://auth-service/auth/ticket/'

            # First request fails, second one succeeds.
            mock.post(
                auth_url,
                payload={
                    'status': 'error',
                    # these should be ignored
                    'session_id': 'sess_invalid',
                    'foo': 'bar',
                },
            )

            mock.post(
                auth_url,
                payload={
                    'status': 'ok',
                    'session_id': 'sess_123',
                    # this should be ignored
                    'foo': 'bar',
                },
            )

            await session.on_client_event(
                {
                    'event': 'auth',
                    'method': 'ticket',
                    'ticket': 'invalid_ticket',
                }
            )
            assert client.log.pop() == {
                'status': 'error',
                'event': 'auth',
                'error': c.ERR_AUTH_FAILED,
            }

            assert session.auth_info == {}

            await session.on_client_event(
                {
                    'event': 'auth',
                    'method': 'ticket',
                    'ticket': 'valid_ticket',
                }
            )
            assert client.log.pop() == {
                'status': 'ok',
                'event': 'auth',
            }

            assert session.auth_info == {
                'session_id': 'sess_123',
            }

            # Ensure we passed the right arguments to the auth endpoint.
            requests = mock.requests[('POST', URL(auth_url))]

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
        Test subscription validation.
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient(shark)
        session = client.session

        await session.on_client_event(
            {
                'event': 'subscribe',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'status': 'error',
            'error': c.ERR_INVALID_SUBSCRIPTION_FORMAT,
        }

        await session.on_client_event(
            {'event': 'subscribe', 'subscription': 'invalid'}
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'invalid',
            'status': 'error',
            'error': c.ERR_INVALID_SUBSCRIPTION_FORMAT,
        }

        await session.on_client_event(
            {'event': 'subscribe', 'subscription': 'invalid.topic'}
        )
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

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'empty.topic',
            }
        )
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

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'ok',
        }

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'error',
            'error': c.ERR_ALREADY_SUBSCRIBED,
        }

        await session.on_client_event(
            {
                'event': 'message',
                'subscription': 'simple.invalid',
                'data': {'foo': 'bar'},
            }
        )
        assert client.log.pop() == {
            'status': 'error',
            'subscription': 'simple.invalid',
            'event': 'message',
            'error': c.ERR_SUBSCRIPTION_NOT_FOUND,
        }

        await session.on_client_event(
            {
                'event': 'unsubscribe',
                'subscription': 'simple.invalid',
            }
        )
        assert client.log.pop() == {
            'event': 'unsubscribe',
            'subscription': 'simple.invalid',
            'status': 'error',
            'error': c.ERR_SUBSCRIPTION_NOT_FOUND,
        }

        await shark.shutdown()

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

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'ok',
        }

        prov_subs = shark.service_receiver.provisional_subscriptions
        assert prov_subs == {}

        conf_subs = shark.service_receiver.confirmed_subscriptions
        sessions = conf_subs['simple.topic']
        assert sessions == {session}

        # Test message from client to server
        await session.on_client_event(
            {
                'event': 'message',
                'subscription': 'simple.topic',
                'data': {'foo': 'bar'},
            }
        )

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + 'simple.topic'

        # This message has no filters and will arrive.
        await redis.publish_json(
            redis_topic,
            {'subscription': 'simple.topic', 'data': {'baz': 'foo'}},
        )

        # This message has an invalid subscription.
        await redis.publish_json(
            redis_topic,
            {
                'subscription': 'simple.othertopic',
                'data': {'never': 'arrives'},
            },
        )

        # This message has incorrect JSON
        await redis.publish(redis_topic, '{')

        # This message will arrive on anonymous sessions only.
        await redis.publish_json(
            redis_topic,
            {
                'subscription': 'simple.topic',
                'session_id': None,
                'data': {'arrives': True},
            },
        )

        # This message will not arrive on sessions with a session_id.
        await redis.publish_json(
            redis_topic,
            {
                'subscription': 'simple.topic',
                'session_id': 'sess_123',
                'data': {'arrives': False},
            },
        )

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': 'simple.topic',
                'data': {'baz': 'foo'},
            },
            {
                'event': 'message',
                'subscription': 'simple.topic',
                'data': {'arrives': True},
            },
        ]

        client.log = []

        await session.on_client_event(
            {
                'event': 'unsubscribe',
                'subscription': 'simple.topic',
            }
        )
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

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple_auth.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple_auth.topic',
            'status': 'error',
            'error': c.ERR_AUTH_REQUIRED,
        }

        await self._auth_session(session)

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple_auth.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple_auth.topic',
            'status': 'ok',
        }

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + 'simple_auth.topic'

        # This message has no filters and will arrive.
        await redis.publish_json(
            redis_topic,
            {'subscription': 'simple_auth.topic', 'data': {'foo': 'bar'}},
        )

        # This message will arrive on an invalid session only.
        await redis.publish_json(
            redis_topic,
            {
                'subscription': 'simple_auth.topic',
                'session_id': 'sess_invalid',
                'data': {'arrives': False},
            },
        )

        # This message will arrive in the current session.
        await redis.publish_json(
            redis_topic,
            {
                'subscription': 'simple_auth.topic',
                'session_id': 'sess_123',
                'data': {'arrives': True},
            },
        )

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': 'simple_auth.topic',
                'data': {'foo': 'bar'},
            },
            {
                'event': 'message',
                'subscription': 'simple_auth.topic',
                'data': {'arrives': True},
            },
        ]

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_authorizer(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'authorizer.topic',
            'status': 'error',
            'error': c.ERR_AUTH_REQUIRED,
        }

        await self._auth_session(session)

        with aioresponses() as mock:
            # Authorizer is unavailable.
            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'authorizer.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'error',
                'error': c.ERR_SERVICE_UNAVAILABLE,
            }

        with aioresponses() as mock:
            # Mock authorizer
            authorizer_url = 'http://auth-service/auth/authorizer/'

            mock.post(
                authorizer_url,
                payload={
                    'status': 'error',
                },
            )

            mock.post(
                authorizer_url,
                payload={
                    'status': 'error',
                    'error': 'test error',
                },
            )

            mock.post(
                authorizer_url,
                payload={
                    'status': 'ok',
                },
            )

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'authorizer.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'error',
                'error': c.ERR_UNAUTHORIZED,
            }

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'authorizer.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'error',
                'error': 'test error',
            }

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'authorizer.topic',
                    'extra': 'foo',
                    'other': 'bar',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'authorizer.topic',
                'status': 'ok',
                'extra': 'foo',
            }

            # Ensure we passed the right arguments to the authorizer endpoint.
            requests = mock.requests[('POST', URL(authorizer_url))]
            r1, r2, r3 = requests
            assert (
                r1.kwargs['json']
                == r2.kwargs['json']
                == {
                    'subscription': 'authorizer.topic',
                    'session_id': 'sess_123',
                }
            )
            assert r3.kwargs['json'] == {
                'subscription': 'authorizer.topic',
                'session_id': 'sess_123',
                'extra': 'foo',
            }

        assert client.log == []

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_authorizer_data(self):
        """
        Test authorizer fields & data.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        conf = TEST_CONFIG['SERVICES']['complex']

        await self._auth_session(session)

        with aioresponses() as mock:
            # Mock authorizer
            mock.post(
                conf['authorizer'],
                payload={
                    'status': 'ok',
                    'capabilities': 'foo',
                    'plan_type': 'bar',
                },
            )

            endpoints = (
                conf['before_subscribe'],
                conf['on_subscribe'],
                conf['before_unsubscribe'],
                conf['on_unsubscribe'],
            )

            for endpoint in endpoints:
                mock.post(endpoint, payload={'status': 'ok'})

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'complex.topic',
                }
            )

            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'ok',
            }

            assert client.log == []

            await shark.shutdown()

            for endpoint in endpoints:
                requests = mock.requests[('POST', URL(endpoint))]
                assert len(requests) == 1
                r = requests[0]

                assert r.kwargs['json'] == {
                    'subscription': 'complex.topic',
                    'session_id': 'sess_123',
                    'capabilities': 'foo',
                }

    @pytest.mark.asyncio
    async def test_subscription_periodic_authorizer(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        await self._auth_session(session)

        with aioresponses() as mock:
            # Mock authorizer
            authorizer_url = 'http://auth-service/auth/authorizer/'

            mock.post(
                authorizer_url,
                payload={
                    'status': 'ok',
                },
            )

            mock.post(
                authorizer_url,
                payload={
                    'status': 'ok',
                },
            )

            mock.post(
                authorizer_url,
                payload={
                    'status': 'error',
                },
            )

            mock.post(
                authorizer_url,
                payload={
                    'status': 'error',
                    'error': 'no longer authorized',
                },
            )

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'periodic_authorizer.topic',
                }
            )

            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'periodic_authorizer.topic',
                'status': 'ok',
            }

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'periodic_authorizer.topic2',
                }
            )

            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'periodic_authorizer.topic2',
                'status': 'ok',
            }

            await asyncio.sleep(0.1)

            assert client.log == []

            await asyncio.sleep(0.2)

            assert client.log.pop(0) == {
                'event': 'unsubscribe',
                'subscription': 'periodic_authorizer.topic',
                'error': c.ERR_UNAUTHORIZED,
            }

            assert client.log.pop(0) == {
                'event': 'unsubscribe',
                'subscription': 'periodic_authorizer.topic2',
                'error': 'no longer authorized',
            }

        assert client.log == []

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_subscription_authorizer_data_periodic(self):
        """
        Test authorization change callback.

        Test authorization change callback is called when authorizer fields
        change during periodic authorization.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        await self._auth_session(session)

        conf = TEST_CONFIG['SERVICES']['periodic_authorizer_with_fields']

        with aioresponses() as mock:
            # Mock authorizer
            mock.post(
                conf['authorizer'],
                payload={
                    'status': 'ok',
                    'capabilities': 'foo',
                },
            )

            mock.post(
                conf['authorizer'],
                payload={
                    'status': 'ok',
                    'capabilities': 'bar',
                },
            )

            mock.post(
                conf['on_subscribe'],
                payload={
                    'status': 'ok',
                },
            )
            mock.post(
                conf['on_authorization_change'],
                payload={
                    'status': 'ok',
                },
            )

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'periodic_authorizer_with_fields.topic',
                }
            )

            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'periodic_authorizer_with_fields.topic',
                'status': 'ok',
            }

            await asyncio.sleep(0.3)

            assert client.log == []

            (request,) = mock.requests[('POST', URL(conf['on_subscribe']))]
            assert request.kwargs['json'] == {
                'subscription': 'periodic_authorizer_with_fields.topic',
                'session_id': 'sess_123',
                'capabilities': 'foo',
            }

            (request,) = mock.requests[
                ('POST', URL(conf['on_authorization_change']))
            ]
            assert request.kwargs['json'] == {
                'subscription': 'periodic_authorizer_with_fields.topic',
                'session_id': 'sess_123',
                'capabilities': 'bar',
            }

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

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'complex.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'error': c.ERR_UNHANDLED_EXCEPTION,
            }

            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(
                conf['before_subscribe'],
                payload={
                    'status': 'error',
                    'error': 'before subscribe error',
                },
            )

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'complex.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'error': 'before subscribe error',
            }

            req_list_1 = mock.requests[('POST', URL(conf['authorizer']))]
            req_list_2 = mock.requests[('POST', URL(conf['before_subscribe']))]
            for request in req_list_1 + req_list_2:
                assert request.kwargs['json'] == {
                    'session_id': 'sess_123',
                    'subscription': 'complex.topic',
                }

        # Test successful subscription with extra field and messages
        with aioresponses() as mock:
            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(conf['before_subscribe'], payload={'status': 'ok'})
            mock.post(conf['on_subscribe'], payload={'doesnt': 'matter'})
            mock.post(conf['on_message'], payload={'status': 'ok'})
            mock.post(
                conf['on_message'],
                payload={
                    'status': 'error',
                    'error': 'on message error',
                },
            )
            mock.post(
                conf['on_message'],
                payload={
                    'status': 'error',
                    'error': 'on message error',
                    'data': {'extra': 'data'},
                },
            )
            mock.post(
                conf['on_message'],
                payload={
                    'status': 'ok',
                    'data': None,
                },
            )
            mock.post(
                conf['on_message'],
                payload={'status': 'ok', 'data': {'reply': True}},
            )

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'complex.topic',
                    'extra': 'hello',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
            }

            # Successful but no reply
            await session.on_client_event(
                {
                    'event': 'message',
                    'subscription': 'complex.topic',
                    'extra': 'irrelevant',
                }
            )

            await session.on_client_event(
                {
                    'event': 'message',
                    'subscription': 'complex.topic',
                    'extra': 'irrelevant',
                    'data': {'foo': 'bar', 'baz': 123},
                }
            )
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': 'on message error',
            }

            await session.on_client_event(
                {
                    'event': 'message',
                    'subscription': 'complex.topic',
                    'extra': 'irrelevant',
                    'data': {'foo': 'bar', 'baz': 123},
                }
            )
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': 'on message error',
                'data': {'extra': 'data'},
            }

            await session.on_client_event(
                {
                    'event': 'message',
                    'subscription': 'complex.topic',
                    'extra': 'irrelevant',
                }
            )
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
            }

            await session.on_client_event(
                {
                    'event': 'message',
                    'subscription': 'complex.topic',
                    'extra': 'irrelevant',
                }
            )
            assert client.log.pop() == {
                'event': 'message',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
                'data': {'reply': True},
            }

            req_list_1 = mock.requests[('POST', URL(conf['authorizer']))]
            req_list_2 = mock.requests[('POST', URL(conf['before_subscribe']))]
            req_list_3 = mock.requests[('POST', URL(conf['on_subscribe']))]
            for request in req_list_1 + req_list_2 + req_list_3:
                assert request.kwargs['json'] == {
                    'session_id': 'sess_123',
                    'subscription': 'complex.topic',
                    'extra': 'hello',
                }

            msg_reqs = mock.requests[('POST', URL(conf['on_message']))]
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
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + 'complex.topic'

        await redis.publish_json(
            redis_topic,
            {'subscription': 'complex.topic', 'data': {'foo': 'bar'}},
        )

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
            mock.post(
                conf['before_unsubscribe'],
                payload={
                    'status': 'error',
                    'error': 'before unsubscribe error',
                },
            )
            mock.post(conf['before_unsubscribe'], payload={'status': 'ok'})
            mock.post(conf['on_unsubscribe'], payload={'doesnt': 'matter'})

            await session.on_client_event(
                {
                    'event': 'unsubscribe',
                    'subscription': 'complex.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': c.ERR_UNHANDLED_EXCEPTION,
            }

            await session.on_client_event(
                {
                    'event': 'unsubscribe',
                    'subscription': 'complex.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
                'status': 'error',
                'extra': 'hello',
                'error': 'before unsubscribe error',
            }

            await session.on_client_event(
                {
                    'event': 'unsubscribe',
                    'subscription': 'complex.topic',
                }
            )
            assert client.log.pop() == {
                'event': 'unsubscribe',
                'subscription': 'complex.topic',
                'status': 'ok',
                'extra': 'hello',
            }

            req_list_1 = mock.requests[
                ('POST', URL(conf['before_unsubscribe']))
            ]
            req_list_2 = mock.requests[('POST', URL(conf['on_unsubscribe']))]
            for request in req_list_1 + req_list_2:
                assert request.kwargs['json'] == {
                    'session_id': 'sess_123',
                    'subscription': 'complex.topic',
                    'extra': 'hello',
                }

        # Test extra data in subscribe/unsubscribe callbacks
        with aioresponses() as mock:
            mock.post(conf['authorizer'], payload={'status': 'ok'})
            mock.post(
                conf['before_subscribe'],
                payload={
                    'status': 'ok',
                    'data': {'foo': 'subscribe'},
                },
            )
            mock.post(conf['on_subscribe'], payload={})
            mock.post(
                conf['before_unsubscribe'],
                payload={
                    'status': 'ok',
                    'data': {'foo': 'unsubscribe'},
                },
            )
            mock.post(conf['on_unsubscribe'], payload={})

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'complex.extra_data',
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': 'complex.extra_data',
                'status': 'ok',
                'data': {'foo': 'subscribe'},
            }

            await session.on_client_event(
                {
                    'event': 'unsubscribe',
                    'subscription': 'complex.extra_data',
                }
            )
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
        """
        Test throttled messages after closing connection.

        Ensure there are no issues after closing the connection when there are
        scheduled throttled messages.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session
        await self._auth_session(session)

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple.one',
            }
        )
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

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': 'complex.two',
                }
            )
            assert session.client.log.pop() == {
                'status': 'ok',
                'event': 'subscribe',
                'subscription': 'complex.two',
            }

        subscription_names = set(session.subscriptions.keys())
        assert subscription_names == {'simple.one', 'complex.two'}

        await session.on_close()

        assert session.subscriptions == {}

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_order_filter(self):
        """
        Test message order filter.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple_before_subscribe.topic'

        conf = TEST_CONFIG['SERVICES']['simple_before_subscribe']

        with aioresponses() as mock:
            mock.post(
                conf['before_subscribe'],
                payload={
                    'status': 'ok',
                    'options': {'order': 2},
                    'data': {'msg': 1},
                },
            )

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': subscription,
                }
            )
            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': subscription,
                'status': 'ok',
                'data': {'msg': 1},  # order 2
            }

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 1},
                'data': {'msg': 2},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 2},
                'data': {'msg': 3},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 3},
                'data': {'msg': 4},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 5},
                'data': {'msg': 5},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 4},
                'data': {'msg': 6},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 5},
                'data': {'msg': 7},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 6},
                'data': {'msg': 8},
            },
        )

        # Test a different order key, and float order
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 1.1, 'order_key': 'other'},
                'data': {'other': 1},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 1.3, 'order_key': 'other'},
                'data': {'other': 2},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 1.2, 'order_key': 'other'},
                'data': {'other': 3},
            },
        )

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'msg': 4},  # order 3
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'msg': 5},  # order 5
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'msg': 8},  # order 6
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'other': 1},  # other order 1
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'other': 2},  # other order 3
            },
        ]

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_order_filter_invalid(self):
        """
        Test invalid message order.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple.topic'

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': subscription,
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': subscription,
            'status': 'ok',
        }

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'order': 'invalid'},
                'data': {'foo': 'invalid'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'data': {'foo': 'bar'},
            },
        )

        redis.close()

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'invalid'},
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'bar'},
            },
        ]

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_filter_extra_fields(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple.topic'

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': subscription,
                'extra': 'bar',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': subscription,
            'status': 'ok',
            'extra': 'bar',
        }

        # Test message from server to client
        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'extra': 'foo',
                'data': {'test': 'foo'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'extra': 'bar',
                'data': {'test': 'bar'},
            },
        )

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'extra': 'bar',
                'data': {'test': 'bar'},
            }
        ]

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_ping_redis(self):
        """
        Test periodical Redis ping.
        """
        original_ping = aioredis.Redis.ping

        def dummy_ping(*args, **kwargs):
            dummy_ping.n_pings += 1
            if dummy_ping.n_pings < 2:
                return original_ping(*args, **kwargs)
            else:
                loop = asyncio.get_event_loop()
                return loop.create_future()

        dummy_ping.n_pings = 0

        shark = SocketShark(TEST_CONFIG_WITH_ALT_REDIS)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        # Have at least one subscription so we-re in pubsub mode.
        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': 'simple.topic',
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': 'simple.topic',
            'status': 'ok',
        }

        with patch('aioredis.Redis.ping', dummy_ping):
            task = asyncio.ensure_future(shark.run_service_receiver())
            await task  # Exits due to the timeout

        assert dummy_ping.n_pings == 3

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_rate_limit(self):
        """
        Make sure SocketShark retries 429 responses appropriately.
        """
        http_retry_config = TEST_CONFIG.copy()
        http_retry_config['HTTP']['tries'] = 2
        http_retry_config['HTTP']['wait'] = 1
        shark = SocketShark(http_retry_config)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple_before_subscribe.topic'

        conf = TEST_CONFIG['SERVICES']['simple_before_subscribe']

        with aioresponses() as mock:
            mock.post(
                conf['before_subscribe'],
                status=429,
                headers={
                    'X-Rate-Limit-Reset': '0.2',
                },
            )
            mock.post(
                conf['before_subscribe'],
                payload={
                    'status': 'ok',
                    'data': {},
                },
            )

            start_time = time.time()

            await session.on_client_event(
                {
                    'event': 'subscribe',
                    'subscription': subscription,
                }
            )

            assert client.log.pop() == {
                'event': 'subscribe',
                'subscription': subscription,
                'status': 'ok',
                'data': {},
            }

            end_time = time.time()

            # Make sure we've waited for the amount of seconds specified in the
            # header (not in the config)
            assert 0.1 < end_time - start_time < 0.3

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_ping_pong(self):
        """
        Test receiving a "ping" from the client and responding with a "pong".
        """
        shark = SocketShark(TEST_CONFIG)
        client = MockClient(shark)
        session = client.session

        message_from_client = {'event': 'ping'}
        await session.on_client_event(message_from_client)
        message_from_server = client.log.pop()
        assert message_from_server == {'event': 'pong', 'data': None}

        # Only a string payload is sent back to the client. Other data types
        # (e.g. int) should be ignored.
        message_from_client = {'event': 'ping', 'data': 123}
        await session.on_client_event(message_from_client)
        message_from_server = client.log.pop()
        assert message_from_server == {'event': 'pong', 'data': None}

        message_from_client = {'event': 'ping', 'data': 'hello'}
        await session.on_client_event(message_from_client)
        message_from_server = client.log.pop()
        assert message_from_server == {'event': 'pong', 'data': 'hello'}


class TestThrottle:
    """
    Test throttling messages.
    """

    @pytest.mark.asyncio
    async def test_throttle(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple.topic'

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': subscription,
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': subscription,
            'status': 'ok',
        }

        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 0.2},
                'data': {'foo': 'one'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 1, 'throttle_key': 'other'},
                'data': {'bar': 'one'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 'invalid'},
                'data': {'invalid': 'one'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'data': {'unthrottled': 'one'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 0.2},
                'data': {'foo': 'two'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 0.2},
                'data': {'foo': 'three'},
            },
        )

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'data': {'unthrottled': 'two'},
            },
        )

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        # Ensure the first message (foo, bar) is immediately published, and
        # that unthrottled messages are always published.
        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'one'},
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'bar': 'one'},
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'invalid': 'one'},
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'unthrottled': 'one'},
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'unthrottled': 'two'},
            },
        ]
        client.log = []

        await asyncio.sleep(0.2)

        # Ensure the last throttled message is eventually published.
        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'three'},
            }
        ]
        client.log = []

        await asyncio.sleep(0.2)

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 0.2},
                'data': {'foo': 'four'},
            },
        )

        # Wait for Redis to propagate the message
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        # Ensure we immediately publish after the throttle period is over.
        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'four'},
            }
        ]

        redis.close()

        await shark.shutdown()

    @pytest.mark.asyncio
    async def test_throttle_close(self):
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple.topic'

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': subscription,
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': subscription,
            'status': 'ok',
        }

        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 1},
                'data': {'foo': 'one'},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': 1},
                'data': {'foo': 'two'},
            },
        )

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'one'},
            }
        ]

        await session.on_client_event(
            {
                'event': 'unsubscribe',
                'subscription': subscription,
            }
        )

        redis.close()

        await shark.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('throttle,wait', [(0.8, 0.4), (0.4, 0.8)])
    async def test_throttle_slow_send(self, throttle, wait):
        """
        Ensure throttling behaves properly when sending messages is delayed.
        """
        shark = SocketShark(TEST_CONFIG)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session

        subscription = 'simple.topic'

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': subscription,
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': subscription,
            'status': 'ok',
        }

        redis_settings = TEST_CONFIG['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': throttle},
                'data': {'foo': 'one'},
            },
        )
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': throttle},
                'data': {'foo': 'two'},
            },
        )

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        # First message was delivered immediately.
        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'one'},
            }
        ]
        client.log = []

        # Slow down sending of the second message.
        original = Session.send_message

        async def dummy_send(*args):
            await asyncio.sleep(wait)
            await original(*args)

        Session.send_message = dummy_send

        # Wait long enough that we're sending the second message, but aren't
        # done sending yet.
        await asyncio.sleep(throttle)

        # No longer slow down sending subsequent messages.
        Session.send_message = original

        # Send a third message.
        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'options': {'throttle': throttle},
                'data': {'foo': 'three'},
            },
        )

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        # We're still sending the second message
        assert client.log == []

        await asyncio.sleep(wait)

        # We're now done sending the second message
        assert client.log.pop(0) == {
            'event': 'message',
            'subscription': subscription,
            'data': {'foo': 'two'},
        }

        if throttle - wait > 0:
            # We only need to wait (throttle-wait) for the third message to go
            # out (since sending the second message took wait and we're no
            # longer slowing down sending)
            await asyncio.sleep(throttle - wait)

        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'three'},
            }
        ]

        await session.on_client_event(
            {
                'event': 'unsubscribe',
                'subscription': subscription,
            }
        )

        redis.close()

        await shark.shutdown()


class TestWebsocket:
    """
    Test an actual WebSocket connection.
    """

    @property
    def ws_url(self):
        return 'http://{}:{}'.format(
            TEST_CONFIG['WS_HOST'], TEST_CONFIG['WS_PORT']
        )

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
                await ws.send_str(
                    json.dumps(
                        {
                            'event': 'subscribe',
                            'subscription': 'ws_test.hello',
                        }
                    )
                )
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
            requests = mock.requests[('POST', URL(conf['on_unsubscribe']))]
            assert len(requests) == 1
            assert requests[0].kwargs['json'] == {
                'subscription': 'ws_test.hello'
            }

            await shark.shutdown()

            done = True

        shark = SocketShark(TEST_CONFIG)
        asyncio.ensure_future(task())
        shark.start()

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
                await ws.send_str(
                    json.dumps(
                        {
                            'event': 'subscribe',
                            'subscription': 'ws_test.hello',
                        }
                    )
                )
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

                asyncio.ensure_future(shark.shutdown())

                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE
                await ws.close()

            await aiosession.close()

        shark = SocketShark(TEST_CONFIG)
        asyncio.ensure_future(task())
        shark.start()
        mock.stop()

        requests = mock.requests[('POST', URL(conf['on_unsubscribe']))]
        assert len(requests) == 1
        assert requests[0].kwargs['json'] == {'subscription': 'ws_test.hello'}

    def test_shutdown_connections(self):
        """
        Make sure we don't allow new WebSocket connections when shutting down.
        """
        # Pretend we have a subscription that takes a long time to close (so
        # we can sneak in a connection attempt).
        mock = aioresponses_delayed()
        conf = TEST_CONFIG['SERVICES']['ws_test']

        async def task():
            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()

            async with aiosession.ws_connect(self.ws_url) as ws:
                await ws.send_str(
                    json.dumps(
                        {
                            'event': 'subscribe',
                            'subscription': 'ws_test.hello',
                        }
                    )
                )
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

                asyncio.ensure_future(shark.shutdown())

                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE
                await ws.close()

                # Ensure we call the on_unsubscribe callback before the
                # stopping the patcher.
                await asyncio.sleep(0.1)

                mock.stop()

            # Attempt a new connection.
            with pytest.raises(aiohttp.ClientConnectionError):
                async with aiosession.ws_connect(self.ws_url) as ws:
                    raise AssertionError  # Whoops!

            await aiosession.close()

        shark = SocketShark(TEST_CONFIG)
        test_task = asyncio.ensure_future(task())
        shark.start()
        test_task.result()  # Raise any exceptions

        requests = mock.requests[('POST', URL(conf['on_unsubscribe']))]
        assert len(requests) == 1
        assert requests[0].kwargs['json'] == {'subscription': 'ws_test.hello'}

    def test_ping(self):
        """
        Ensure server sends periodic pings and disconnects timed out clients.
        """

        async def task():
            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()

            # Set up client with autoping disabled.
            async with aiosession.ws_connect(
                self.ws_url, autoping=False
            ) as ws:

                # Respond to ping in time
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.PING
                await ws.pong(msg.data)

                # Respond to ping late
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.PING
                await asyncio.sleep(0.1)
                await ws.pong(msg.data)

                # Ensure we get disconnected
                msg = await ws.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE

                await ws.close()

            await aiosession.close()
            asyncio.ensure_future(shark.shutdown())

        shark = SocketShark(TEST_CONFIG)
        asyncio.ensure_future(task())
        shark.start()

    def test_ping_2(self):
        """
        Test message receiving after a fail.

        Ensure we can receive service messages when sending a message fails due
        a timed out WebSocket.
        """

        async def task():
            subscription = 'simple.topic'

            # Set up Redis connection
            redis_settings = TEST_CONFIG['REDIS']
            redis = await aioredis.create_redis(
                (redis_settings['host'], redis_settings['port'])
            )
            redis_topic = redis_settings['channel_prefix'] + subscription

            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            aiosession = aiohttp.ClientSession()

            # Set up client with autoping disabled.
            async with aiosession.ws_connect(
                self.ws_url, autoping=False
            ) as ws1:

                await ws1.send_str(
                    json.dumps(
                        {
                            'event': 'subscribe',
                            'subscription': subscription,
                        }
                    )
                )
                msg = await ws1.receive()
                assert msg.type == aiohttp.WSMsgType.TEXT
                data = json.loads(msg.data)
                assert data == {
                    'event': 'subscribe',
                    'subscription': subscription,
                    'status': 'ok',
                }

                # Don't respond to ping
                msg = await ws1.receive()
                assert msg.type == aiohttp.WSMsgType.PING
                await asyncio.sleep(0.1)

                # Publish a message that may hit a closed WebSocket
                await redis.publish_json(
                    redis_topic,
                    {'subscription': subscription, 'data': {'baz': 'old'}},
                )

                # Ensure we get disconnected
                msg = await ws1.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE

                # Eventually close the socket.
                await ws1.close()

            # Set up a new connection
            ws2 = await aiosession.ws_connect(self.ws_url)
            await ws2.send_str(
                json.dumps(
                    {
                        'event': 'subscribe',
                        'subscription': subscription,
                    }
                )
            )

            msg = await ws2.receive()
            assert msg.type == aiohttp.WSMsgType.TEXT
            data = json.loads(msg.data)
            assert data == {
                'event': 'subscribe',
                'subscription': subscription,
                'status': 'ok',
            }

            # Publish a new message
            await redis.publish_json(
                redis_topic,
                {'subscription': subscription, 'data': {'baz': 'new'}},
            )

            # Wait until backend is ready.
            await asyncio.sleep(0.1)

            msg = await ws2.receive()
            assert msg.type == aiohttp.WSMsgType.TEXT
            data = json.loads(msg.data)
            assert data == {
                'event': 'message',
                'subscription': subscription,
                'data': {'baz': 'new'},
            }

            redis.close()

            await ws2.close()

            await aiosession.close()
            asyncio.ensure_future(shark.shutdown())

        shark = SocketShark(TEST_CONFIG)
        asyncio.ensure_future(task())
        shark.start()

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
            await ws2.send_str(
                json.dumps(
                    {
                        'event': 'subscribe',
                        'subscription': 'simple.hello',
                    }
                )
            )
            msg = await ws2.receive()
            assert msg.type == aiohttp.WSMsgType.TEXT
            data = json.loads(msg.data)
            assert data == {
                'event': 'subscribe',
                'subscription': 'simple.hello',
                'status': 'ok',
            }

            shark.redis_connections[0].redis.close()

            msg = await ws1.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE

            msg = await ws2.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE

            await aiosession.close()

        shark = SocketShark(TEST_CONFIG)
        asyncio.ensure_future(task())
        shark.start()


class TestRedisConnection:
    """
    Test throttling messages.
    """

    @pytest.mark.asyncio
    async def test_multiple_connections(self):
        shark = SocketShark(TEST_CONFIG_WITH_ALT_REDIS)
        await shark.prepare()
        client = MockClient(shark)
        session = client.session
        subscription = 'simple.topic'

        await session.on_client_event(
            {
                'event': 'subscribe',
                'subscription': subscription,
            }
        )
        assert client.log.pop() == {
            'event': 'subscribe',
            'subscription': subscription,
            'status': 'ok',
        }

        redis_settings = TEST_CONFIG_WITH_ALT_REDIS['REDIS']
        redis = await aioredis.create_redis(
            (redis_settings['host'], redis_settings['port'])
        )
        redis_topic = redis_settings['channel_prefix'] + subscription

        alt_redis_settings = TEST_CONFIG_WITH_ALT_REDIS['REDIS']
        alt_redis = await aioredis.create_redis(
            (alt_redis_settings['host'], alt_redis_settings['port'])
        )
        alt_redis_topic = alt_redis_settings['channel_prefix'] + subscription

        await redis.publish_json(
            redis_topic,
            {
                'subscription': subscription,
                'data': {'foo': 'one'},
            },
        )

        await alt_redis.publish_json(
            alt_redis_topic,
            {
                'subscription': subscription,
                'data': {'bar': 'one'},
            },
        )

        # Wait for Redis to propagate the messages
        await asyncio.sleep(0.1)

        has_messages = await shark.run_service_receiver(once=True)
        assert has_messages

        assert client.log == [
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'foo': 'one'},
            },
            {
                'event': 'message',
                'subscription': subscription,
                'data': {'bar': 'one'},
            },
        ]

        redis.close()
        alt_redis.close()

        await shark.shutdown()

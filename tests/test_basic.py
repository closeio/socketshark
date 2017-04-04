import asyncio

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
            auth_url = 'http://auth-service/auth/ticket/'

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

        await session.on_client_event({
            'event': 'message',
            'subscription': 'simple.topic',
            'data': { 'foo': 'bar' },
        })

        await session.on_client_event({
            'event': 'unsubscribe',
            'subscription': 'simple.topic',
        })
        assert client.log.pop() == {
            'status': 'ok',
            'event': 'unsubscribe',
        }

        assert not client.log

        await shark.shutdown()

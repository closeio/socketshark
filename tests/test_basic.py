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
        shark = SocketShark(TEST_CONFIG)
        client = MockClient()
        session = Session(shark, client)

        await session.on_client_event({'event': 'auth', 'method': 'x'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'auth',
            'error': c.ERR_AUTH_UNSUPPORTED,
        }

    @pytest.mark.asyncio
    async def test_auth_ticket(self):
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

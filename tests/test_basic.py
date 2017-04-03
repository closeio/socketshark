import asyncio

import pytest

from socketshark import events, SocketShark
from socketshark.session import Session


TEST_CONFIG = {
    'REDIS': {
        'host': 'localhost',
        'port': 6379,
        'channel_prefix': '',
    }
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
            'error': events.ERR_INVALID_EVENT,
        }

        await session.on_client_event('hello')
        assert client.log.pop() == {
            'status': 'error',
            'error': events.ERR_INVALID_EVENT,
        }

        await session.on_client_event({})
        assert client.log.pop() == {
            'status': 'error',
            'error': events.ERR_INVALID_EVENT,
        }

        await session.on_client_event({'event': None})
        assert client.log.pop() == {
            'status': 'error',
            'error': events.ERR_INVALID_EVENT,
        }

        await session.on_client_event({'event': ''})
        assert client.log.pop() == {
            'status': 'error',
            'event': '',
            'error': events.ERR_EVENT_NOT_FOUND,
        }

        await session.on_client_event({'event': 'hello'})
        assert client.log.pop() == {
            'status': 'error',
            'event': 'hello',
            'error': events.ERR_EVENT_NOT_FOUND,
        }

        assert not client.log

import asyncio

from socketshark import SocketShark


TEST_CONFIG = {
    'REDIS': {
        'host': 'localhost',
        'port': 6379,
        'channel_prefix': '',
    }
}


class TestBasic:
    def test_shark_init(self):
        shark = SocketShark(TEST_CONFIG)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(shark.prepare())

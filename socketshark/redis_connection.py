import aioredis
from aioredis.pubsub import Receiver

from .exceptions import RedisConnectionError
from .types import RedisSettings


class RedisConnection:
    """
    Redis connection wrapper.
    """

    def __init__(self, redis_settings: RedisSettings) -> None:
        self.host: str = redis_settings["host"]
        self.port: int = redis_settings["port"]
        self.db: int = redis_settings.get("db", 0)
        self.channel_prefix: str = redis_settings["channel_prefix"]
        self.ping_interval: int = redis_settings["ping_interval"]
        self.ping_timeout: int = redis_settings["ping_timeout"]

    async def connect(self) -> None:
        self.redis_receiver = Receiver()
        self.redis = await aioredis.create_redis(
            (self.host, self.port), db=self.db
        )

        # Some features (e.g. pinging) don't work on old Redis versions.
        info = await self.redis.info("server")
        version_info = info["server"]["redis_version"].split(".")
        major, minor = int(version_info[0]), int(version_info[1])
        if not (major > 3 or major == 3 and minor >= 2):
            msg = "Redis version must be at least 3.2"
            raise RedisConnectionError(msg)
        # We use a special channel to pass the stop message to the reader.
        self.stop_channel = self.redis_receiver.channel("_internal")

    @classmethod
    async def create(cls, redis_settings: RedisSettings) -> "RedisConnection":
        connection = cls(redis_settings)
        await connection.connect()
        return connection

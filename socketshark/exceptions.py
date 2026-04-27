from .types import EventErrorData


class EventError(Exception):
    def __init__(self, error: str, data: EventErrorData | None = None) -> None:
        """
        Take an error message and an optional dict with extra data.
        """
        super().__init__(error)
        self.error = error
        self.data = data


class RedisConnectionError(Exception):
    pass

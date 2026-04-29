from typing import TYPE_CHECKING

from ..types import MetricsProviderConfig

if TYPE_CHECKING:
    from .. import SocketShark


class LogMetrics:
    """
    Log metrics provider that prints each metric to the log.
    """

    def __init__(
        self, shark: "SocketShark", config: MetricsProviderConfig
    ) -> None:
        self.shark = shark
        self.connection_count = 0
        self.active_connections = 0

    def initialize(self) -> None:
        pass

    def set_ready(self, ready: bool) -> None:
        self.shark.log.debug("metrics", ready=ready)

    def decrease_connection_count(self) -> None:
        self.active_connections -= 1
        self.shark.log.debug(
            "metrics",
            active_connections=self.active_connections,
            total_connections=self.connection_count,
        )

    def increase_connection_count(self) -> None:
        self.connection_count += 1
        self.active_connections += 1
        self.shark.log.debug(
            "metrics",
            active_connections=self.active_connections,
            total_connections=self.connection_count,
        )

    def log_event(self, event: str, success: bool | None) -> None:
        self.shark.log.debug("metrics", evt=event, success=success)

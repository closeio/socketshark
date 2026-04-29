import importlib
from typing import TYPE_CHECKING, Any

from ..types import MetricsProviderConfig

if TYPE_CHECKING:
    from .. import SocketShark


class Metrics:
    """
    Main metrics class. Proxies events to configured metrics providers.
    """

    def __init__(self, shark: 'SocketShark') -> None:
        self.shark = shark
        metrics_config: dict[str, MetricsProviderConfig] = shark.config[
            'METRICS'
        ]
        self.providers: dict[str, Any] = {
            provider: self._get_provider(provider, settings)
            for provider, settings in metrics_config.items()
        }

    def _get_provider(
        self, provider: str, settings: MetricsProviderConfig
    ) -> Any:
        metrics_module = f'socketshark.metrics.{provider}'
        module = importlib.import_module(metrics_module)
        cls_name = f'{provider.capitalize()}Metrics'
        cls = getattr(module, cls_name)
        return cls(self.shark, settings)

    def initialize(self) -> None:
        for name, provider in self.providers.items():
            self.shark.log.info('initializing metrics', provider=name)
            provider.initialize()

    def decrease_connection_count(self) -> None:
        for provider in self.providers.values():
            provider.decrease_connection_count()

    def increase_connection_count(self) -> None:
        for provider in self.providers.values():
            provider.increase_connection_count()

    def set_ready(self, ready: bool) -> None:
        for provider in self.providers.values():
            provider.set_ready(ready)

    def log_event(self, event: str, success: bool | None) -> None:
        for provider in self.providers.values():
            provider.log_event(event, success)

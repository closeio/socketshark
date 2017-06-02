import importlib


class Metrics:
    """
    Main metrics class. Proxies events to configured metrics providers.
    """
    def __init__(self, shark):
        self.shark = shark
        metrics_config = shark.config['METRICS']
        self.providers = { # provider_name -> provider_instance
            provider: self._get_provider(provider, settings)
            for provider, settings in metrics_config.items()
        }

    def _get_provider(self, provider, settings):
        metrics_module = 'socketshark.metrics.{}'.format(provider)
        module = importlib.import_module(metrics_module)
        cls_name = '{}Metrics'.format(provider.capitalize())
        cls = getattr(module, cls_name)
        return cls(self.shark, settings)

    def initialize(self):
        for name, provider in self.providers.items():
            self.shark.log.info('initializing metrics', provider=name)
            provider.initialize()

    def set_ready(self, ready):
        for provider in self.providers.values():
            provider.set_ready(ready)

    def set_connection_count(self, count):
        for provider in self.providers.values():
            provider.set_connection_count(count)

    def log_event(self, event, success):
        for provider in self.providers.values():
            provider.log_event(event, success)

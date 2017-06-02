class LogMetrics:
    """
    Log metrics provider that prints each metric to the log.
    """
    def __init__(self, shark, config):
        self.shark = shark

    def initialize(self):
        pass

    def set_ready(self, ready):
        self.shark.log.debug('metrics', ready=ready)

    def set_connection_count(self, count):
        self.shark.log.debug('metrics', connection_count=count)

    def log_event(self, event, success):
        self.shark.log.debug('metrics', evt=event, success=success)

class LogMetrics:
    """
    Log metrics provider that prints each metric to the log.
    """
    def __init__(self, shark, config):
        self.shark = shark
        self.connection_count = 0

    def initialize(self):
        pass

    def set_ready(self, ready):
        self.shark.log.debug('metrics', ready=ready)

    def increase_connection_count(self):
        self.connection_count += 1
        self.shark.log.debug('metrics', total_connections=self.connection_count)

    def set_active_connection_count(self, count):
        self.shark.log.debug('metrics', active_connections=count)

    def log_event(self, event, success):
        self.shark.log.debug('metrics', evt=event, success=success)

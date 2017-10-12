class EventError(Exception):
    def __init__(self, error, data=None):
        """
        Takes an error message and an optional dict with extra data.
        """
        super().__init__(error)
        self.error = error
        self.data = data

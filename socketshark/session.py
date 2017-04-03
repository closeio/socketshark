from .events import Event


class Session:
    """
    Represents a client session
    """
    def __init__(self, shark, client):
        self.auth_info = {}
        self.subscriptions = set()
        self.shark = shark
        self.config = shark.config
        self.client = client

    async def on_client_event(self, data):
        await Event.from_data(self, data).full_process()

    async def on_service_event(self, data):
        # TODO: validate data
        await self.send(data)

    async def send(self, data):
        await self.client.send(data)

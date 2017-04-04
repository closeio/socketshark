from .events import Event


class Session:
    """
    Represents a client session
    """
    def __init__(self, shark, client):
        self.auth_info = {}
        self.subscriptions = set()
        self.extra_data = dict()  # {subscription: {extra_data}}
        self.shark = shark
        self.config = shark.config
        self.client = client

    async def on_client_event(self, data):
        await Event.from_data(self, data).full_process()

    async def on_service_event(self, data):
        # Filter by comparing filter_fields to auth_info
        subscription = data['subscription']
        service, topic = subscription.split('.', 1)
        service_config = self.config['SERVICES'][service]
        filter_fields = service_config.get('filter_fields', [])
        for field in filter_fields:
            if field in data:
                if self.auth_info.get(field) != data[field]:
                    # Message filtered.
                    return

        if subscription not in self.subscriptions:
            return

        msg = {
            'event': 'message',
            'subscription': subscription,
            'data': data['data'],
        }
        extra_data = self.extra_data[subscription]
        msg.update(extra_data)
        await self.send(msg)

    async def send(self, data):
        await self.client.send(data)

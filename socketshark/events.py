from typing import TYPE_CHECKING, Any, Optional

from . import constants as c
from .exceptions import EventError
from .subscription import Subscription
from .types import (
    AuthConfig,
    AuthInfo,
    ClientEventData,
    ClientMessage,
    Config,
    EventErrorData,
    ServiceRequestData,
    SubscriptionName,
)
from .utils import http_post

if TYPE_CHECKING:
    from . import SocketShark
    from .session import Session


class Event:
    @classmethod
    def from_data(cls, session: 'Session', data: Any) -> 'Event':
        if not isinstance(data, dict) or 'event' not in data:
            return InvalidEvent(session)

        event = data['event']

        # Make sure we don't echo back large messages.
        if not isinstance(event, str) or len(event) > c.MAX_EVENT_LENGTH:
            return InvalidEvent(session)

        event_class = {
            'auth': AuthEvent,
            'message': MessageEvent,
            'subscribe': SubscribeEvent,
            'unsubscribe': UnsubscribeEvent,
            'ping': PingEvent,
        }.get(event, UnknownEvent)

        return event_class(session, ClientEventData(data))

    def __init__(self, session: 'Session', data: ClientEventData) -> None:
        self.config: Config = session.config
        self.data = data
        self.event: str = data['event']
        self.extra_data: dict[str, Any] = {}
        self.session = session
        self.shark: 'SocketShark' = session.shark

    async def send_error(
        self,
        error: str,
        data: EventErrorData | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        msg: dict[str, Any] = {
            'event': self.event,
            'status': 'error',
            'error': error,
        }
        msg.update(self.extra_data)
        if data is not None:
            msg['data'] = data
        if extra_data is not None:
            msg.update(extra_data)
        await self.session.send(ClientMessage(msg))

    async def send_ok(
        self,
        data: dict[str, Any] | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        msg: dict[str, Any] = {
            'event': self.event,
            'status': 'ok',
        }
        msg.update(self.extra_data)
        if data is not None:
            msg['data'] = data
        if extra_data is not None:
            msg.update(extra_data)
        await self.session.send(ClientMessage(msg))

    async def process(self) -> bool:
        raise NotImplementedError

    async def full_process(self) -> bool | None:
        """
        Fully process an event and return whether it was successful.
        """
        try:
            return await self.process()
        except EventError as e:
            await self.send_error(e.error, data=e.data)
            return False


class InvalidEvent(Event):
    def __init__(self, session: 'Session') -> None:
        self.session = session
        self.event: str | None = None  # type: ignore[assignment]
        self.extra_data: dict[str, Any] = {}

    async def full_process(self) -> bool | None:
        msg = ClientMessage(
            {
                'status': 'error',
                'error': c.ERR_INVALID_EVENT,
            }
        )
        await self.session.send(msg)
        return False


class UnknownEvent(Event):
    async def process(self) -> bool:
        raise EventError(c.ERR_EVENT_NOT_FOUND)


class AuthEvent(Event):
    def __init__(self, session: 'Session', data: ClientEventData) -> None:
        super().__init__(session, data)
        self.auth_config: AuthConfig = self.config['AUTHENTICATION']
        self.method: str = data.get('method', c.DEFAULT_AUTH_METHOD)

    async def process(self) -> bool:
        if self.method not in self.auth_config:
            raise EventError(c.ERR_AUTH_UNSUPPORTED)

        # The only supported method.
        assert self.method == 'ticket'

        auth_method_config = self.auth_config[self.method]

        ticket = self.data.get('ticket')
        if not ticket:
            raise EventError(c.ERR_NEEDS_TICKET)

        auth_url = auth_method_config['validation_url']
        auth_fields = auth_method_config['auth_fields']
        result = await http_post(
            self.shark,
            auth_url,
            ServiceRequestData({'ticket': ticket}),
        )
        if result.get('status') != 'ok':
            raise EventError(result.get('error', c.ERR_AUTH_FAILED))
        auth_info = AuthInfo({field: result[field] for field in auth_fields})
        self.session.auth_info = auth_info
        self.session.log.debug('auth info', auth_info=auth_info)
        await self.send_ok()
        return True


class SubscriptionEvent(Event):
    def __init__(self, session: 'Session', data: ClientEventData) -> None:
        super().__init__(session, data)
        raw_name: str | None = data.get('subscription') or None
        subscription_name: SubscriptionName | None = (
            SubscriptionName(raw_name) if raw_name else None
        )
        self.subscription_name = subscription_name
        if subscription_name is not None:
            self.subscription = self.session.subscriptions.get(
                subscription_name, Subscription(self.config, session, data)
            )
        else:
            self.subscription = Subscription(self.config, session, data)
        self.extra_data = self.subscription.extra_data

    async def send_error(  # type: ignore[override]
        self,
        error: str,
        data: EventErrorData | None = None,
    ) -> None:
        await super().send_error(
            error,
            data=data,
            extra_data={
                'subscription': self.subscription_name,
            }
            if self.subscription_name
            else {},
        )

    async def send_ok(  # type: ignore[override]
        self,
        data: dict[str, Any] | None = None,
    ) -> None:
        await super().send_ok(
            data=data,
            extra_data={
                'subscription': self.subscription_name,
            }
            if self.subscription_name
            else {},
        )

    async def process(self) -> bool:
        self.subscription.validate()
        return True


class SubscribeEvent(SubscriptionEvent):
    async def process(self) -> bool:
        await super().process()
        await self.subscription.subscribe(self)
        return True


class MessageEvent(SubscriptionEvent):
    async def process(self) -> bool:
        await super().process()
        await self.subscription.message(self)
        return True


class UnsubscribeEvent(SubscriptionEvent):
    async def process(self) -> bool:
        await super().process()
        await self.subscription.unsubscribe(self)
        return True


class PingEvent(Event):
    async def send_pong(self, data: str | None = None) -> None:
        msg = ClientMessage({'event': 'pong', 'data': data})
        await self.session.send(msg)

    async def process(self) -> bool:
        raw_data = self.data.get('data')

        # If the "ping" event included some "data", send the same data back
        # so that pings and their pongs can be tied together. However, only
        # accept string data and only up to 128 characters.
        data: Optional[str] = None
        if isinstance(raw_data, str):
            data = raw_data[:128]

        await self.send_pong(data)
        return True

import asyncio
import datetime
import json
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from .redis_connection import RedisConnection
from .session import Session
from .types import ServiceEventData, SubscriptionName

if TYPE_CHECKING:
    from . import SocketShark


class ServiceReceiver:
    """
    Receives messages from services and forwards them to subscribing sessions.
    """

    def __init__(self, shark: "SocketShark") -> None:
        self.shark = shark

        self.subscriptions: set[SubscriptionName] = set()
        self.provisional_subscriptions: defaultdict[
            SubscriptionName, set[Session]
        ] = defaultdict(set)
        self.provisional_events: defaultdict[
            Session, list[ServiceEventData]
        ] = defaultdict(list)
        self.confirmed_subscriptions: defaultdict[
            SubscriptionName, set[Session]
        ] = defaultdict(set)

        self.redis_connections: list[RedisConnection] = shark.redis_connections
        self._stop: bool = False  # Stop flag

    def _channel(
        self, redis_connection: RedisConnection, name: SubscriptionName
    ) -> str:
        return redis_connection.channel_prefix + name

    async def ping_handler(self) -> None:
        handlers = [self._ping_handler(c) for c in self.redis_connections]
        await asyncio.gather(*handlers)

    async def _ping_handler(self, redis_connection: RedisConnection) -> None:
        ping_interval = redis_connection.ping_interval
        ping_timeout = redis_connection.ping_timeout
        if not ping_interval or not ping_timeout:
            return

        latency: float = 0
        ping: Any | None = None
        wait: Any | None = None

        try:
            while True:
                # Sleep before pings
                await asyncio.sleep(ping_interval - latency)

                self.shark.trace_log.debug("redis ping")

                start_time = time.time()

                ping = redis_connection.redis.ping()
                wait = asyncio.ensure_future(asyncio.sleep(ping_timeout))

                done, pending = await asyncio.wait(
                    [ping, wait], return_when=asyncio.FIRST_COMPLETED
                )

                if ping and ping in pending:
                    # Ping timeout
                    ping.cancel()
                    self.shark.log.warn("redis ping timeout")
                    self._stop = True
                    break

                latency = time.time() - start_time
                self.shark.trace_log.debug(
                    "redis pong", latency=round(latency, 3)
                )

        except asyncio.CancelledError:  # Cancelled by ping_handler.cancel()
            if ping:
                ping.cancel()
            if wait:
                wait.cancel()
            self.shark.log.debug("redis ping handler cancelled")
        except Exception:
            self.shark.log.exception("unhandled exception in ping handler")
            self._stop = True
        finally:
            if self._stop:
                await self.stop()

    async def reader(self, once: bool = False) -> list[bool | None] | None:
        self._stop = False
        try:
            ping_handler = asyncio.ensure_future(self.ping_handler())
            tasks = [
                self._reader_for_connection(connection, once=once)
                for connection in self.redis_connections
            ]
            result = await asyncio.gather(*tasks)
            return result
        except Exception:
            self.shark.log.exception("unhandled exception in receiver")
            return None
        finally:
            ping_handler.cancel()

    async def _handle_service_event(
        self,
        session: Session,
        service_event: ServiceEventData,
        received_at: datetime.datetime,
        queue_size: int,
    ) -> None:
        try:
            await session.on_service_event(
                service_event, received_at=received_at, queue_size=queue_size
            )
        except Exception:
            self.shark.log.exception("unhandled exception in receiver")

    def _dispatch_service_event(
        self,
        service_event: ServiceEventData,
        subscription_name: SubscriptionName,
        received_at: datetime.datetime,
        queue_size: int,
    ) -> list["asyncio.Task[None]"]:
        """
        Dispatch handling the given service event.

        Note that this truly *dispatches* the handling, meaning it does not
        block till the handling is complete. That said, all asyncio Tasks are
        returned to the caller, so the caller can await them if desired.
        """
        self.shark.trace_log.debug("service event", data=service_event)

        # The subscription arrays may change during execution, therefore we
        # create a snapshot before looping.
        confirmed_sessions = list(
            self.confirmed_subscriptions[subscription_name]
        )
        provisional_sessions = list(
            self.provisional_subscriptions[subscription_name]
        )
        tasks = [
            asyncio.create_task(
                self._handle_service_event(
                    session, service_event, received_at, queue_size
                )
            )
            for session in confirmed_sessions
        ]
        for session in provisional_sessions:
            self.provisional_events[session].append(service_event)
        return tasks

    async def _reader_for_connection(
        self, connection: RedisConnection, once: bool = False
    ) -> bool | None:
        prefix_length = len(connection.channel_prefix)
        if once and not connection.redis_receiver._queue.qsize():
            return False

        # Track in-flight tasks so we can drain them before returning.
        # Completed tasks remove themselves via the done callback, so the set
        # only ever contains tasks that are still running.
        pending_tasks: set[asyncio.Task[None]] = set()
        result: bool | None = None

        while True:
            queue_size = connection.redis_receiver._queue.qsize()
            redis_event = await connection.redis_receiver.get()
            received_at = datetime.datetime.now(datetime.timezone.utc)
            channel, event_json_bytes = redis_event
            if channel == connection.stop_channel:
                break

            subscription_name = SubscriptionName(
                channel.name.decode()[prefix_length:]
            )
            try:
                service_event = ServiceEventData(
                    json.loads(event_json_bytes.decode())
                )
                tasks = self._dispatch_service_event(
                    service_event, subscription_name, received_at, queue_size
                )
                for task in tasks:
                    pending_tasks.add(task)
                    task.add_done_callback(pending_tasks.discard)
            except json.decoder.JSONDecodeError:
                self.shark.log.exception("JSONDecodeError")
            except Exception:
                self.shark.log.exception("unhandled exception in receiver")
            if once and not connection.redis_receiver._queue.qsize():
                result = True
                break

        if pending_tasks:
            await asyncio.gather(*pending_tasks)
        return result

    async def add_provisional_subscription(
        self, session: Session, subscription_name: SubscriptionName
    ) -> None:
        if subscription_name not in self.subscriptions:
            self.subscriptions.add(subscription_name)
            await asyncio.gather(
                *[
                    c.redis.subscribe(
                        c.redis_receiver.channel(
                            self._channel(c, subscription_name)
                        )
                    )
                    for c in self.redis_connections
                ]
            )
        self.provisional_subscriptions[subscription_name].add(session)

    async def confirm_subscription(
        self, session: Session, subscription_name: SubscriptionName
    ) -> None:
        self.confirmed_subscriptions[subscription_name].add(session)
        self.provisional_subscriptions[subscription_name].remove(session)

        # Clear empty set
        if not self.provisional_subscriptions[subscription_name]:
            del self.provisional_subscriptions[subscription_name]

        # Flush provisional messages
        events = self.provisional_events.pop(session, [])
        for event in events:
            await session.on_service_event(event)

    async def delete_subscription(
        self, session: Session, subscription_name: SubscriptionName
    ) -> None:
        conf_set = self.confirmed_subscriptions[subscription_name]
        conf_set.discard(session)
        prov_set = self.provisional_subscriptions[subscription_name]
        prov_set.discard(session)

        # Clear empty set
        if not conf_set:
            del self.confirmed_subscriptions[subscription_name]
        if not prov_set:
            del self.provisional_subscriptions[subscription_name]

        if not conf_set and not prov_set:
            self.subscriptions.remove(subscription_name)
            await asyncio.gather(
                *[
                    c.redis.unsubscribe(
                        c.redis_receiver.channel(
                            self._channel(c, subscription_name)
                        )
                    )
                    for c in self.redis_connections
                    if not c.redis.closed
                ]
            )

    async def stop(self) -> None:
        self._stop = True
        for c in self.redis_connections:
            c.stop_channel.put_nowait(None)

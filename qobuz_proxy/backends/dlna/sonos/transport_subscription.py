"""One room's AVTransport event subscription: subscribing to its coordinator,
renewing before it lapses, subscribing afresh when the device forgot it, and
handing each parsed event on.

Events are an accelerator, never a requirement: whatever goes wrong here the
backend keeps polling, and `healthy` says whether the events can be relied on
to arrive (the first event after subscribing is the proof the path works).
"""

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

from .events import EventSource, GenaSubscription
from .transport_events import AVTRANSPORT_EVENT_PATH, TransportEvent, parse_transport_event

logger = logging.getLogger(__name__)

# Renew this long before the granted timeout runs out.
RENEWAL_MARGIN_SECONDS = 120.0
# Never wait less than this between renewal attempts (a very short timeout).
MIN_RENEWAL_WAIT_SECONDS = 5.0
# After a failed (re)subscribe, try again after this long.
RETRY_SECONDS = 30.0

EventCallback = Callable[[TransportEvent], Awaitable[None]]


class TransportEventStream:
    def __init__(self, source: EventSource, name: str, on_event: EventCallback) -> None:
        self._source = source
        self._name = name
        self._on_event = on_event
        self._target: Optional[tuple[str, int]] = None
        self._sub: Optional[GenaSubscription] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._last_event_at: Optional[float] = None
        self._stopped = True

    @property
    def healthy(self) -> bool:
        """Subscribed, and the device has proven it delivers (an event arrived
        on this subscription)."""
        return self._sub is not None and self._last_event_at is not None

    @property
    def subscribed(self) -> bool:
        return self._sub is not None

    async def start(self, ip: str, port: int) -> None:
        """Subscribe to `ip` (the room's coordinator) and keep the subscription
        alive. Replaces any earlier one (a retarget)."""
        await self.stop()
        self._stopped = False
        self._target = (ip, port)
        await self._subscribe()
        self._task = asyncio.create_task(self._keep_alive())

    async def stop(self) -> None:
        self._stopped = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self._drop()

    async def _drop(self) -> None:
        sub, self._sub = self._sub, None
        self._last_event_at = None
        if sub is not None:
            await self._source.subscriber.unsubscribe_service(sub)

    async def _subscribe(self) -> bool:
        if self._target is None:
            return False
        ip, port = self._target
        self._last_event_at = None
        self._sub = await self._source.subscriber.subscribe_service(
            ip, port, AVTRANSPORT_EVENT_PATH, self._source.callback_url, self._handle_notify
        )
        if self._sub is None:
            logger.info(f"[{self._name}] could not subscribe to transport events on {ip}")
            return False
        logger.info(
            f"[{self._name}] subscribed to transport events on {ip} "
            f"(timeout {self._sub.timeout_seconds}s)"
        )
        return True

    async def _handle_notify(self, body: str) -> None:
        event = parse_transport_event(body)
        if event is None:
            return
        self._last_event_at = time.monotonic()
        await self._on_event(event)

    async def _keep_alive(self) -> None:
        while not self._stopped:
            sub = self._sub
            if sub is None:
                await asyncio.sleep(RETRY_SECONDS)
                if not await self._subscribe():
                    continue
                sub = self._sub
                if sub is None:
                    continue
            wait = max(sub.timeout_seconds - RENEWAL_MARGIN_SECONDS, MIN_RENEWAL_WAIT_SECONDS)
            await asyncio.sleep(wait)
            if self._stopped or self._sub is not sub:
                continue
            renewed = await self._source.subscriber.renew_service(sub)
            if renewed is not None:
                self._sub = renewed
                continue
            # The device no longer knows the subscription: start afresh.
            logger.info(f"[{self._name}] transport event subscription lapsed; resubscribing")
            self._sub = None
            self._last_event_at = None
            if not await self._subscribe():
                continue


__all__ = ["EventCallback", "TransportEventStream"]

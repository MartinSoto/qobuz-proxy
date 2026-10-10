"""Running queue syncs one at a time, newest wish first.

Queue changes arrive in bursts (a run of drags, shuffle toggled right after an
edit, a quick tap-tap-tap through albums) and the device is slow compared to
the app, so every intermediate state that is already out of date by the time
its turn comes is wasted work and, worse, a source of device chatter. The
runner keeps at most one operation in flight and at most one waiting:

- a **reconcile** request replaces any other pending reconcile (only the
  newest desired queue matters) and waits for the running operation;
- a **load** request means a new queue generation: it cancels the running
  operation, which is for a queue that no longer exists, and replaces whatever
  was pending. A reconcile that arrives while a load is pending or running
  folds into it: the load's queue becomes the newest one.

Cancelling mid-operation is safe because SonosQueueSync re-reads the device
before it acts again whenever it was interrupted.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Sequence

from .audio_url import EntryKey
from .queue_sync import SonosQueueSync

logger = logging.getLogger(__name__)

ErrorCallback = Callable[[Exception], None]


@dataclass
class _Target:
    desired: list[EntryKey]
    anchor: Optional[EntryKey] = None
    load_start: Optional[EntryKey] = None  # set: this target is a load
    on_start_ready: Optional[Callable[[int], Awaitable[None]]] = None

    @property
    def is_load(self) -> bool:
        return self.load_start is not None


class QueueSyncRunner:
    def __init__(
        self, sync: SonosQueueSync, on_error: Optional[ErrorCallback] = None, name: str = "sonos"
    ) -> None:
        """
        Args:
            sync: The synchroniser to drive.
            on_error: Told about a sync that failed (QueueSyncError), including
                AnchorLost: the playing entry is gone from the desired queue
                and the owner must issue a load. Never raised into the caller.
        """
        self._sync = sync
        self._on_error = on_error
        self._name = name
        self._pending: Optional[_Target] = None
        self._worker: Optional["asyncio.Task[None]"] = None
        self._current: Optional["asyncio.Future[None]"] = None
        self._idle = asyncio.Event()
        self._idle.set()
        self._stopped = False

    # ------------------------------------------------------------------
    # Requests (never block)
    # ------------------------------------------------------------------

    def request_load(
        self,
        desired: Sequence[EntryKey],
        start: EntryKey,
        on_start_ready: Optional[Callable[[int], Awaitable[None]]] = None,
    ) -> None:
        """A new queue generation: publish `desired`, `start` first."""
        self._pending = _Target(list(desired), load_start=start, on_start_ready=on_start_ready)
        if self._current is not None and not self._current.done():
            self._current.cancel()  # for a queue that no longer exists
        self._kick()

    def request_reconcile(
        self, desired: Sequence[EntryKey], anchor: Optional[EntryKey] = None
    ) -> None:
        """The current generation changed: make the device match `desired`
        without disturbing `anchor` (the playing entry)."""
        pending = self._pending
        if pending is not None and pending.is_load:
            pending.desired = list(desired)  # the load's queue is now this one
        else:
            # Replaces any pending reconcile; if a load is running, this goes
            # after it (its own queue is stale already).
            self._pending = _Target(list(desired), anchor=anchor)
        self._kick()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def wait_idle(self) -> None:
        """Returns once nothing is running or waiting."""
        await self._idle.wait()

    async def stop(self) -> None:
        """Drop pending work and cancel what is running."""
        self._stopped = True
        self._pending = None
        if self._current is not None and not self._current.done():
            self._current.cancel()
        if self._worker is not None:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
        self._idle.set()

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    def _kick(self) -> None:
        if self._stopped:
            return
        self._idle.clear()
        if self._worker is None or self._worker.done():
            self._worker = asyncio.ensure_future(self._work())

    async def _work(self) -> None:
        try:
            while self._pending is not None and not self._stopped:
                target, self._pending = self._pending, None
                self._current = asyncio.ensure_future(self._run(target))
                try:
                    await self._current
                except asyncio.CancelledError:
                    if self._current.cancelled() and not self._stopped:
                        continue  # superseded by a newer load: go on with that one
                    raise
                except Exception as e:
                    logger.warning(f"[{self._name}] queue sync failed: {e}")
                    if self._on_error is not None:
                        self._on_error(e)
        finally:
            self._current = None
            if self._pending is None:
                self._idle.set()

    async def _run(self, target: _Target) -> None:
        if target.load_start is not None:
            await self._sync.load(
                target.desired, target.load_start, on_start_ready=target.on_start_ready
            )
        else:
            await self._sync.reconcile(target.desired, target.anchor)

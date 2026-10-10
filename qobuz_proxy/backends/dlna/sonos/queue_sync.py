"""Keeping a Sonos device's own queue equal to the Qobuz queue.

`SonosQueueSync` owns a *mirror* of what the device queue holds (a list of
EntryKey) and brings it to a desired list of keys with the fewest device
calls, never touching the playing entry (see queue_plan). Two entry points:

- `load`: a new queue generation — clear and publish the whole queue, starting
  with the entry that must play so playback can begin after the first batch
  while the rest is still being published.
- `reconcile`: the same generation changed (shuffle toggled, an edit) — plan
  the minimal change around the playing entry and apply it.

The device is the source of truth, not the mirror: every reconcile ends by
reading the queue back and comparing, and anything unexpected (a refused
operation, a mismatch, an interrupted earlier call) triggers a re-read and a
re-plan. The planner works from any starting queue, so a half-finished load or
someone else's edit is repaired by the next reconcile instead of needing a
special case. Per-entry content (stream URL and DIDL-Lite) comes from an
EntryBuilder, asked for a batch at a time, so metadata for the entries that
matter first is fetched first and a large queue never waits on its tail.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Optional, Protocol, Sequence

from .audio_url import EntryKey, parse_entry_url
from .client import ANY_UPDATE_ID, MAX_URIS_PER_ADD, QueueOpResult, QueuePage
from .queue_plan import InsertRun, MoveBlock, Op, RemoveRange, apply_ops, plan_reconcile

logger = logging.getLogger(__name__)

# How often a reconcile re-plans after the device disagreed before giving up.
MAX_ATTEMPTS = 3
# Entries per ContentDirectory.Browse request (the device's own maximum).
BROWSE_PAGE = 1000


class QueueSyncError(Exception):
    """The device queue could not be brought to the desired state."""


class AnchorLost(QueueSyncError):
    """The playing entry is not part of the desired queue (or not on the
    device any more), so it cannot be kept: the queue has to be loaded anew."""


@dataclass(frozen=True)
class BuiltEntry:
    """Everything the device needs for one queue entry."""

    url: str
    didl: str


class QueueDevice(Protocol):
    """The queue actions SonosClient provides (and tests fake)."""

    async def clear_queue(self) -> bool: ...

    async def add_uris_to_queue(
        self,
        uris: Sequence[str],
        metadata: Sequence[str],
        position: int = 0,
        update_id: int = ANY_UPDATE_ID,
    ) -> QueueOpResult: ...

    async def remove_queue_range(
        self, start: int, count: int, update_id: int = ANY_UPDATE_ID
    ) -> QueueOpResult: ...

    async def move_queue_range(
        self, start: int, count: int, insert_before: int, update_id: int = ANY_UPDATE_ID
    ) -> QueueOpResult: ...

    async def browse_queue(
        self, start: int = 0, count: int = BROWSE_PAGE
    ) -> Optional[QueuePage]: ...


class EntryBuilder(Protocol):
    """Turns queue keys into publishable entries (metadata, container choice,
    stream URL, DIDL-Lite). Called with at most MAX_URIS_PER_ADD keys."""

    async def build(self, keys: Sequence[EntryKey]) -> list[BuiltEntry]: ...


def _foreign(position: int) -> EntryKey:
    """A stand-in key for a device entry that is not one of ours (or a
    duplicate of one): unique, never desired, so it gets removed."""
    return EntryKey("foreign", -position)


class SonosQueueSync:
    def __init__(self, device: QueueDevice, builder: EntryBuilder, name: str = "sonos") -> None:
        self._device = device
        self._builder = builder
        self._name = name
        self._mirror: list[EntryKey] = []
        # Set before a device call that changes the queue, cleared once the
        # mirror reflects it: if that code is cancelled in between, the device
        # may hold something the mirror does not, and the next operation starts
        # by re-reading the queue.
        self._dirty = False
        self._lock = asyncio.Lock()

    @property
    def mirror(self) -> list[EntryKey]:
        """What the device queue is believed to hold, in order."""
        return list(self._mirror)

    # ------------------------------------------------------------------
    # Reading the device
    # ------------------------------------------------------------------

    async def resync(self) -> bool:
        """Re-read the whole device queue into the mirror."""
        async with self._lock:
            return await self._resync_locked()

    async def _resync_locked(self) -> bool:
        keys: list[EntryKey] = []
        seen: set[EntryKey] = set()
        start = 0
        total = 0
        while True:
            page = await self._device.browse_queue(start, BROWSE_PAGE)
            if page is None:
                logger.warning(f"[{self._name}] could not read the device queue")
                return False
            total = page.total
            for entry in page.entries:
                parsed = parse_entry_url(entry.url)
                key = parsed.key if parsed else _foreign(entry.position)
                if key in seen:
                    key = _foreign(entry.position)
                seen.add(key)
                keys.append(key)
            start += len(page.entries)
            if not page.entries or start >= total:
                break
        self._mirror = keys
        self._dirty = False
        return True

    # ------------------------------------------------------------------
    # Loading a new generation
    # ------------------------------------------------------------------

    async def load(
        self,
        desired: Sequence[EntryKey],
        start: EntryKey,
        on_start_ready: Optional[Callable[[int], Awaitable[None]]] = None,
    ) -> None:
        """Replace the device queue with `desired`, entry `start` first.

        The batch holding `start` is published first, then `on_start_ready` is
        awaited with the entry's 1-based position in the device queue (always
        1 at that moment) so the caller can begin playback; the rest follows:
        what comes after `start` is appended, what comes before it is inserted
        in front. Raises QueueSyncError if a step fails (after re-reading the
        queue: a following `reconcile` repairs whatever was published).
        """
        if start not in desired:
            raise QueueSyncError(f"start entry {start} is not in the queue to publish")
        async with self._lock:
            index = list(desired).index(start)
            if not await self._device.clear_queue():
                raise QueueSyncError("could not clear the device queue")
            self._mirror = []
            self._dirty = False

            forward = list(desired[index:])
            backward = list(desired[:index])
            ready_sent = False
            try:
                async for batch_keys, built in self._built_batches(forward):
                    await self._add(batch_keys, built, position=0)
                    if not ready_sent:
                        ready_sent = True
                        if on_start_ready is not None:
                            await on_start_ready(1)
                inserted = 0
                async for batch_keys, built in self._built_batches(backward):
                    await self._add(batch_keys, built, position=inserted + 1)
                    inserted += len(batch_keys)
            except QueueSyncError:
                # Leave the mirror describing what the device really holds, so
                # a following reconcile can repair a half-published queue.
                await self._resync_locked()
                raise

    # ------------------------------------------------------------------
    # Following changes within a generation
    # ------------------------------------------------------------------

    async def reconcile(
        self, desired: Sequence[EntryKey], anchor: Optional[EntryKey] = None
    ) -> None:
        """Bring the device queue to `desired` without disturbing `anchor`
        (the playing entry; None when nothing is playing).

        Raises AnchorLost if the anchor cannot be kept (the caller should
        `load` instead), QueueSyncError if the device would not end up right.
        """
        async with self._lock:
            if self._dirty and not await self._resync_locked():
                raise QueueSyncError("device queue unreadable after an interrupted change")
            for attempt in range(1, MAX_ATTEMPTS + 1):
                ops = plan_reconcile(self._mirror, desired, anchor)
                if ops is None:
                    raise AnchorLost(f"{anchor} cannot be kept")
                if ops and not await self._execute(ops):
                    logger.info(f"[{self._name}] attempt {attempt}: a step failed, re-reading")
                    if not await self._resync_locked():
                        raise QueueSyncError("device queue unreadable after a failed step")
                    continue
                # Trust but verify: the device decides what the queue is.
                if not await self._resync_locked():
                    raise QueueSyncError("device queue unreadable after publishing")
                if self._mirror == list(desired):
                    return
                logger.info(
                    f"[{self._name}] attempt {attempt}: device queue differs from the plan, "
                    "re-planning"
                )
            raise QueueSyncError(f"device queue still differs after {MAX_ATTEMPTS} attempts")

    # ------------------------------------------------------------------
    # Executing operations
    # ------------------------------------------------------------------

    async def _execute(self, ops: Sequence[Op]) -> bool:
        for op in ops:
            if isinstance(op, RemoveRange):
                self._dirty = True
                result = await self._device.remove_queue_range(op.start, op.count)
                if not result.ok:
                    return False
            elif isinstance(op, MoveBlock):
                self._dirty = True
                result = await self._device.move_queue_range(op.start, op.count, op.insert_before)
                if not result.ok:
                    return False
            else:
                if not await self._execute_insert(op):
                    return False
                continue
            self._mirror = apply_ops(self._mirror, [op])
            self._dirty = False
        return True

    async def _execute_insert(self, op: InsertRun) -> bool:
        offset = 0
        async for batch_keys, built in self._built_batches(list(op.keys)):
            try:
                await self._add(batch_keys, built, position=op.position + offset)
            except QueueSyncError:
                return False
            offset += len(batch_keys)
        return True

    async def _add(
        self, keys: Sequence[EntryKey], built: Sequence[BuiltEntry], position: int
    ) -> None:
        self._dirty = True
        result = await self._device.add_uris_to_queue(
            [b.url for b in built], [b.didl for b in built], position=position
        )
        if not result.ok:
            raise QueueSyncError(f"adding {len(keys)} entries at {position} failed")
        # What the device did, in the mirror's terms (an insert past the end appends).
        self._mirror = apply_ops(
            self._mirror, [InsertRun(position if position else len(self._mirror) + 1, tuple(keys))]
        )
        self._dirty = False

    async def _built_batches(
        self, keys: Sequence[EntryKey]
    ) -> AsyncIterator[tuple[list[EntryKey], list[BuiltEntry]]]:
        """Yield (keys, built entries) in batches the device accepts, building
        the next batch while the caller publishes the current one."""
        chunks = [
            list(keys[i : i + MAX_URIS_PER_ADD]) for i in range(0, len(keys), MAX_URIS_PER_ADD)
        ]
        pending: Optional["asyncio.Task[list[BuiltEntry]]"] = None
        try:
            if chunks:
                pending = asyncio.ensure_future(self._build(chunks[0]))
            for i, chunk in enumerate(chunks):
                assert pending is not None
                built = await pending
                pending = (
                    asyncio.ensure_future(self._build(chunks[i + 1]))
                    if i + 1 < len(chunks)
                    else None
                )
                yield chunk, built
        finally:
            if pending is not None and not pending.done():
                pending.cancel()

    async def _build(self, keys: Sequence[EntryKey]) -> list[BuiltEntry]:
        built = await self._builder.build(keys)
        if len(built) != len(keys):
            raise QueueSyncError(f"builder returned {len(built)} entries for {len(keys)} keys")
        for key, entry in zip(keys, built):
            parsed = parse_entry_url(entry.url)
            if parsed is None or parsed.key != key:
                raise QueueSyncError(f"built URL does not identify {key}: {entry.url}")
        return built

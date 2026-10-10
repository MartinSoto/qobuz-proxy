"""An in-memory Sonos queue that behaves the way the real device was measured
to (docs/sonos-queue-handover-design.md 9.2 and the 2026-10-10 probes): 16
entries per add, exact remove ranges, block moves, UpdateID checks, and the
UPnP error codes it answers with. Written independently of queue_plan.apply_ops
so the planner is checked against it rather than against itself (a test also
keeps the two in agreement).

It also supports what the tests need to provoke: one-shot failures, a reply
lost after the change was applied, and edits made behind the publisher's back.
"""

import re
from typing import Optional, Sequence

from qobuz_proxy.backends.dlna.sonos.client import (
    ANY_UPDATE_ID,
    ERROR_INVALID_ARGS,
    ERROR_NOT_COORDINATOR,
    ERROR_STALE_UPDATE_ID,
    MAX_URIS_PER_ADD,
    DeviceQueueEntry,
    QueueOpResult,
    QueuePage,
)


class FakeSonosQueue:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []  # (url, didl)
        self.update_id = 100
        self.calls: list[tuple] = []  # (action, *args) of every mutating call
        self.reads = 0
        self._fail_next: dict[str, int] = {}
        self._lose_reply: set[str] = set()
        self.unreadable = False

    # --- helpers for tests -------------------------------------------------

    @property
    def urls(self) -> list[str]:
        return [u for u, _ in self.entries]

    def fail_next(self, action: str, code: int = ERROR_INVALID_ARGS) -> None:
        """The next call of this action is refused with this UPnP error."""
        self._fail_next[action] = code

    def lose_reply_of_next(self, action: str) -> None:
        """The next call of this action is applied, but the reply never arrives."""
        self._lose_reply.add(action)

    def external_edit(self, entries: list[tuple[str, str]]) -> None:
        """Someone else rewrites the queue (another app, a retried call)."""
        self.entries = list(entries)
        self.update_id += 1

    # --- internals ----------------------------------------------------------

    def _gate(self, action: str, update_id: int) -> Optional[QueueOpResult]:
        self.calls.append((action,))
        if action in self._fail_next:
            return QueueOpResult(ok=False, error_code=self._fail_next.pop(action))
        if update_id != ANY_UPDATE_ID and update_id != self.update_id:
            return QueueOpResult(ok=False, error_code=ERROR_STALE_UPDATE_ID)
        return None

    def _done(self, action: str, **extra: Optional[int]) -> QueueOpResult:
        self.update_id += 1
        if action in self._lose_reply:
            self._lose_reply.discard(action)
            return QueueOpResult(ok=False, error_code=None)
        return QueueOpResult(
            ok=True, update_id=self.update_id, queue_length=len(self.entries), **extra
        )

    # --- the QueueDevice surface -------------------------------------------

    async def clear_queue(self) -> bool:
        self.calls.append(("clear",))
        self.entries = []
        self.update_id += 1
        return True

    async def add_uris_to_queue(
        self,
        uris: Sequence[str],
        metadata: Sequence[str],
        position: int = 0,
        update_id: int = ANY_UPDATE_ID,
    ) -> QueueOpResult:
        refused = self._gate("add", update_id)
        if refused:
            return refused
        if not 0 < len(uris) <= MAX_URIS_PER_ADD or len(uris) != len(metadata):
            return QueueOpResult(ok=False, error_code=ERROR_INVALID_ARGS)
        before = len(self.entries)
        at = before if position == 0 or position > before + 1 else position - 1
        self.calls[-1] = ("add", position, len(uris))
        self.entries[at:at] = list(zip(uris, metadata))
        return self._done("add", first_position=at + 1, added=len(uris))

    async def remove_queue_range(
        self, start: int, count: int, update_id: int = ANY_UPDATE_ID
    ) -> QueueOpResult:
        refused = self._gate("remove", update_id)
        if refused:
            return refused
        self.calls[-1] = ("remove", start, count)
        if start < 1 or count < 1 or start + count - 1 > len(self.entries):
            return QueueOpResult(ok=False, error_code=ERROR_INVALID_ARGS)
        del self.entries[start - 1 : start - 1 + count]
        return self._done("remove")

    async def move_queue_range(
        self, start: int, count: int, insert_before: int, update_id: int = ANY_UPDATE_ID
    ) -> QueueOpResult:
        refused = self._gate("move", update_id)
        if refused:
            return refused
        self.calls[-1] = ("move", start, count, insert_before)
        n = len(self.entries)
        if count < 1:
            return QueueOpResult(ok=False, error_code=ERROR_INVALID_ARGS)
        if start < 1 or start > n or insert_before > n + 1:
            return QueueOpResult(ok=False, error_code=ERROR_NOT_COORDINATOR)
        end = min(start + count - 1, n)
        if not start <= insert_before <= end + 1:
            block = self.entries[start - 1 : end]
            anchor = self.entries[insert_before - 1] if insert_before <= n else None
            del self.entries[start - 1 : end]
            at = len(self.entries) if anchor is None else self.entries.index(anchor)
            self.entries[at:at] = block
        return self._done("move")

    async def browse_queue(self, start: int = 0, count: int = 1000) -> Optional[QueuePage]:
        self.reads += 1
        if self.unreadable:
            return None
        window = self.entries[start : start + min(count, 1000)]
        return QueuePage(
            entries=[
                DeviceQueueEntry(
                    position=start + i + 1,
                    title=(re.search(r"<dc:title>(.*?)</dc:title>", didl) or [None, ""])[1],  # type: ignore[index]
                    url=url,
                )
                for i, (url, didl) in enumerate(window)
            ],
            total=len(self.entries),
            update_id=self.update_id,
        )

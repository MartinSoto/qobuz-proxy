"""Planning how to turn the device's queue into the desired one.

Pure logic, no I/O: given the entries the device holds and the entries it
should hold, produce the short list of operations that gets there, in the
three kinds the Sonos queue offers (remove a range, move a block, insert a
run). The one constraint that shapes everything: the entry that is playing
must never be removed or moved (doing so would interrupt playback, while
every other change around it is seamless — docs/sonos-queue-handover-design.md,
9.2), so it is the fixed point the rest is arranged around.

Positions in an operation are 1-based and refer to the queue as it is after
all earlier operations of the same plan, so a plan is applied front to back.
`apply_ops` implements the device's measured semantics and is both the
reference the planner is tested against and the model behind the fake device
used in tests.
"""

import bisect
from dataclasses import dataclass
from typing import Optional, Sequence, Union

from .audio_url import EntryKey

# How many entries one AddMultipleURIsToQueue call takes (see client.py).
INSERT_BATCH = 16


@dataclass(frozen=True)
class RemoveRange:
    start: int
    count: int


@dataclass(frozen=True)
class MoveBlock:
    """Move `count` entries from `start` so they end up directly before the
    entry that is at `insert_before` *before* the move (len + 1 = the end)."""

    start: int
    count: int
    insert_before: int


@dataclass(frozen=True)
class InsertRun:
    """Insert these entries as one contiguous run so the first takes
    `position` (len + 1 = append)."""

    position: int
    keys: tuple[EntryKey, ...]

    @property
    def calls(self) -> int:
        return -(-len(self.keys) // INSERT_BATCH)


Op = Union[RemoveRange, MoveBlock, InsertRun]


class PlanError(ValueError):
    """An operation sequence that the device would refuse."""


def apply_ops(current: Sequence[EntryKey], ops: Sequence[Op]) -> list[EntryKey]:
    """What the queue looks like after `ops`, with the device's own rules:
    a remove range must lie inside the queue exactly; a move target past
    len + 1 is refused, one inside or right next to the moved block changes
    nothing; an insert past the end appends."""
    queue = list(current)
    for op in ops:
        if isinstance(op, RemoveRange):
            if op.count < 1 or op.start < 1 or op.start + op.count - 1 > len(queue):
                raise PlanError(f"remove range out of bounds: {op} on {len(queue)} entries")
            del queue[op.start - 1 : op.start - 1 + op.count]
        elif isinstance(op, MoveBlock):
            if op.count < 1 or op.start < 1 or op.start > len(queue):
                raise PlanError(f"move source out of bounds: {op} on {len(queue)} entries")
            if op.start + op.count - 1 > len(queue) or op.insert_before > len(queue) + 1:
                raise PlanError(f"move out of bounds: {op} on {len(queue)} entries")
            if op.start <= op.insert_before <= op.start + op.count:
                continue  # inside or directly after the block: nothing to do
            block = queue[op.start - 1 : op.start - 1 + op.count]
            target = None if op.insert_before == len(queue) + 1 else queue[op.insert_before - 1]
            del queue[op.start - 1 : op.start - 1 + op.count]
            at = len(queue) if target is None else queue.index(target)
            queue[at:at] = block
        else:
            at = min(max(op.position, 1), len(queue) + 1) - 1
            queue[at:at] = op.keys
    return queue


def plan_cost(ops: Sequence[Op]) -> int:
    """Device calls a plan needs (an insert run is chunked at INSERT_BATCH)."""
    return sum(op.calls if isinstance(op, InsertRun) else 1 for op in ops)


def _longest_increasing(values: Sequence[int]) -> set[int]:
    """Indexes into `values` of one longest strictly increasing subsequence."""
    tails: list[int] = []  # smallest tail value of an increasing run of each length
    tail_index: list[int] = []
    previous = [-1] * len(values)
    for i, v in enumerate(values):
        k = bisect.bisect_left(tails, v)
        if k == len(tails):
            tails.append(v)
            tail_index.append(i)
        else:
            tails[k] = v
            tail_index[k] = i
        previous[i] = tail_index[k - 1] if k else -1
    kept: set[int] = set()
    i = tail_index[-1] if tail_index else -1
    while i != -1:
        kept.add(i)
        i = previous[i]
    return kept


def _rebuild_ops(
    current: Sequence[EntryKey], desired: Sequence[EntryKey], anchor: Optional[EntryKey]
) -> list[Op]:
    """Throw everything away except the anchor and put the rest back in order:
    the always-available way, and the cheaper one when most of the queue
    moved (a fresh shuffle)."""
    ops: list[Op] = []
    if anchor is None:
        if current:
            ops.append(RemoveRange(1, len(current)))
        if desired:
            ops.append(InsertRun(1, tuple(desired)))
        return ops
    a = list(current).index(anchor)
    if a + 1 < len(current):
        ops.append(RemoveRange(a + 2, len(current) - a - 1))
    if a > 0:
        ops.append(RemoveRange(1, a))
    d = list(desired).index(anchor)
    if d > 0:
        ops.append(InsertRun(1, tuple(desired[:d])))
    if d + 1 < len(desired):
        ops.append(InsertRun(d + 2, tuple(desired[d + 1 :])))
    return ops


def plan_reconcile(
    current: Sequence[EntryKey],
    desired: Sequence[EntryKey],
    anchor: Optional[EntryKey] = None,
) -> Optional[list[Op]]:
    """Operations turning `current` into `desired` without touching `anchor`.

    Entries are identified by key; keys within each list must be unique.
    Returns None when the anchor cannot be kept — it is not in both lists —
    because then the playing entry itself has to change and the caller must
    reload the queue instead.
    """
    if anchor is not None and (anchor not in current or anchor not in desired):
        return None

    wanted = set(desired)
    position_in_desired = {key: i for i, key in enumerate(desired)}
    queue = list(current)
    ops: list[Op] = []

    # 1. Removals: every entry that should not be there, as ranges, last first
    #    so the positions of the ones still to come stay valid.
    runs: list[tuple[int, int]] = []
    for i, key in enumerate(queue, start=1):
        if key in wanted:
            continue
        if runs and runs[-1][0] + runs[-1][1] == i:
            runs[-1] = (runs[-1][0], runs[-1][1] + 1)
        else:
            runs.append((i, 1))
    for start, count in reversed(runs):
        ops.append(RemoveRange(start, count))
    queue = [key for key in queue if key in wanted]

    # 2. Order: keep the longest stretch that is already in the right relative
    #    order — with the anchor in it — and move everything else, each entry
    #    to directly after its nearest predecessor that is present.
    order = [position_in_desired[key] for key in queue]
    if anchor is None:
        keep = _longest_increasing(order)
    else:
        a = queue.index(anchor)
        left = [i for i in range(a) if order[i] < order[a]]
        right = [i for i in range(a + 1, len(queue)) if order[i] > order[a]]
        keep = {a}
        keep |= {left[i] for i in _longest_increasing([order[i] for i in left])}
        keep |= {right[i] for i in _longest_increasing([order[i] for i in right])}
    keep_keys = {queue[i] for i in keep}
    present = set(queue)
    for key in sorted((k for k in queue if k not in keep_keys), key=position_in_desired.get):  # type: ignore[arg-type]
        d = position_in_desired[key]
        predecessor = next(
            (desired[i] for i in range(d - 1, -1, -1) if desired[i] in present), None
        )
        p = queue.index(predecessor) if predecessor is not None else -1
        j = queue.index(key)
        if j == p + 1:
            continue
        ops.append(MoveBlock(start=j + 1, count=1, insert_before=p + 2))
        del queue[j]
        queue.insert(p + 1 if j > p else p, key)

    # 3. Insertions: each run of missing entries goes in directly after the
    #    entry that precedes it in the desired order.
    i = 0
    while i < len(desired):
        if desired[i] in present:
            i += 1
            continue
        end = i
        while end < len(desired) and desired[end] not in present:
            end += 1
        position = queue.index(desired[i - 1]) + 2 if i else 1
        run = tuple(desired[i:end])
        ops.append(InsertRun(position, run))
        queue[position - 1 : position - 1] = run
        present.update(run)
        i = end

    rebuild = _rebuild_ops(current, desired, anchor)
    return rebuild if plan_cost(rebuild) < plan_cost(ops) else ops

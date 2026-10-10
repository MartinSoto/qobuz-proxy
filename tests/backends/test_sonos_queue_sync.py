"""SonosQueueSync against a fake Sonos queue: loading a generation, following
changes around the playing entry, and repairing itself when the device does
not do what was planned."""

import asyncio
import random
from typing import Sequence

import pytest

from qobuz_proxy.backends.dlna.sonos.audio_url import EntryKey, parse_entry_url
from qobuz_proxy.backends.dlna.sonos.queue_plan import (
    InsertRun,
    MoveBlock,
    RemoveRange,
    apply_ops,
)
from qobuz_proxy.backends.dlna.sonos.queue_sync import (
    AnchorLost,
    BuiltEntry,
    QueueSyncError,
    SonosQueueSync,
)

from tests.backends.fake_sonos_queue import FakeSonosQueue


def K(*items: int) -> list[EntryKey]:
    return [EntryKey(f"T{i}", i) for i in items]


def url_for(key: EntryKey) -> str:
    return (
        f"http://proxy:7120/audio/{key.track_id}?format=flac&depth=16&rate=44100"
        f"&item={key.track_id}_{key.queue_item_id}&s=ab12cd34&v=5&i={key.queue_item_id}"
    )


class FakeBuilder:
    def __init__(self) -> None:
        self.batches: list[list[int]] = []
        self.gate: asyncio.Event | None = None  # when set, building blocks on it
        self.block_from_batch: int | None = None

    async def build(self, keys: Sequence[EntryKey]) -> list[BuiltEntry]:
        self.batches.append([k.queue_item_id for k in keys])
        if self.gate is not None and len(self.batches) >= (self.block_from_batch or 1):
            await self.gate.wait()
        return [
            BuiltEntry(
                url=url_for(k),
                didl=f"<DIDL-Lite><item><dc:title>{k.track_id}</dc:title></item></DIDL-Lite>",
            )
            for k in keys
        ]


def make() -> tuple[SonosQueueSync, FakeSonosQueue, FakeBuilder]:
    device, builder = FakeSonosQueue(), FakeBuilder()
    return SonosQueueSync(device, builder, name="test"), device, builder


def device_keys(device: FakeSonosQueue) -> list[EntryKey]:
    return [parse_entry_url(u).key for u in device.urls]  # type: ignore[union-attr]


class TestFakeDeviceAgreesWithTheReferenceModel:
    """The fake and queue_plan.apply_ops were written separately; they must
    describe the same device."""

    async def test_random_operation_sequences(self) -> None:
        rng = random.Random(5)
        for _ in range(300):
            device = FakeSonosQueue()
            size = rng.randint(0, 12)
            keys = K(*range(1, size + 1))
            device.entries = [(url_for(k), "<d/>") for k in keys]
            model = list(keys)
            next_id = 100
            for _ in range(rng.randint(1, 10)):
                kind = rng.choice(["remove", "move", "insert"])
                n = len(model)
                if kind == "remove" and n:
                    start = rng.randint(1, n)
                    count = rng.randint(1, n - start + 1)
                    assert (await device.remove_queue_range(start, count)).ok
                    model = apply_ops(model, [RemoveRange(start, count)])
                elif kind == "move" and n:
                    start = rng.randint(1, n)
                    count = rng.randint(1, n - start + 1)
                    before = rng.randint(1, n + 1)
                    assert (await device.move_queue_range(start, count, before)).ok
                    model = apply_ops(model, [MoveBlock(start, count, before)])
                else:
                    position = rng.randint(1, n + 1)
                    new = K(next_id)
                    next_id += 1
                    assert (
                        await device.add_uris_to_queue([url_for(new[0])], ["<d/>"], position)
                    ).ok
                    model = apply_ops(model, [InsertRun(position, tuple(new))])
                assert device_keys(device) == model


class TestLoad:
    async def test_publishes_everything_in_order(self) -> None:
        sync, device, _ = make()
        desired = K(*range(1, 41))

        await sync.load(desired, start=desired[0])

        assert device_keys(device) == desired
        assert sync.mirror == desired

    async def test_starts_with_the_entry_that_must_play_and_calls_back_after_the_first_batch(
        self,
    ) -> None:
        sync, device, _ = make()
        desired = K(*range(1, 41))
        seen: list[tuple[int, list[int]]] = []

        async def ready(position: int) -> None:
            seen.append((position, [k.queue_item_id for k in device_keys(device)]))

        await sync.load(desired, start=desired[19], on_start_ready=ready)  # item 20

        assert [p for p, _ in seen] == [1]
        # at that moment the device held just the first batch, starting at the entry to play
        assert seen[0][1] == list(range(20, 36))
        # and in the end everything is there in the original order
        assert device_keys(device) == desired

    async def test_entries_before_the_start_are_put_in_front_in_order(self) -> None:
        sync, device, _ = make()
        desired = K(*range(1, 8))

        await sync.load(desired, start=desired[4])  # item 5

        assert [k.queue_item_id for k in device_keys(device)] == [1, 2, 3, 4, 5, 6, 7]

    async def test_uses_batches_of_at_most_sixteen(self) -> None:
        sync, device, builder = make()
        desired = K(*range(1, 51))

        await sync.load(desired, start=desired[0])

        assert all(len(b) <= 16 for b in builder.batches)
        adds = [c for c in device.calls if c[0] == "add"]
        assert all(c[2] <= 16 for c in adds)
        assert len(adds) == 4  # 16 + 16 + 16 + 2

    async def test_replaces_whatever_the_device_held(self) -> None:
        sync, device, _ = make()
        device.entries = [(url_for(k), "<d/>") for k in K(900, 901)]

        await sync.load(K(1, 2, 3), start=K(1)[0])

        assert device_keys(device) == K(1, 2, 3)

    async def test_an_empty_start_is_refused(self) -> None:
        sync, device, _ = make()

        with pytest.raises(QueueSyncError):
            await sync.load(K(1, 2), start=K(9)[0])

    async def test_a_failed_step_leaves_the_mirror_matching_the_device(self) -> None:
        sync, device, _ = make()
        desired = K(*range(1, 41))
        original_add = device.add_uris_to_queue
        adds = 0

        async def flaky(*args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal adds
            adds += 1
            if adds == 2:
                device.fail_next("add")
            return await original_add(*args, **kwargs)

        device.add_uris_to_queue = flaky  # type: ignore[method-assign]

        with pytest.raises(QueueSyncError):
            await sync.load(desired, start=desired[0])

        assert sync.mirror == device_keys(device)  # the half-published queue, as it really is
        assert 0 < len(sync.mirror) < 40
        # ... and a following reconcile completes the job
        await sync.reconcile(desired, anchor=desired[0])
        assert device_keys(device) == desired


class TestReconcile:
    async def _loaded(self, desired: list[EntryKey]) -> tuple[SonosQueueSync, FakeSonosQueue]:
        sync, device, _ = make()
        await sync.load(desired, start=desired[0])
        device.calls.clear()
        return sync, device

    async def test_nothing_to_do_makes_no_changing_call(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))

        await sync.reconcile(K(1, 2, 3), anchor=K(1)[0])

        assert device.calls == []

    async def test_play_next_is_one_insert(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))

        await sync.reconcile(K(1, 9, 2, 3), anchor=K(1)[0])

        assert device_keys(device) == K(1, 9, 2, 3)
        assert device.calls == [("add", 2, 1)]

    async def test_a_drag_is_one_move(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3, 4, 5))

        await sync.reconcile(K(1, 5, 2, 3, 4), anchor=K(1)[0])

        assert device_keys(device) == K(1, 5, 2, 3, 4)
        assert device.calls == [("move", 5, 1, 2)]

    async def test_removal(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3, 4, 5))

        await sync.reconcile(K(1, 2, 5), anchor=K(2)[0])

        assert device_keys(device) == K(1, 2, 5)
        assert device.calls == [("remove", 3, 2)]

    async def test_a_new_shuffle_around_the_playing_entry(self) -> None:
        desired = K(*range(1, 41))
        sync, device = await self._loaded(desired)
        shuffled = list(desired)
        random.Random(3).shuffle(shuffled)
        anchor = desired[0]
        playing_url = device.urls[0]

        await sync.reconcile(shuffled, anchor=anchor)

        assert device_keys(device) == shuffled
        assert url_for(anchor) == playing_url
        # a rebuild around the anchor: a couple of removals and a few adds, nowhere near 40 calls
        assert len(device.calls) <= 2 + 2 * 3
        removed = [c for c in device.calls if c[0] == "remove"]
        assert removed and all(c[1] >= 2 for c in removed)  # position 1 holds the playing entry

    async def test_a_lost_anchor_asks_for_a_reload(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))

        with pytest.raises(AnchorLost):
            await sync.reconcile(K(7, 8), anchor=K(1)[0])

        assert device_keys(device) == K(1, 2, 3)  # untouched

    async def test_without_an_anchor_the_queue_can_be_replaced(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))

        await sync.reconcile(K(7, 8), anchor=None)

        assert device_keys(device) == K(7, 8)


class TestRepair:
    async def _loaded(self, desired: list[EntryKey]) -> tuple[SonosQueueSync, FakeSonosQueue]:
        sync, device, _ = make()
        await sync.load(desired, start=desired[0])
        device.calls.clear()
        return sync, device

    async def test_a_refused_step_is_retried_after_re_reading(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3, 4, 5))
        device.fail_next("move")

        await sync.reconcile(K(1, 5, 2, 3, 4), anchor=K(1)[0])

        assert device_keys(device) == K(1, 5, 2, 3, 4)

    async def test_a_reply_lost_after_the_change_does_not_duplicate_entries(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))
        device.lose_reply_of_next("add")

        await sync.reconcile(K(1, 9, 2, 3), anchor=K(1)[0])

        assert device_keys(device) == K(1, 9, 2, 3)

    async def test_someone_else_changing_the_queue_is_noticed_and_repaired(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))
        device.external_edit([(url_for(k), "<d/>") for k in K(3, 2, 1, 50)])  # behind our back

        await sync.reconcile(K(1, 2, 3, 4), anchor=K(1)[0])

        assert device_keys(device) == K(1, 2, 3, 4)

    async def test_foreign_and_duplicate_entries_are_cleared_out(self) -> None:
        sync, device = await self._loaded(K(1, 2))
        device.external_edit(
            [
                (url_for(K(1)[0]), "<d/>"),
                ("x-sonos-spotify:spotify%3atrack%3a1", "<d/>"),  # not ours
                (url_for(K(1)[0]), "<d/>"),  # a duplicate of an entry of ours
                (url_for(K(2)[0]), "<d/>"),
            ]
        )
        await sync.resync()

        await sync.reconcile(K(1, 2), anchor=K(1)[0])

        assert device_keys(device) == K(1, 2)

    async def test_a_device_that_keeps_refusing_ends_in_an_error(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3, 4, 5))
        original = device.move_queue_range

        async def refuse(*args, **kwargs):  # type: ignore[no-untyped-def]
            device.fail_next("move")
            return await original(*args, **kwargs)

        device.move_queue_range = refuse  # type: ignore[method-assign]

        with pytest.raises(QueueSyncError):
            await sync.reconcile(K(1, 5, 2, 3, 4), anchor=K(1)[0])

    async def test_an_unreadable_device_ends_in_an_error(self) -> None:
        sync, device = await self._loaded(K(1, 2, 3))
        device.unreadable = True

        with pytest.raises(QueueSyncError):
            await sync.reconcile(K(1, 2, 3, 4), anchor=K(1)[0])

    async def test_a_cancelled_change_is_repaired_by_the_next_one(self) -> None:
        sync, device, builder = make()
        desired = K(*range(1, 41))
        builder.gate = asyncio.Event()
        builder.block_from_batch = 2  # the second batch of the load hangs
        task = asyncio.create_task(sync.load(desired, start=desired[0]))
        for _ in range(100):
            await asyncio.sleep(0)
            if len(device.entries) == 16:
                break
        assert len(device.entries) == 16

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        builder.gate.set()
        builder.block_from_batch = None
        await sync.reconcile(desired, anchor=desired[0])

        assert device_keys(device) == desired

    async def test_a_change_cancelled_after_the_device_applied_it_is_not_repeated(self) -> None:
        """The device did the insert, the mirror was not updated yet: the next
        change must start by re-reading the queue, not re-plan from a stale
        mirror and add the entry a second time."""
        sync, device = await self._loaded(K(1, 2, 3))
        original = device.add_uris_to_queue
        gate = asyncio.Event()

        async def applied_then_stuck(*args, **kwargs):  # type: ignore[no-untyped-def]
            result = await original(*args, **kwargs)
            await gate.wait()
            return result

        device.add_uris_to_queue = applied_then_stuck  # type: ignore[method-assign]
        task = asyncio.create_task(sync.reconcile(K(1, 9, 2, 3), anchor=K(1)[0]))
        for _ in range(100):
            await asyncio.sleep(0)
            if len(device.entries) == 4:
                break
        assert len(device.entries) == 4
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        device.add_uris_to_queue = original  # type: ignore[method-assign]
        device.calls.clear()

        await sync.reconcile(K(1, 9, 2, 3), anchor=K(1)[0])

        assert device_keys(device) == K(1, 9, 2, 3)
        assert device.calls == []  # nothing left to change: no second insert

    async def test_a_builder_that_returns_the_wrong_entries_is_an_error(self) -> None:
        sync, device, builder = make()

        async def wrong(keys: Sequence[EntryKey]) -> list[BuiltEntry]:
            return [BuiltEntry(url_for(EntryKey("T999", 999)), "<d/>") for _ in keys]

        builder.build = wrong  # type: ignore[method-assign]

        with pytest.raises(QueueSyncError):
            await sync.load(K(1, 2), start=K(1)[0])


class TestResync:
    async def test_reads_a_queue_longer_than_one_page(self) -> None:
        sync, device, _ = make()
        keys = K(*range(1, 1201))
        device.entries = [(url_for(k), "<d/>") for k in keys]

        assert await sync.resync()

        assert sync.mirror == keys
        assert device.reads == 2  # 1000 + 200

    async def test_an_unreadable_device_reports_failure(self) -> None:
        sync, device, _ = make()
        device.unreadable = True

        assert not await sync.resync()


class TestRealisticSequence:
    async def test_a_shuffled_queue_edited_a_few_times(self) -> None:
        """The shape of the live capture (O19): shuffle on, add, play next,
        remove, reorders, shuffle off — each change one or two device calls."""
        sync, device, _ = make()
        original = K(*range(255, 266))
        shuffled = K(255, 262, 256, 259, 261, 265, 260, 264, 263, 258, 257)
        await sync.load(shuffled, start=shuffled[0])
        anchor = K(262)[0]  # the playing entry after the user skipped

        steps = [
            shuffled + K(266),  # add
            K(
                255, 262, 266, 256, 259, 261, 264, 260, 263, 267, 258, 257, 265
            ),  # play next re-arranges
            K(255, 262, 256, 259, 261, 264, 260, 263, 267, 258, 257, 265),  # remove 266
            K(255, 262, 261, 259, 264, 260, 256, 263, 267, 258, 257, 265),  # drags
            [k for k in original if k.queue_item_id not in (266,)],  # shuffle off, original order
        ]
        for desired in steps:
            await sync.reconcile(desired, anchor=anchor if anchor in desired else None)
            assert device_keys(device) == desired

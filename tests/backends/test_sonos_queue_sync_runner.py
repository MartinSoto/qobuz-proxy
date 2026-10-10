"""QueueSyncRunner: one operation at a time, newest request wins, a load
supersedes everything older, failures are reported not raised."""

import asyncio

from qobuz_proxy.backends.dlna.sonos.audio_url import EntryKey, parse_entry_url
from qobuz_proxy.backends.dlna.sonos.queue_sync import SonosQueueSync
from qobuz_proxy.backends.dlna.sonos.queue_sync_runner import QueueSyncRunner

from tests.backends.fake_sonos_queue import FakeSonosQueue
from tests.backends.test_sonos_queue_sync import FakeBuilder, K


def make() -> tuple[QueueSyncRunner, FakeSonosQueue, FakeBuilder, list[Exception]]:
    device, builder = FakeSonosQueue(), FakeBuilder()
    errors: list[Exception] = []
    runner = QueueSyncRunner(SonosQueueSync(device, builder), on_error=errors.append, name="test")
    return runner, device, builder, errors


def keys_on(device: FakeSonosQueue) -> list[EntryKey]:
    return [parse_entry_url(u).key for u in device.urls]  # type: ignore[union-attr]


async def loaded(runner: QueueSyncRunner, desired: list[EntryKey]) -> None:
    runner.request_load(desired, desired[0])
    await runner.wait_idle()


class TestRunner:
    async def test_a_load_publishes_the_queue(self) -> None:
        runner, device, _, errors = make()

        await loaded(runner, K(*range(1, 21)))

        assert keys_on(device) == K(*range(1, 21))
        assert errors == []

    async def test_only_the_newest_pending_reconcile_runs(self) -> None:
        runner, device, builder, _ = make()
        await loaded(runner, K(1, 2, 3))
        builder.gate = asyncio.Event()  # the next insert hangs in the builder
        runner.request_reconcile(K(1, 8, 2, 3), anchor=K(1)[0])  # starts, then hangs
        await asyncio.sleep(0.01)

        runner.request_reconcile(K(1, 8, 9, 2, 3), anchor=K(1)[0])  # waits
        runner.request_reconcile(K(1, 8, 9, 5, 2, 3), anchor=K(1)[0])  # replaces the waiting one
        builder.gate.set()
        await runner.wait_idle()

        assert keys_on(device) == K(1, 8, 9, 5, 2, 3)
        # the skipped intermediate (1,8,9,2,3) was never published: entries 9 and 5
        # went out together as one run, not one after the other
        assert [9, 5] in builder.batches
        assert [9] not in builder.batches and [5] not in builder.batches

    async def test_a_load_cancels_the_running_operation_and_replaces_pending_work(self) -> None:
        runner, device, builder, _ = make()
        await loaded(runner, K(*range(1, 41)))
        builder.gate = asyncio.Event()
        runner.request_reconcile(K(*range(1, 41)) + K(100), anchor=K(1)[0])  # hangs building 100
        await asyncio.sleep(0.01)
        runner.request_reconcile(K(*range(1, 41)) + K(101), anchor=K(1)[0])  # pending

        builder.gate.set()
        runner.request_load(K(500, 501, 502), K(500)[0])  # a different queue altogether
        await runner.wait_idle()

        assert keys_on(device) == K(500, 501, 502)

    async def test_a_reconcile_during_a_load_ends_up_applied(self) -> None:
        runner, device, builder, _ = make()
        desired = K(*range(1, 41))
        builder.gate = asyncio.Event()
        runner.request_load(desired, desired[0])  # hangs building its first batch
        await asyncio.sleep(0.01)

        newer = K(*range(1, 41)) + K(900)
        runner.request_reconcile(newer, anchor=desired[0])
        builder.gate.set()
        await runner.wait_idle()

        assert keys_on(device) == newer

    async def test_a_failure_is_reported_and_does_not_stop_the_runner(self) -> None:
        runner, device, _, errors = make()
        await loaded(runner, K(1, 2, 3))

        runner.request_reconcile(K(7, 8), anchor=K(1)[0])  # the playing entry is gone
        await runner.wait_idle()
        assert len(errors) == 1 and "cannot be kept" in str(errors[0])

        runner.request_reconcile(K(1, 2, 3, 4), anchor=K(1)[0])
        await runner.wait_idle()
        assert keys_on(device) == K(1, 2, 3, 4)

    async def test_stop_cancels_and_drops_pending_work(self) -> None:
        runner, device, builder, _ = make()
        await loaded(runner, K(1, 2, 3))
        builder.gate = asyncio.Event()
        runner.request_reconcile(K(1, 8, 2, 3), anchor=K(1)[0])
        await asyncio.sleep(0.01)
        runner.request_reconcile(K(1, 8, 9, 2, 3), anchor=K(1)[0])

        await runner.stop()
        builder.gate.set()
        await asyncio.sleep(0.01)

        assert keys_on(device) == K(1, 2, 3)
        runner.request_reconcile(K(1, 2, 3, 4), anchor=K(1)[0])  # ignored once stopped
        await asyncio.sleep(0.01)
        assert keys_on(device) == K(1, 2, 3)

    async def test_wait_idle_returns_immediately_when_nothing_was_requested(self) -> None:
        runner, _, _, _ = make()

        await asyncio.wait_for(runner.wait_idle(), timeout=1)

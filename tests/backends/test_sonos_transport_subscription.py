"""TransportEventStream: subscribing to a room's AVTransport events, keeping
the subscription alive, and telling when the events can be relied on."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from qobuz_proxy.backends.dlna.sonos import transport_subscription as ts
from qobuz_proxy.backends.dlna.sonos.events import EventSource, SonosEventSubscriber
from qobuz_proxy.backends.dlna.sonos.transport_subscription import TransportEventStream

from tests.backends.test_sonos_events import _FakeServiceDevice

FIXTURES = Path(__file__).parent.parent / "fixtures" / "sonos_events"
PLAYING = (FIXTURES / "avt_playing.xml").read_text()


async def eventually(condition, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached"
        await asyncio.sleep(0.005)


@pytest.fixture
async def device():  # type: ignore[no-untyped-def]
    fake = _FakeServiceDevice(timeout_header="Second-0")
    runner, port = await fake.start()
    fake.port = port
    yield fake
    await runner.cleanup()


def make_stream(on_event=None):  # type: ignore[no-untyped-def]
    subscriber = SonosEventSubscriber()
    source = EventSource(subscriber, "http://cb/sonos-events")
    return TransportEventStream(source, "Room", on_event or AsyncMock()), subscriber


class TestStream:
    async def test_subscribes_but_is_not_healthy_until_an_event_arrives(self, device) -> None:
        stream, subscriber = make_stream()

        await stream.start("127.0.0.1", device.port)
        try:
            assert stream.subscribed
            assert not stream.healthy  # nothing has proven the device can reach us yet

            (sid,) = subscriber._handlers
            await subscriber._handlers[sid](PLAYING)

            assert stream.healthy
        finally:
            await stream.stop()

    async def test_events_are_parsed_and_handed_on(self, device) -> None:
        on_event = AsyncMock()
        stream, subscriber = make_stream(on_event)
        await stream.start("127.0.0.1", device.port)
        try:
            (sid,) = subscriber._handlers
            await subscriber._handlers[sid](PLAYING)

            event = on_event.await_args.args[0]
            assert event.transport_state == "PLAYING"
        finally:
            await stream.stop()

    async def test_something_that_is_not_a_transport_event_is_ignored(self, device) -> None:
        on_event = AsyncMock()
        stream, subscriber = make_stream(on_event)
        await stream.start("127.0.0.1", device.port)
        try:
            (sid,) = subscriber._handlers
            await subscriber._handlers[sid]("<not-an-event/>")

            on_event.assert_not_called()
            assert not stream.healthy
        finally:
            await stream.stop()

    async def test_an_unreachable_device_leaves_it_unsubscribed_and_unhealthy(self) -> None:
        stream, _ = make_stream()

        await stream.start("127.0.0.1", 1)  # nothing listens there
        try:
            assert not stream.subscribed and not stream.healthy
        finally:
            await stream.stop()

    async def test_stopping_unsubscribes(self, device) -> None:
        stream, subscriber = make_stream()
        await stream.start("127.0.0.1", device.port)

        await stream.stop()

        assert device.unsubscribe_headers and subscriber._handlers == {}
        assert not stream.subscribed

    async def test_starting_again_moves_to_the_new_device(self, device) -> None:
        stream, subscriber = make_stream()
        await stream.start("127.0.0.1", device.port)
        first = stream._sub

        await stream.start("127.0.0.1", device.port)  # a retarget

        try:
            assert stream._sub is not first
            assert len(subscriber._handlers) == 1  # the old one is gone
        finally:
            await stream.stop()


class TestKeepingItAlive:
    async def test_the_subscription_is_renewed_before_it_lapses(self, device, monkeypatch) -> None:
        monkeypatch.setattr(ts, "RENEWAL_MARGIN_SECONDS", 0.0)
        monkeypatch.setattr(ts, "MIN_RENEWAL_WAIT_SECONDS", 0.02)
        stream, _ = make_stream()
        await stream.start("127.0.0.1", device.port)
        try:
            await eventually(
                lambda: any("CALLBACK" not in h and "SID" in h for h in device.subscribe_headers)
            )
            assert stream.subscribed
        finally:
            await stream.stop()

    async def test_a_lapsed_subscription_is_replaced_by_a_new_one(
        self, device, monkeypatch
    ) -> None:
        monkeypatch.setattr(ts, "RENEWAL_MARGIN_SECONDS", 0.0)
        monkeypatch.setattr(ts, "MIN_RENEWAL_WAIT_SECONDS", 0.02)
        stream, _ = make_stream()
        await stream.start("127.0.0.1", device.port)
        first_sid = stream._sub.sid  # type: ignore[union-attr]
        device.refuse_renewal = True
        try:
            await eventually(lambda: stream._sub is not None and stream._sub.sid != first_sid)
            assert stream.subscribed
            assert not stream.healthy  # it must prove itself again
        finally:
            await stream.stop()

    async def test_a_failed_resubscribe_is_retried(self, monkeypatch) -> None:
        monkeypatch.setattr(ts, "RETRY_SECONDS", 0.02)
        stream, subscriber = make_stream()
        calls = []
        sub = object()

        async def flaky(*args, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(1)
            return None if len(calls) == 1 else sub

        subscriber.subscribe_service = flaky  # type: ignore[method-assign]
        subscriber.unsubscribe_service = AsyncMock()  # type: ignore[method-assign]
        stream._target = ("127.0.0.1", 1)
        stream._stopped = False
        # first attempt fails; the keep-alive loop tries again after RETRY_SECONDS
        stream._sub = None
        task = asyncio.create_task(stream._keep_alive())
        try:
            await eventually(lambda: stream._sub is sub)
        finally:
            stream._stopped = True
            task.cancel()

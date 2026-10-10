"""SonosQueueBackend: starting playback by publishing the whole queue to the
device, jumping within it afterwards, following queue changes, and noticing
which entry the device plays. The device is a fake queue behind a real
SonosClient subclass, so the whole play() path runs."""

from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlsplit

from qobuz_proxy.backends.dlna.capabilities import DLNACapabilities, QOBUZ_QUALITY_CD
from qobuz_proxy.backends.dlna.sonos.audio_url import EntryKey, parse_entry_url
from qobuz_proxy.backends.dlna.sonos.client import SonosClient
from qobuz_proxy.backends.dlna.sonos.queue_backend import SonosQueueBackend
from qobuz_proxy.backends.types import BackendTrackMetadata, PlaybackState, PlayResult
from qobuz_proxy.playback.queue import QobuzQueue, QueueVersion
from qobuz_proxy.playback.stream_resolver import ResolvedStream

from tests.backends.fake_sonos_queue import FakeSonosQueue

CAPS = DLNACapabilities(supports_flac=True, max_sample_rate=48000, max_bit_depth=24)
CATALOG = {
    str(i): {
        "title": f"Title {i}",
        "artist": "A",
        "album": "B",
        "album_art_url": "",
        "duration_ms": 200_000,
        "max_sample_rate_hz": 44100,
        "max_bit_depth": 16,
    }
    for i in range(1, 80)
}


class FakeClient(SonosClient):
    """A real SonosClient whose queue and transport are a fake device."""

    def __init__(self) -> None:
        super().__init__("10.0.0.5")
        self.device = FakeSonosQueue()
        self.played_from: list[int] = []
        self.play_ok = True
        self.play_modes: list[str] = []

    async def clear_queue(self) -> bool:
        return await self.device.clear_queue()

    async def add_uris_to_queue(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return await self.device.add_uris_to_queue(*args, **kwargs)

    async def remove_queue_range(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return await self.device.remove_queue_range(*args, **kwargs)

    async def move_queue_range(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return await self.device.move_queue_range(*args, **kwargs)

    async def browse_queue(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return await self.device.browse_queue(*args, **kwargs)

    async def set_play_mode(self, mode: str) -> bool:
        self.play_modes.append(mode)
        return True

    async def play_from_queue(self, index: int = 0) -> bool:
        self.played_from.append(index)
        return self.play_ok


def keys_on(client: FakeClient) -> list[EntryKey]:
    return [parse_entry_url(u).key for u in client.device.urls]  # type: ignore[union-attr]


def K(*items: int) -> list[EntryKey]:
    return [EntryKey(str(i), i) for i in items]


async def make(
    items: list[int], session: str = "s", major: int = 5
) -> tuple[SonosQueueBackend, FakeClient, QobuzQueue]:
    queue = QobuzQueue()
    queue.set_session(session)
    await queue.set_version(QueueVersion(major, 1))
    await queue.apply_snapshot([(i, str(i)) for i in items])

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(
        return_value=ResolvedStream(
            url="https://cdn/x",
            blob="blob",
            format_id=QOBUZ_QUALITY_CD,
            sample_rate=44100,
            bit_depth=16,
            fetched_at=0.0,
        )
    )
    resolver.api_client = MagicMock()
    resolver.api_client.get_tracks_metadata = AsyncMock(
        side_effect=lambda ids: {i: CATALOG[i] for i in ids if i in CATALOG}
    )
    resolver.api_client.get_tracks_audio_info = AsyncMock(return_value={})

    backend = SonosQueueBackend("10.0.0.5")
    backend._client = FakeClient()
    backend._is_connected = True
    backend._capabilities = CAPS
    backend._proxy_server = backend.create_proxy_server(
        resolver, host="127.0.0.1", port=7120, queue=queue
    )
    return backend, backend._client, queue  # type: ignore[return-value]


def meta(item: int) -> BackendTrackMetadata:
    return BackendTrackMetadata(
        track_id=str(item), title=f"Title {item}", duration_ms=200_000, queue_item_id=item
    )


async def settled(backend: SonosQueueBackend) -> None:
    assert backend._runner is not None
    await backend._runner.wait_idle()


class TestPlay:
    async def test_the_first_play_publishes_the_whole_queue_and_starts_at_the_entry(self) -> None:
        backend, client, _ = await make(list(range(1, 41)))

        result = await backend.play(meta(20))
        await settled(backend)

        assert keys_on(client) == K(*range(1, 41))
        assert client.played_from[0] == 0  # started right after the first batch
        assert isinstance(result, PlayResult) and result.blob == "blob"

    async def test_playing_starts_before_the_whole_queue_is_published(self) -> None:
        backend, client, _ = await make(list(range(1, 41)))
        seen_at_start: list[int] = []
        original = client.play_from_queue

        async def spy(index: int = 0) -> bool:
            seen_at_start.append(len(client.device.entries))
            return await original(index)

        client.play_from_queue = spy  # type: ignore[method-assign]

        await backend.play(meta(20))
        await settled(backend)

        assert seen_at_start == [16]  # one batch out when playback began

    async def test_another_track_of_the_same_queue_is_just_a_jump(self) -> None:
        backend, client, _ = await make(list(range(1, 21)))
        await backend.play(meta(1))
        await settled(backend)
        client.device.calls.clear()

        await backend.play(meta(7))

        assert client.device.calls == []  # nothing republished
        assert client.played_from[-1] == 6  # position 7 (0-based 6)

    async def test_a_new_queue_generation_is_published_again(self) -> None:
        backend, client, queue = await make(list(range(1, 11)))
        await backend.play(meta(1))
        await settled(backend)
        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(i, str(i)) for i in range(30, 36)])

        await backend.play(meta(31))
        await settled(backend)

        assert keys_on(client) == K(*range(30, 36))
        # started on the first batch, where the entry to play comes first; the
        # entries before it in the queue were put in front afterwards
        assert client.played_from[-1] == 0

    async def test_an_entry_outside_the_queue_is_added_for_the_autoplay_case(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(1))
        await settled(backend)

        await backend.play(meta(50))  # not in the queue: an autoplay track
        await settled(backend)

        assert keys_on(client) == K(1, 2, 3, 50)
        assert client.played_from[-1] == 3

    async def test_a_queue_that_is_not_known_yet_publishes_just_the_entry(self) -> None:
        backend, client, queue = await make([1, 2, 3])
        await queue.add_queue_tracks([(9, "9")])  # a delta: the order is stale

        await backend.play(meta(2))
        await settled(backend)

        assert keys_on(client) == K(2)

    async def test_failing_to_start_is_reported_as_failure(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        client.play_ok = False

        assert not await backend._start_transport(
            "http://127.0.0.1:7120/audio/1?format=flac&i=1&s=x&v=5", "<d/>"
        )

    def test_the_device_is_left_playing_on_shutdown(self) -> None:
        assert SonosQueueBackend("10.0.0.5").keeps_playing_on_shutdown is True

    def test_gapless_arming_is_off(self) -> None:
        assert SonosQueueBackend("10.0.0.5").supports_gapless is False

    async def test_nothing_is_ever_armed_so_no_second_copy_lands_in_the_queue(self) -> None:
        backend, client, _ = await make(list(range(1, 11)))
        await backend.play(meta(1))
        await settled(backend)
        before = keys_on(client)

        assert await backend.set_next_track(meta(2), 2) is None

        assert keys_on(client) == before  # the entry already there is not duplicated


class TestQueueChanged:
    async def test_a_new_order_is_brought_to_the_device_around_the_playing_entry(self) -> None:
        backend, client, queue = await make(list(range(1, 11)))
        await backend.play(meta(4))
        await settled(backend)
        await queue.apply_snapshot(
            [(i, str(i)) for i in range(1, 11)],
            shuffle_mode=True,
            shuffled_indexes=[3, 9, 1, 7, 0, 5, 2, 8, 4, 6],
        )

        await backend.queue_changed()
        await settled(backend)

        assert [k.queue_item_id for k in keys_on(client)] == [4, 10, 2, 8, 1, 6, 3, 9, 5, 7]

    async def test_nothing_happens_before_anything_was_published(self) -> None:
        backend, client, _ = await make([1, 2, 3])

        await backend.queue_changed()

        assert client.device.calls == []

    async def test_a_stale_order_is_not_acted_on(self) -> None:
        backend, client, queue = await make([1, 2, 3])
        await backend.play(meta(1))
        await settled(backend)
        client.device.calls.clear()
        await queue.remove_queue_tracks([2])  # the snapshot that follows has not arrived

        await backend.queue_changed()

        assert client.device.calls == []

    async def test_a_snapshot_of_another_generation_is_ignored(self) -> None:
        backend, client, queue = await make([1, 2, 3])
        await backend.play(meta(1))
        await settled(backend)
        client.device.calls.clear()
        await queue.set_version(QueueVersion(9, 1))
        await queue.apply_snapshot([(7, "7")])

        await backend.queue_changed()

        assert client.device.calls == []  # the next play loads it


class TestFollowingTheDevice:
    async def _playing(self, items: list[int], item: int):  # type: ignore[no-untyped-def]
        backend, client, queue = await make(items)
        await backend.play(meta(item))
        await settled(backend)
        events: list[tuple[str, int, object]] = []
        backend.on_item_changed(lambda t, i, r: events.append((t, i, r)))
        return backend, events

    def url(self, backend: SonosQueueBackend, item: int) -> str:
        assert backend._proxy_server is not None
        return f"{backend._proxy_server.base_url}/audio/{item}?format=flac&depth=16&rate=44100&i={item}"

    async def test_the_device_moving_to_another_entry_is_reported(self) -> None:
        backend, events = await self._playing(list(range(1, 11)), 3)
        await backend._observe_polled_uri(self.url(backend, 3))  # confirms entry 3

        await backend._observe_polled_uri(self.url(backend, 4))

        assert len(events) == 1 and events[0][:2] == ("4", 4)
        assert isinstance(events[0][2], PlayResult)
        assert backend._current_key == EntryKey("4", 4)

    async def test_the_old_entry_still_reported_right_after_our_own_switch_is_ignored(self) -> None:
        """The device lags for a moment after a jump: not a transition."""
        backend, events = await self._playing(list(range(1, 11)), 3)

        await backend._observe_polled_uri(self.url(backend, 2))  # entry 3 not confirmed yet

        assert events == []

    async def test_the_entry_we_started_just_confirms(self) -> None:
        backend, events = await self._playing(list(range(1, 11)), 3)

        await backend._observe_polled_uri(self.url(backend, 3))

        assert events == [] and backend._current_track_confirmed

    async def test_a_foreign_url_is_not_ours_to_report(self) -> None:
        backend, events = await self._playing(list(range(1, 11)), 3)
        await backend._observe_polled_uri(self.url(backend, 3))

        await backend._observe_polled_uri("x-sonos-spotify:spotify%3atrack%3a1")
        await backend._observe_polled_uri(None)
        await backend._observe_polled_uri("")

        assert events == []

    async def test_the_same_report_twice_is_one_event(self) -> None:
        backend, events = await self._playing(list(range(1, 11)), 3)
        await backend._observe_polled_uri(self.url(backend, 3))

        await backend._observe_polled_uri(self.url(backend, 4))
        await backend._observe_polled_uri(self.url(backend, 4))

        assert len(events) == 1

    async def test_a_lookup_failure_still_reports_the_move(self) -> None:
        backend, events = await self._playing(list(range(1, 11)), 3)
        await backend._observe_polled_uri(self.url(backend, 3))
        backend._proxy_server.resolve_track = AsyncMock(side_effect=RuntimeError("no"))  # type: ignore[union-attr,method-assign]

        await backend._observe_polled_uri(self.url(backend, 5))

        assert len(events) == 1 and events[0][:2] == ("5", 5) and events[0][2] is None


class TestLifecycle:
    async def test_retargeting_forgets_what_the_old_device_held(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(1))
        await settled(backend)

        await backend._reset_queue_state()

        assert backend._published_generation is None and backend._runner is None
        await backend.play(meta(2))  # publishes again on the (new) device
        await settled(backend)
        assert keys_on(client) == K(1, 2, 3)

    async def test_a_failed_sync_makes_the_next_play_publish_afresh(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(1))
        await settled(backend)

        backend._on_sync_error(RuntimeError("device unreachable"))

        assert backend._published_generation is None

    async def test_an_unidentifiable_url_falls_back_to_the_single_entry_way(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        client.clear_queue = AsyncMock(return_value=True)  # type: ignore[method-assign]
        client.add_uri_to_queue = AsyncMock(return_value=1)  # type: ignore[method-assign]

        # a URL without a queue item id: the plain SonosBackend path
        ok = await backend._start_transport("http://127.0.0.1:7120/audio/1?format=flac", "<d/>")

        assert ok
        client.add_uri_to_queue.assert_awaited_once()
        assert client.device.entries == []  # nothing of the queue was published


class TestRepeatMode:
    async def test_the_qobuz_modes_become_the_devices_play_modes(self) -> None:
        backend, client, _ = await make([1, 2, 3])

        for qobuz, device in (("one", "REPEAT_ONE"), ("all", "REPEAT_ALL"), ("off", "NORMAL")):
            await backend.set_repeat_mode(qobuz)
            assert client.play_modes[-1] == device

    async def test_an_unknown_mode_means_normal(self) -> None:
        backend, client, _ = await make([1, 2, 3])

        await backend.set_repeat_mode("bogus")

        assert client.play_modes == ["NORMAL"]

    async def test_a_fresh_queue_is_started_in_the_current_repeat_mode(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.set_repeat_mode("one")
        client.play_modes.clear()

        await backend.play(meta(1))
        await settled(backend)

        assert client.play_modes == ["REPEAT_ONE"]

    async def test_the_normal_mode_is_not_re_sent_on_every_load(self) -> None:
        backend, client, _ = await make([1, 2, 3])

        await backend.play(meta(1))
        await settled(backend)

        assert client.play_modes == []


def pointer(item: int) -> dict:
    return {"trackId": str(item), "queueItemId": item, "contextUuid": None}


class TestAutoplayTail:
    """The server's next pointer leaving the queue (an autoplay track, O20)
    is published after the queue, so the device runs on into it by itself."""

    async def test_a_pointer_outside_the_queue_is_published_after_it(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(3))
        await settled(backend)

        await backend.next_pointer_changed(pointer(50))
        await settled(backend)

        assert keys_on(client) == K(1, 2, 3, 50)

    async def test_a_pointer_inside_the_queue_adds_nothing(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(1))
        await settled(backend)

        await backend.next_pointer_changed(pointer(2))
        await settled(backend)

        assert keys_on(client) == K(1, 2, 3)

    async def test_the_list_is_followed_one_entry_at_a_time(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(3))
        await settled(backend)
        await backend.next_pointer_changed(pointer(50))
        await settled(backend)

        backend._current_key = EntryKey("50", 50)  # the device ran on into it
        await backend.next_pointer_changed(pointer(51))
        await settled(backend)

        assert keys_on(client) == K(1, 2, 3, 50, 51)

    async def test_a_pointer_that_goes_away_takes_its_entry_with_it(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(3))
        await settled(backend)
        await backend.next_pointer_changed(pointer(50))
        await settled(backend)

        await backend.next_pointer_changed(None)  # autoplay switched off: the sentinel
        await settled(backend)

        assert keys_on(client) == K(1, 2, 3)

    async def test_the_entry_playing_stays_when_the_pointer_goes_away(self) -> None:
        backend, client, _ = await make([1, 2, 3])
        await backend.play(meta(3))
        await settled(backend)
        await backend.next_pointer_changed(pointer(50))
        await settled(backend)
        backend._current_key = EntryKey("50", 50)
        await backend.next_pointer_changed(pointer(51))
        await settled(backend)

        await backend.next_pointer_changed(None)
        await settled(backend)

        assert keys_on(client) == K(1, 2, 3, 50)

    async def test_an_edit_of_the_queue_keeps_the_tail(self) -> None:
        backend, client, queue = await make([1, 2, 3])
        await backend.play(meta(3))
        await settled(backend)
        await backend.next_pointer_changed(pointer(50))
        await settled(backend)

        await queue.apply_snapshot([(1, "1"), (3, "3"), (2, "2")])
        await backend.queue_changed()
        await settled(backend)

        assert keys_on(client) == K(1, 3, 2, 50)

    async def test_another_queue_starts_without_the_old_tail(self) -> None:
        backend, client, queue = await make([1, 2, 3])
        await backend.play(meta(3))
        await settled(backend)
        await backend.next_pointer_changed(pointer(50))
        await settled(backend)

        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(10, "10"), (11, "11")])
        backend._pointer = None  # the new load's own next pointer: inside it
        await backend.play(meta(10))
        await settled(backend)

        assert keys_on(client) == K(10, 11)


class TestOwnUrls:
    async def test_an_entry_naming_another_rooms_proxy_port_is_still_ours(self) -> None:
        backend, client, _ = await make([1, 2])
        await backend.play(meta(1))
        await settled(backend)
        url = client.device.urls[0]
        own_port = urlsplit(backend._proxy_server.base_url).port
        other = url.replace(f":{own_port}/", f":{(own_port or 0) + 3}/")
        assert other != url

        assert backend._is_own_proxy_url(url)
        assert backend._is_own_proxy_url(other)

    async def test_foreign_urls_are_not(self) -> None:
        backend, _, _ = await make([1])
        host = urlsplit(backend._proxy_server.base_url).hostname

        assert not backend._is_own_proxy_url("http://elsewhere:7120/audio/1?x=1")
        assert not backend._is_own_proxy_url(f"http://{host}:7120/other/1")
        assert not backend._is_own_proxy_url("x-rincon-queue:RINCON_1#0")


class TestCarryingOn:
    """A proxy that restarted under a device still playing its entries."""

    async def _playing_on_device(self):  # type: ignore[no-untyped-def]
        backend, client, queue = await make([1, 2, 3])
        await backend.play(meta(2))
        await settled(backend)
        url = client.device.urls[1]  # the entry the device is on
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)  # type: ignore[method-assign]
        backend._get_current_transport_uri = AsyncMock(return_value=url)  # type: ignore[method-assign]
        backend.get_position = AsyncMock(return_value=61_000)  # type: ignore[method-assign]
        return backend, client, queue

    async def test_the_entry_playing_is_found(self) -> None:
        backend, _, queue = await self._playing_on_device()

        found = await backend.playing_entry()

        assert found is not None
        assert (found.track_id, found.queue_item_id) == ("2", 2)
        assert (found.session, found.major) == queue.generation
        assert found.position_ms == 61_000 and found.paused is False

    async def test_nothing_is_found_while_the_device_is_stopped(self) -> None:
        backend, _, _ = await self._playing_on_device()
        backend.get_state = AsyncMock(return_value=PlaybackState.STOPPED)  # type: ignore[method-assign]

        assert await backend.playing_entry() is None

    async def test_something_foreign_is_not_found(self) -> None:
        backend, _, _ = await self._playing_on_device()
        backend._get_current_transport_uri = AsyncMock(return_value="http://elsewhere/x.mp3")  # type: ignore[method-assign]

        assert await backend.playing_entry() is None

    async def test_adopting_learns_the_device_queue_so_edits_can_follow(self) -> None:
        backend, client, queue = await self._playing_on_device()
        found = await backend.playing_entry()
        await backend._reset_queue_state()  # the restart: nothing known any more
        assert found is not None

        await backend.adopt(found, 200_000)
        await queue.apply_snapshot([(1, "1"), (3, "3"), (2, "2")])
        await backend.queue_changed()
        await settled(backend)

        assert backend._current_key == EntryKey("2", 2)
        assert keys_on(client) == K(1, 3, 2)


class TestDeviceEvents:
    """An AVTransport event wakes the poll loop; the subscription follows the
    device the backend drives."""

    def _source(self):  # type: ignore[no-untyped-def]
        from qobuz_proxy.backends.dlna.sonos.events import EventSource, SonosEventSubscriber

        subscriber = SonosEventSubscriber()
        subscriber.subscribe_service = AsyncMock(return_value=None)  # type: ignore[method-assign]
        subscriber.unsubscribe_service = AsyncMock()  # type: ignore[method-assign]
        return EventSource(subscriber, "http://cb/sonos-events"), subscriber

    async def test_attaching_subscribes_to_the_device_we_drive(self) -> None:
        backend, _, _ = await make([1])
        source, subscriber = self._source()

        await backend.attach_events(source)

        args = subscriber.subscribe_service.await_args.args  # type: ignore[attr-defined]
        assert args[0] == backend._ip and args[1] == backend._port
        assert args[2].endswith("/AVTransport/Event") and args[3] == "http://cb/sonos-events"
        await backend._stop_events()

    async def test_an_event_wakes_the_poll_loop(self) -> None:
        from qobuz_proxy.backends.dlna.sonos.transport_events import TransportEvent

        backend, _, _ = await make([1])
        backend.wake_poll = MagicMock()  # type: ignore[method-assign]

        await backend._on_transport_event(TransportEvent(transport_state="PLAYING"))

        backend.wake_poll.assert_called_once()

    async def test_something_that_is_not_a_source_is_ignored(self) -> None:
        backend, _, _ = await make([1])

        await backend.attach_events(object())

        assert backend._events is None

    async def test_disconnecting_stops_the_subscription(self) -> None:
        backend, _, _ = await make([1])
        source, _ = self._source()
        await backend.attach_events(source)
        stream = backend._events
        assert stream is not None
        stream.stop = AsyncMock()  # type: ignore[method-assign]

        await backend.disconnect(send_device_stop=False)

        stream.stop.assert_awaited()

    async def test_a_retarget_moves_the_subscription(self) -> None:
        backend, _, _ = await make([1])
        source, subscriber = self._source()
        await backend.attach_events(source)
        backend._events.start = AsyncMock()  # type: ignore[union-attr, method-assign]
        backend._client = backend._client or MagicMock()

        from unittest.mock import patch

        with patch.object(
            type(backend).__mro__[2], "retarget", AsyncMock(return_value=True)
        ):  # the plain retarget: only what follows it is under test
            ok = await backend.retarget("10.0.0.9", 1400)

        assert ok
        backend._events.start.assert_awaited_once_with(backend._ip, backend._port)  # type: ignore[union-attr]

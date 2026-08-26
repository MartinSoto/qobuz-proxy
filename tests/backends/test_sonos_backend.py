"""Tests for SonosBackend — queue-based playback/gapless and the
Sonos-specific "what's currently playing" URI lookup."""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends.types import BackendTrackMetadata
from qobuz_proxy.backends.dlna.sonos.backend import SonosBackend
from qobuz_proxy.backends.dlna.sonos.client import SonosClient


def _make_metadata(**kwargs) -> BackendTrackMetadata:  # type: ignore[no-untyped-def]
    defaults = {
        "track_id": "1",
        "title": "Test Track",
        "artist": "Test Artist",
        "album": "Test Album",
        "duration_ms": 180000,
        "artwork_url": "",
    }
    defaults.update(kwargs)
    return BackendTrackMetadata(**defaults)


def _make_backend():  # type: ignore[no-untyped-def]
    backend = SonosBackend("10.0.0.5")
    backend._gapless_supported = True
    client = MagicMock(spec=SonosClient)
    client.add_uri_to_queue = AsyncMock(return_value=7)
    client.remove_track_from_queue = AsyncMock(return_value=True)
    backend._client = client
    return backend, client


class TestSonosGaplessQueue:
    """Sonos gapless arming appends to the device queue — duplicates replay the song."""

    async def test_set_next_track_stores_queue_position(self):
        backend, client = _make_backend()
        meta = _make_metadata(track_id="222")

        assert await backend.set_next_track("http://proxy/audio/222_9.flac", meta, 9)

        client.add_uri_to_queue.assert_awaited_once()
        assert backend._next_track_queue_nr == 7

    async def test_set_next_track_skips_duplicate_url(self):
        backend, client = _make_backend()
        meta = _make_metadata(track_id="222")

        assert await backend.set_next_track("http://proxy/audio/222_9.flac", meta, 9)
        assert await backend.set_next_track("http://proxy/audio/222_9.flac", meta, 9)

        client.add_uri_to_queue.assert_awaited_once()

    async def test_clear_next_track_removes_queued_entry(self):
        backend, client = _make_backend()
        meta = _make_metadata(track_id="222")
        await backend.set_next_track("http://proxy/audio/222_9.flac", meta, 9)

        await backend.clear_next_track()

        client.remove_track_from_queue.assert_awaited_once_with(7)
        assert backend._next_track_queue_nr is None
        assert backend._next_track_proxy_url is None

    async def test_clear_next_track_without_armed_entry_is_noop(self):
        backend, client = _make_backend()

        await backend.clear_next_track()

        client.remove_track_from_queue.assert_not_called()


class TestIsPlayingOurContent:
    """Sonos queue playback's GetMediaInfo.CurrentURI is the *queue* URI,
    not the track URL — SonosBackend uses GetPositionInfo.TrackURI
    (get_track_uri) instead. See test_dlna_backend.py's own
    TestIsPlayingOurContent for the generic (non-Sonos) path."""

    def _make_takeover_backend(self, proxy_url: str = "http://proxy/track.flac"):  # type: ignore[no-untyped-def]
        backend, client = _make_backend()
        backend._current_proxy_url = proxy_url
        backend._next_track_proxy_url = None
        client.get_track_uri = AsyncMock()
        return backend, client

    async def test_true_when_uri_matches(self):
        backend, client = self._make_takeover_backend()
        client.get_track_uri.return_value = "http://proxy/track.flac"

        assert await backend.is_playing_our_content() is True

    async def test_false_when_uri_does_not_match(self):
        backend, client = self._make_takeover_backend()
        client.get_track_uri.return_value = "http://someone-else/spotify-stream"

        assert await backend.is_playing_our_content() is False

    async def test_true_when_uri_matches_the_armed_next_track(self):
        # A gapless transition already in flight is a legitimate URI
        # change, not a takeover.
        backend, client = self._make_takeover_backend()
        backend._next_track_proxy_url = "http://proxy/next.flac"
        client.get_track_uri.return_value = "http://proxy/next.flac"

        assert await backend.is_playing_our_content() is True

    async def test_true_when_nothing_of_ours_playing_yet(self):
        backend, client = self._make_takeover_backend(proxy_url="")
        backend._current_proxy_url = None

        assert await backend.is_playing_our_content() is True
        client.get_track_uri.assert_not_called()

    async def test_true_on_transient_read_failure(self):
        backend, client = self._make_takeover_backend()
        client.get_track_uri.return_value = None

        assert await backend.is_playing_our_content() is True

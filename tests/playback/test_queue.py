"""Tests for queue management."""

from unittest.mock import AsyncMock

import pytest

from qobuz_proxy.playback.queue import (
    QobuzQueue,
    QueueTrack,
    QueueVersion,
    RepeatMode,
)


class TestQueueVersion:
    """Tests for QueueVersion class."""

    def test_str(self) -> None:
        """Test string representation."""
        version = QueueVersion(major=5, minor=3)
        assert str(version) == "5.3"

    def test_is_newer_than_major(self) -> None:
        """Test version comparison by major."""
        v1 = QueueVersion(major=2, minor=0)
        v2 = QueueVersion(major=1, minor=5)
        assert v1.is_newer_than(v2) is True
        assert v2.is_newer_than(v1) is False

    def test_is_newer_than_minor(self) -> None:
        """Test version comparison by minor."""
        v1 = QueueVersion(major=1, minor=5)
        v2 = QueueVersion(major=1, minor=3)
        assert v1.is_newer_than(v2) is True
        assert v2.is_newer_than(v1) is False

    def test_is_newer_than_equal(self) -> None:
        """Test version comparison when equal."""
        v1 = QueueVersion(major=1, minor=5)
        v2 = QueueVersion(major=1, minor=5)
        assert v1.is_newer_than(v2) is False
        assert v2.is_newer_than(v1) is False


class TestQueueTrack:
    """Tests for QueueTrack class."""

    def test_defaults(self) -> None:
        """Test default values."""
        track = QueueTrack(queue_item_id=1, track_id="12345")
        assert track.queue_item_id == 1
        assert track.track_id == "12345"
        assert track.context_uuid is None
        assert track.streaming_url is None
        assert track.metadata == {}
        assert track.start_ms == 0
        assert track.duration_ms == 0

    def test_url_staleness(self) -> None:
        """A cached URL is only trusted within its TTL."""
        track = QueueTrack(queue_item_id=1, track_id="12345")
        assert track.url_is_stale()  # no URL at all

        track.set_streaming_url("https://example.com/track.flac")
        assert not track.url_is_stale()

        track.url_fetched_at -= 241  # age the URL past the 240s TTL
        assert track.url_is_stale()

        track.set_streaming_url(None)
        assert track.url_is_stale()
        assert track.url_fetched_at == 0.0


class TestTrackCaching:
    """QobuzQueue.get_track_url/get_track_metadata — the single place
    "is this cached, if not fetch and cache it" is implemented, shared by
    _preload_upcoming and QobuzPlayer (which used to each have their own
    copy of this logic)."""

    @pytest.fixture
    def queue(self) -> QobuzQueue:
        return QobuzQueue()

    @pytest.mark.asyncio
    async def test_get_track_url_fetches_and_caches_on_miss(self, queue: QobuzQueue) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")
        url_callback = AsyncMock(return_value="https://example.com/a.flac")
        queue.set_url_callback(url_callback)

        url = await queue.get_track_url(track)

        assert url == "https://example.com/a.flac"
        assert track.streaming_url == "https://example.com/a.flac"
        url_callback.assert_awaited_once_with("A")

    @pytest.mark.asyncio
    async def test_get_track_url_returns_cached_value_without_fetching(
        self, queue: QobuzQueue
    ) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")
        track.set_streaming_url("https://cached.example.com/a.flac")
        url_callback = AsyncMock(return_value="https://fresh.example.com/a.flac")
        queue.set_url_callback(url_callback)

        url = await queue.get_track_url(track)

        assert url == "https://cached.example.com/a.flac"
        url_callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_track_url_refetches_a_stale_cached_value(self, queue: QobuzQueue) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")
        track.set_streaming_url("https://cached.example.com/a.flac")
        track.url_fetched_at -= 300  # past the TTL
        url_callback = AsyncMock(return_value="https://fresh.example.com/a.flac")
        queue.set_url_callback(url_callback)

        url = await queue.get_track_url(track)

        assert url == "https://fresh.example.com/a.flac"
        assert track.streaming_url == "https://fresh.example.com/a.flac"

    @pytest.mark.asyncio
    async def test_get_track_url_returns_none_without_a_callback(self, queue: QobuzQueue) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")

        assert await queue.get_track_url(track) is None

    @pytest.mark.asyncio
    async def test_get_track_metadata_fetches_and_caches_on_miss(self, queue: QobuzQueue) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")
        metadata_callback = AsyncMock(return_value={"title": "Song", "duration_ms": 123})
        queue.set_metadata_callback(metadata_callback)

        meta = await queue.get_track_metadata(track)

        assert meta == {"title": "Song", "duration_ms": 123}
        assert track.metadata == {"title": "Song", "duration_ms": 123}
        assert track.duration_ms == 123
        metadata_callback.assert_awaited_once_with("A")

    @pytest.mark.asyncio
    async def test_get_track_metadata_returns_cached_value_without_fetching(
        self, queue: QobuzQueue
    ) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A", metadata={"title": "Cached"})
        metadata_callback = AsyncMock(return_value={"title": "Fresh"})
        queue.set_metadata_callback(metadata_callback)

        meta = await queue.get_track_metadata(track)

        assert meta == {"title": "Cached"}
        metadata_callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_track_metadata_returns_none_without_a_callback(
        self, queue: QobuzQueue
    ) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")

        assert await queue.get_track_metadata(track) is None


class TestQobuzQueue:
    """Tests for QobuzQueue class.

    A renderer session never has an ordered track list to load/navigate/
    shuffle — see queue.py's module docstring. What's left is exactly what
    SET_STATE feeds a renderer directly: repeat mode and the queue version
    stamp, both covered below.
    """

    @pytest.fixture
    def queue(self) -> QobuzQueue:
        """Create a fresh queue."""
        return QobuzQueue()

    @pytest.mark.asyncio
    async def test_repeat_mode_state_reported(self, queue: QobuzQueue) -> None:
        """Test repeat mode is reported in state."""
        state = await queue.get_state()
        assert state.repeat_mode == RepeatMode.OFF

        await queue.set_repeat_mode(RepeatMode.ALL)

        state = await queue.get_state()
        assert state.repeat_mode == RepeatMode.ALL

    @pytest.mark.asyncio
    async def test_version_tracking(self, queue: QobuzQueue) -> None:
        """Test queue version is stored and retrieved."""
        await queue.set_version(QueueVersion(major=5, minor=3))

        stored_version = await queue.get_version()
        assert stored_version.major == 5
        assert stored_version.minor == 3

    @pytest.mark.asyncio
    async def test_get_state(self, queue: QobuzQueue) -> None:
        """Test getting queue state snapshot."""
        await queue.set_version(QueueVersion(major=3, minor=2))
        await queue.set_repeat_mode(RepeatMode.ALL)

        state = await queue.get_state()

        assert state.version.major == 3
        assert state.version.minor == 2
        assert state.repeat_mode == RepeatMode.ALL

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
        assert track.metadata == {}
        assert track.start_ms == 0
        assert track.duration_ms == 0
        assert track.blob == ""
        assert track.actual_quality == 0


class TestTrackCaching:
    """QobuzQueue.get_track_metadata (fetch-and-cache display metadata) and
    set_track_stream_info (record what the backend resolved/served — see
    AudioBackend.play's PlayResult) — the streaming URL itself is no
    longer cached here, see queue.py's module docstring."""

    @pytest.fixture
    def queue(self) -> QobuzQueue:
        return QobuzQueue()

    def test_set_track_stream_info_records_blob_and_quality(self, queue: QobuzQueue) -> None:
        track = QueueTrack(queue_item_id=1, track_id="A")

        queue.set_track_stream_info(track, blob="the-blob", actual_quality=27)

        assert track.blob == "the-blob"
        assert track.actual_quality == 27

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

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


class TestQueueMembership:
    """QobuzQueue's tracking of which tracks are in the current manual
    queue — fed by full snapshots (SrvrCtrlQueueTracksLoaded/QueueState)
    and incremental deltas (Added/Inserted/Removed/Cleared) on top of one;
    see command_handler.py's handlers. The one consumer today is Sonos's
    approximated-album-gain grouping (backends/dlna/sonos/gain.py)."""

    @pytest.fixture
    def queue(self) -> QobuzQueue:
        return QobuzQueue()

    @pytest.mark.asyncio
    async def test_untrustworthy_before_any_full_snapshot(self, queue: QobuzQueue) -> None:
        """A fresh queue has never seen a full snapshot — current_track_ids
        must say "unknown", not "empty", so a caller doesn't mistake never
        having heard from the queue for it genuinely being empty."""
        assert await queue.current_track_ids() is None

    @pytest.mark.asyncio
    async def test_replace_queue_tracks_becomes_trustworthy(self, queue: QobuzQueue) -> None:
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])

        assert await queue.current_track_ids() == frozenset({"A", "B"})

    @pytest.mark.asyncio
    async def test_add_queue_tracks_is_a_delta_on_top_of_a_snapshot(
        self, queue: QobuzQueue
    ) -> None:
        """Confirmed live (2026-09-15): Added/Inserted carry only the new
        track(s), not the whole queue — add_queue_tracks must not replace
        what's already known."""
        await queue.replace_queue_tracks([(1, "A")])

        await queue.add_queue_tracks([(2, "B")])

        assert await queue.current_track_ids() == frozenset({"A", "B"})

    @pytest.mark.asyncio
    async def test_remove_queue_tracks_removes_by_queue_item_id(self, queue: QobuzQueue) -> None:
        await queue.replace_queue_tracks([(1, "A"), (2, "B"), (3, "C")])

        await queue.remove_queue_tracks([2])

        assert await queue.current_track_ids() == frozenset({"A", "C"})

    @pytest.mark.asyncio
    async def test_remove_unknown_queue_item_id_is_a_no_op(self, queue: QobuzQueue) -> None:
        await queue.replace_queue_tracks([(1, "A")])

        await queue.remove_queue_tracks([999])

        assert await queue.current_track_ids() == frozenset({"A"})

    @pytest.mark.asyncio
    async def test_clear_queue_tracks_is_a_known_empty_state(self, queue: QobuzQueue) -> None:
        await queue.replace_queue_tracks([(1, "A")])

        await queue.clear_queue_tracks()

        assert await queue.current_track_ids() == frozenset()

    @pytest.mark.asyncio
    async def test_reordering_two_tracks_at_the_same_ids_keeps_membership(
        self, queue: QobuzQueue
    ) -> None:
        """Reordered carries no track data at all (see QueueMembership) —
        membership must be unaffected by anything other than the four
        methods that actually change it."""
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])

        assert await queue.current_track_ids() == frozenset({"A", "B"})

    @pytest.mark.asyncio
    async def test_version_going_backward_discards_membership(self, queue: QobuzQueue) -> None:
        """Queue-generation counters only restart from 1 when a brand new
        session starts (confirmed live) — membership reconstructed under
        the old generation must not silently persist into the new one."""
        await queue.set_version(QueueVersion(major=8, minor=1))
        await queue.replace_queue_tracks([(1, "A")])

        await queue.set_version(QueueVersion(major=1, minor=1))

        assert await queue.current_track_ids() is None

    @pytest.mark.asyncio
    async def test_version_advancing_within_a_session_keeps_membership(
        self, queue: QobuzQueue
    ) -> None:
        await queue.set_version(QueueVersion(major=1, minor=1))
        await queue.replace_queue_tracks([(1, "A")])

        await queue.set_version(QueueVersion(major=1, minor=2))

        assert await queue.current_track_ids() == frozenset({"A"})

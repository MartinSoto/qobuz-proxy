"""Tests for SrvrCtrlQueueLoadTracks (type 91) handling via
PlaybackCommandHandler.

Confirmed live (2026-09-13, docs/connect-activation-model-hypotheses.md):
tapping a track in an album/playlist view never produces a
SrvrRndrSetState with a currentQueueItem — only this message, with
queuePosition/queueVersion advancing. This is the real "play this track"
signal, not SET_STATE.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.command_handler import (
    MSG_TYPE_QUEUE_TRACKS_LOADED,
    PlaybackCommandHandler,
)
from qobuz_proxy.proto import qconnect_payload_pb2 as pb

from tests.playback.test_player_serialization import _make_player


def _queue_tracks_loaded_msg(
    *,
    tracks: list[tuple[int, int]],  # (queueItemId, trackId) pairs, in order
    queue_position: int | None = None,
    context_uuid: bytes | None = None,
) -> pb.QConnectMessage:
    """Build a server->controller SrvrCtrlQueueLoadTracks (type 91) message."""
    msg = pb.QConnectMessage()
    msg.messageType = MSG_TYPE_QUEUE_TRACKS_LOADED
    loaded = msg.srvrCtrlQueueTracksLoaded
    for queue_item_id, track_id in tracks:
        ref = loaded.tracks.add()
        if queue_item_id:
            ref.queueItemId = queue_item_id
        ref.trackId = track_id
    if queue_position is not None:
        loaded.queuePosition = queue_position
    if context_uuid is not None:
        loaded.contextUuid = context_uuid
    return msg


class TestQueueTracksLoadedPlaysSelectedTrack:
    async def test_plays_the_track_at_queue_position(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001), (1, 3002), (2, 3003)], queue_position=1)
        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is not None
        assert player.current_track.track_id == "3002"
        assert backend.played == ["3002"]
        assert player.state == PlaybackState.PLAYING

    async def test_loading_the_track_already_playing_republishes_it(self) -> None:
        """A load starts a new queue generation; the device must get the
        current track again (new stream URL), not just a seek back to 0 —
        its old URL belongs to the previous generation."""
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)
        tracks = [(0, 3001), (1, 3002)]

        await handler._handle_queue_tracks_loaded(_queue_tracks_loaded_msg(tracks=tracks))
        await handler._handle_queue_tracks_loaded(_queue_tracks_loaded_msg(tracks=tracks))

        assert backend.played == ["3001", "3001"]

    async def test_absent_queue_position_defaults_to_the_first_track(self) -> None:
        """queuePosition absent from the message reads as 0 — matches the
        server's own convention (confirmed live: the first
        QUEUE_TRACKS_LOADED of a fresh "play from the start" never carries
        the field at all)."""
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001), (1, 3002)])
        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is not None
        assert player.current_track.track_id == "3001"

    async def test_starts_at_position_zero_even_if_something_stale_was_paused(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001)], queue_position=0)
        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_position_ms == 0

    async def test_propagates_context_uuid_from_the_track(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        ctx = bytes(range(16))
        msg = pb.QConnectMessage()
        msg.messageType = MSG_TYPE_QUEUE_TRACKS_LOADED
        loaded = msg.srvrCtrlQueueTracksLoaded
        ref = loaded.tracks.add()
        ref.queueItemId = 1
        ref.trackId = 3001
        ref.contextUuid = ctx
        loaded.queuePosition = 0

        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is not None
        assert player.current_track.context_uuid == ctx

    async def test_falls_back_to_message_level_context_uuid(self) -> None:
        """A track without its own contextUuid still gets the message-level
        one, mirroring currentQueueItem.contextUuid's own fallback."""
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        ctx = bytes(range(16))
        msg = _queue_tracks_loaded_msg(tracks=[(1, 3001)], queue_position=0, context_uuid=ctx)

        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is not None
        assert player.current_track.context_uuid == ctx


class TestQueueTracksLoadedIgnoredWhenNotActive:
    async def test_does_not_touch_playback_while_not_the_active_renderer(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = False
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001)], queue_position=0)
        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is None
        assert backend.played == []

    async def test_still_stores_queue_version_while_not_active(self) -> None:
        """So a report sent once we *do* become active reflects reality —
        see docs/connect-activation-model-hypotheses.md's "queueVersion
        never echoed" finding."""
        from qobuz_proxy.playback.queue import QobuzQueue, QueueVersion

        player, backend = _make_player()
        player._is_active_renderer = False
        queue = QobuzQueue()
        handler = PlaybackCommandHandler(player, queue=queue)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001)], queue_position=0)
        msg.srvrCtrlQueueTracksLoaded.queueVersion.major = 17
        msg.srvrCtrlQueueTracksLoaded.queueVersion.minor = 1

        await handler._handle_queue_tracks_loaded(msg)

        assert await queue.get_version() == QueueVersion(major=17, minor=1)


class TestQueueTracksLoadedOutOfRangePosition:
    async def test_out_of_range_position_does_not_crash_or_touch_playback(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001)], queue_position=5)
        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is None
        assert backend.played == []

    async def test_missing_field_does_not_crash(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = pb.QConnectMessage()
        msg.messageType = MSG_TYPE_QUEUE_TRACKS_LOADED

        await handler._handle_queue_tracks_loaded(msg)

        assert player.current_track is None


class TestQueueTracksLoadedGaplessNextTrack:
    async def test_populates_next_track_info_for_gapless(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001), (1, 3002), (2, 3003)], queue_position=0)
        await handler._handle_queue_tracks_loaded(msg)

        next_info = handler.get_next_track_info()
        assert next_info is not None
        assert next_info["trackId"] == "3002"

    async def test_last_track_clears_next_track_info(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)
        on_changed = AsyncMock()
        handler.set_on_next_track_changed(on_changed)

        # First arm a next track...
        first = _queue_tracks_loaded_msg(tracks=[(0, 3001), (1, 3002)], queue_position=0)
        await handler._handle_queue_tracks_loaded(first)
        assert handler.get_next_track_info() is not None
        on_changed.reset_mock()

        # ...then select the last track — nothing left to gapless-arm.
        second = _queue_tracks_loaded_msg(tracks=[(0, 3001), (1, 3002)], queue_position=1)
        await handler._handle_queue_tracks_loaded(second)

        assert handler.get_next_track_info() is None
        on_changed.assert_awaited_once()

    async def test_single_track_queue_leaves_no_next_track(self) -> None:
        player, backend = _make_player()
        player._is_active_renderer = True
        handler = PlaybackCommandHandler(player)

        msg = _queue_tracks_loaded_msg(tracks=[(0, 3001)], queue_position=0)
        await handler._handle_queue_tracks_loaded(msg)

        assert handler.get_next_track_info() is None

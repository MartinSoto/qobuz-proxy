"""Tests for QobuzPlayer gapless re-arming."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.player import QobuzPlayer


def _make_player(next_track_info=None):
    """Build a player with mocked queue/metadata/backend."""
    queue = MagicMock()
    metadata = MagicMock()
    metadata.get_streaming_url = AsyncMock(return_value="http://proxy:7120/audio/222_9.flac")
    meta_obj = MagicMock()
    meta_obj.to_dict.return_value = {
        "title": "Track",
        "artist": "Artist",
        "album": "Album",
        "duration_ms": 1000,
        "artwork_url": "",
    }
    metadata.get_metadata = AsyncMock(return_value=meta_obj)
    metadata.get_track_format.return_value = (6, 44100, 16)

    backend = MagicMock()
    backend.supports_gapless = True
    backend.clear_next_track = AsyncMock()
    backend.set_next_track = AsyncMock(return_value=True)

    player = QobuzPlayer(queue=queue, metadata_service=metadata, backend=backend)
    player.set_next_track_callbacks(
        get_callback=lambda: next_track_info,
        clear_callback=lambda: None,
    )
    return player, backend


class TestOnNextTrackInfoChanged:
    """Queue edits mid-track (e.g. 'play next' in the app) must re-arm gapless."""

    async def test_rearms_with_new_next_track_while_playing(self):
        new_next = {"trackId": "222", "queueItemId": 9}
        player, backend = _make_player(next_track_info=new_next)
        player._state = PlaybackState.PLAYING
        # A stale track armed before the queue edit
        player._gapless_armed = True
        player._pending_next_track = {"trackId": "111", "queueItemId": 8}

        await player.on_next_track_info_changed()

        backend.clear_next_track.assert_awaited_once()
        backend.set_next_track.assert_awaited_once()
        assert player._pending_next_track["trackId"] == "222"
        assert player._gapless_armed is True

    async def test_clears_stale_arming_when_not_playing(self):
        player, backend = _make_player(next_track_info=None)
        player._state = PlaybackState.PAUSED
        player._gapless_armed = True
        player._pending_next_track = {"trackId": "111", "queueItemId": 8}

        await player.on_next_track_info_changed()

        backend.clear_next_track.assert_awaited_once()
        backend.set_next_track.assert_not_called()
        assert player._gapless_armed is False
        assert player._pending_next_track is None

    async def test_noop_when_armed_next_track_unchanged(self):
        """Redundant change events for the already-armed track must not re-arm.

        Re-arming appends a duplicate entry to the Sonos queue, which makes
        the song play twice.
        """
        same_next = {"trackId": "222", "queueItemId": 9}
        player, backend = _make_player(next_track_info=same_next)
        player._state = PlaybackState.PLAYING
        player._gapless_armed = True
        player._pending_next_track = {"trackId": "222", "queueItemId": 9}

        await player.on_next_track_info_changed()

        backend.clear_next_track.assert_not_called()
        backend.set_next_track.assert_not_called()
        assert player._gapless_armed is True


class TestPrepareNextTrackConcurrency:
    """Arming must be serialized — overlapping calls double-queue the next track."""

    async def test_concurrent_prepare_arms_backend_once(self):
        next_info = {"trackId": "222", "queueItemId": 9}
        player, backend = _make_player(next_track_info=next_info)
        player._state = PlaybackState.PLAYING

        async def slow_arm(*args, **kwargs):
            await asyncio.sleep(0.05)
            return True

        backend.set_next_track = AsyncMock(side_effect=slow_arm)

        await asyncio.gather(
            player._prepare_next_track_for_gapless(),
            player._prepare_next_track_for_gapless(),
        )

        backend.set_next_track.assert_awaited_once()
        assert player._gapless_armed is True

    async def test_stale_arm_undone_when_state_cleared_mid_arm(self):
        """A skip/stop while an arm is in flight must discard the stale arm."""
        next_info = {"trackId": "222", "queueItemId": 9}
        player, backend = _make_player(next_track_info=next_info)
        player._state = PlaybackState.PLAYING

        async def slow_arm(*args, **kwargs):
            await asyncio.sleep(0.05)
            return True

        backend.set_next_track = AsyncMock(side_effect=slow_arm)

        task = asyncio.create_task(player._prepare_next_track_for_gapless())
        await asyncio.sleep(0.01)  # let the arm reach the backend call
        player._clear_gapless_state()
        await task

        assert player._gapless_armed is False
        assert player._pending_next_track is None
        backend.clear_next_track.assert_awaited()

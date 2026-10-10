"""The control channel dropping and rejoining under live playback: the join
tells the truth, and the server's replay of what it remembers is not obeyed.
"""

import time
from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.queue import QueueEntry, QueueTrack

from tests.playback.test_player import _make_player


def _playing_player():  # type: ignore[no-untyped-def]
    player, backend = _make_player()
    backend.seek = AsyncMock()
    backend.stop = AsyncMock()
    backend.pause = AsyncMock(return_value=True)
    player._current_track = QueueTrack(queue_item_id=9, track_id="222")
    player._state = PlaybackState.PLAYING
    player._backend_engaged = True
    player._current_duration_ms = 200_000
    player._position_value_ms = 61_000
    player._position_timestamp_ms = int(time.time() * 1000)
    player.queue.play_order = AsyncMock(
        return_value=[QueueEntry(queue_item_id=8, track_id="111"), QueueEntry(9, "222")]
    )
    return player, backend


class TestJoinSnapshot:
    async def test_describes_the_playing_track(self) -> None:
        player, _ = _playing_player()

        snapshot = await player.join_snapshot()

        assert snapshot is not None
        assert snapshot.playing_state == 2
        assert 61_000 <= snapshot.position_ms < 62_000
        assert snapshot.duration_ms == 200_000
        assert snapshot.queue_index == 1

    async def test_a_paused_track_is_reported_paused(self) -> None:
        player, _ = _playing_player()
        player._state = PlaybackState.PAUSED

        snapshot = await player.join_snapshot()

        assert snapshot is not None and snapshot.playing_state == 3

    async def test_nothing_playing_means_no_snapshot(self) -> None:
        player, _ = _playing_player()
        player._state = PlaybackState.STOPPED

        assert await player.join_snapshot() is None

    async def test_nothing_loaded_means_no_snapshot(self) -> None:
        player, _ = _make_player()

        assert await player.join_snapshot() is None


class TestRestoreAfterRejoin:
    async def test_a_replayed_position_does_not_seek_a_playing_track(self) -> None:
        player, backend = _playing_player()
        player._last_activated_at = time.monotonic()

        await player.apply_remote_state(
            track_id="222", queue_item_id=9, position_ms=32_000, playing_state=2
        )

        backend.seek.assert_not_called()
        assert player._state == PlaybackState.PLAYING

    async def test_a_replayed_stop_does_not_stop_a_playing_track(self) -> None:
        player, backend = _playing_player()
        player._last_activated_at = time.monotonic()

        await player.apply_remote_state(
            track_id="222", queue_item_id=9, position_ms=0, playing_state=1
        )

        backend.stop.assert_not_called()
        assert player._state == PlaybackState.PLAYING

    async def test_a_scrub_long_after_rejoining_is_obeyed(self) -> None:
        player, backend = _playing_player()
        player._last_activated_at = time.monotonic() - 60

        await player.apply_remote_state(
            track_id="222", queue_item_id=9, position_ms=10_000, playing_state=2
        )

        backend.seek.assert_awaited()

    async def test_another_track_right_after_rejoining_is_a_real_command(self) -> None:
        player, _ = _playing_player()
        player._last_activated_at = time.monotonic()
        player._load_track_locked = AsyncMock(return_value=False)  # type: ignore[method-assign]

        await player.apply_remote_state(
            track_id="333", queue_item_id=10, position_ms=0, playing_state=2
        )

        player._load_track_locked.assert_awaited()

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


class TestCarryingOnUnderAPlayingDevice:
    """The proxy restarted while the device kept playing one of its entries: the
    app rejoining must not silence it, and the state the server replays (what
    it remembers from before) must not be applied over what the device does."""

    def _fresh_player(self, playing):  # type: ignore[no-untyped-def]
        player, backend = _make_player()
        player.queue.generation = ("tag", 64)
        backend.playing_entry = AsyncMock(return_value=playing)
        backend.adopt = AsyncMock()
        backend.stop = AsyncMock()
        backend.play = AsyncMock()
        player._send_state_update = AsyncMock()  # type: ignore[method-assign]
        return player, backend

    @staticmethod
    def _playing(**kw):  # type: ignore[no-untyped-def]
        from qobuz_proxy.backends import AdoptedPlayback

        fields = dict(
            track_id="333",
            queue_item_id=3,
            session="tag",
            major=64,
            position_ms=90_000,
            paused=False,
            url="http://x/audio/333",
        )
        fields.update(kw)
        return AdoptedPlayback(**fields)  # type: ignore[arg-type]

    async def test_claiming_does_not_stop_a_device_playing_our_entry(self) -> None:
        player, backend = self._fresh_player(self._playing())

        await player.claim_device()

        backend.stop.assert_not_called()

    async def test_claiming_still_stops_a_device_playing_something_else(self) -> None:
        player, backend = self._fresh_player(None)

        await player.claim_device()

        backend.stop.assert_awaited_once()

    async def test_another_sessions_entry_is_not_carried_on_with(self) -> None:
        player, backend = self._fresh_player(self._playing(session="other"))

        await player.claim_device()

        backend.stop.assert_awaited_once()

    async def test_the_replayed_state_is_answered_with_what_the_device_does(self) -> None:
        player, backend = self._fresh_player(self._playing())
        await player.claim_device()

        # The server remembers item 2 paused at 154s; the device is on item 3.
        await player.apply_remote_state(
            track_id="222", queue_item_id=2, position_ms=154_749, playing_state=3
        )

        assert player._current_track is not None
        assert (player._current_track.track_id, player._current_track.queue_item_id) == ("333", 3)
        assert player._state == PlaybackState.PLAYING
        assert 90_000 <= player.current_position_ms < 91_000
        backend.adopt.assert_awaited_once()
        backend.play.assert_not_called()
        backend.stop.assert_not_called()
        player._send_state_update.assert_awaited()

    async def test_a_paused_device_is_adopted_paused(self) -> None:
        player, _ = self._fresh_player(self._playing(paused=True))
        await player.claim_device()

        await player.apply_remote_state(
            track_id="222", queue_item_id=2, position_ms=0, playing_state=2
        )

        assert player._state == PlaybackState.PAUSED

    async def test_an_older_queue_on_the_device_is_stopped_and_the_state_applied(self) -> None:
        player, backend = self._fresh_player(self._playing(major=60))
        player._load_track_locked = AsyncMock(return_value=False)  # type: ignore[method-assign]
        await player.claim_device()

        await player.apply_remote_state(
            track_id="222", queue_item_id=2, position_ms=0, playing_state=2
        )

        backend.stop.assert_awaited_once()
        backend.adopt.assert_not_called()
        player._load_track_locked.assert_awaited()

    async def test_only_the_first_replay_is_answered_that_way(self) -> None:
        player, backend = self._fresh_player(self._playing())
        await player.claim_device()
        await player.apply_remote_state(
            track_id="222", queue_item_id=2, position_ms=0, playing_state=3
        )
        backend.adopt.reset_mock()
        player._load_track_locked = AsyncMock(return_value=False)  # type: ignore[method-assign]

        await player.apply_remote_state(
            track_id="444", queue_item_id=4, position_ms=0, playing_state=2
        )

        backend.adopt.assert_not_called()
        player._load_track_locked.assert_awaited()

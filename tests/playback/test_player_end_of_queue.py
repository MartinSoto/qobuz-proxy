"""The end of the queue: the player lands on the first item of the play order,
stopped at 0, and a bare PLAY starts it again; a PLAY that cannot be honored
is answered with a state report. See docs/queue-and-shuffle-wire-observations.md,
O2, O21 and O22.
"""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.queue import QueueEntry, QueueTrack

from tests.playback.test_player import _make_player


def _ended_player(order):  # type: ignore[no-untyped-def]
    player, backend = _make_player()
    backend.stop = AsyncMock()
    backend.play = AsyncMock()
    backend.play.return_value.blob = "blob"
    backend.play.return_value.format_id = 6
    queue_state = MagicMock()
    queue_state.repeat_mode = 0
    player.queue.get_state = AsyncMock(return_value=queue_state)
    player.queue.play_order = AsyncMock(return_value=order)
    player._current_track = QueueTrack(queue_item_id=9, track_id="999")
    player._state = PlaybackState.PLAYING
    player._send_state_update = AsyncMock()  # type: ignore[method-assign]
    return player, backend


class TestEndOfQueue:
    async def test_lands_on_the_first_item_stopped(self) -> None:
        player, _ = _ended_player([QueueEntry(track_id="111", queue_item_id=0)])

        await player._handle_track_ended(player._current_track)

        assert player._state == PlaybackState.STOPPED
        assert player._current_track is not None
        assert (player._current_track.track_id, player._current_track.queue_item_id) == ("111", 0)
        assert player._position_value_ms == 0
        assert player._current_duration_ms == 1000  # the real duration is reported
        player._send_state_update.assert_awaited()

    async def test_nothing_stays_loaded_without_a_queue_to_land_on(self) -> None:
        player, _ = _ended_player(None)  # the snapshot is outdated or never came

        await player._handle_track_ended(player._current_track)

        assert player._state == PlaybackState.STOPPED
        assert player._current_track is None

    async def test_a_bare_play_afterwards_starts_the_first_item(self) -> None:
        player, backend = _ended_player([QueueEntry(track_id="111", queue_item_id=0)])
        await player._handle_track_ended(player._current_track)

        assert await player.play() is True

        backend.play.assert_awaited_once()
        assert backend.play.await_args.args[0].track_id == "111"
        assert backend.play.await_args.args[0].queue_item_id == 0
        assert player._state == PlaybackState.PLAYING


class TestPlayWithNothingLoaded:
    async def test_the_app_is_told_the_real_state(self) -> None:
        player, _ = _make_player()
        player._send_state_update = AsyncMock()  # type: ignore[method-assign]

        assert await player.play() is False

        player._send_state_update.assert_awaited_once()

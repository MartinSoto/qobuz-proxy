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


class TestHandoffOfADeviceThatKeepsPlaying:
    """The coordinator changes while the device plays its own queue: the queue
    moves to the new coordinator and carries on, so the player must report that,
    not the 'loading' the detach left behind."""

    def _player(self, keeps_playing: bool = True, playing=None):  # type: ignore[no-untyped-def]
        player, backend = _make_player()
        player.queue.generation = ("tag", 64)
        backend.keeps_playing_on_shutdown = keeps_playing
        backend.disconnect = AsyncMock()
        backend.retarget = AsyncMock(return_value=True)
        backend.playing_entry = AsyncMock(return_value=playing)
        backend.adopt = AsyncMock()
        player._send_state_update = AsyncMock()  # type: ignore[method-assign]
        player._current_track = QueueTrack(queue_item_id=3, track_id="333")
        player._state = PlaybackState.PLAYING
        player._backend_attached = True
        return player, backend

    @staticmethod
    def _playing(**kw):  # type: ignore[no-untyped-def]
        return TestCarryingOnUnderAPlayingDevice._playing(**kw)

    async def test_detaching_does_not_stop_a_device_that_plays_on(self) -> None:
        player, backend = self._player(keeps_playing=True)

        await player._detach_locked()

        backend.disconnect.assert_awaited_once_with(send_device_stop=False)

    async def test_detaching_still_stops_any_other_device(self) -> None:
        player, backend = self._player(keeps_playing=False)

        await player._detach_locked()

        backend.disconnect.assert_awaited_once_with(send_device_stop=True)

    async def test_the_new_coordinator_playing_our_entry_is_taken_over(self) -> None:
        player, backend = self._player(playing=self._playing(queue_item_id=4, track_id="444"))
        await player._detach_locked()
        assert player._state == PlaybackState.LOADING  # what the detach reports

        await player._retarget_locked("10.0.0.9", 1400)

        assert player._state == PlaybackState.PLAYING
        assert player._current_track is not None
        assert player._current_track.queue_item_id == 4  # it may have moved on meanwhile
        backend.adopt.assert_awaited_once()
        player._send_state_update.assert_awaited()

    async def test_nothing_found_straight_away_leaves_it_loading_for_now(self) -> None:
        player, _ = self._player(playing=None)
        await player._detach_locked()

        await player._retarget_locked("10.0.0.9", 1400)

        assert player._state == PlaybackState.LOADING  # the queue may still be arriving

    async def test_giving_up_waiting_settles_on_stopped(self) -> None:
        player, _ = self._player(playing=None)
        await player._detach_locked()
        await player._retarget_locked("10.0.0.9", 1400)

        await player._resync_after_retarget(settled=True)

        assert player._state == PlaybackState.STOPPED

    async def test_the_settled_callback_takes_over_what_the_device_plays(self) -> None:
        player, backend = self._player(playing=self._playing())
        await player._detach_locked()
        await player._retarget_locked("10.0.0.9", 1400)
        player._state = PlaybackState.LOADING  # as if it had been found only later

        await player._resync_after_retarget(settled=True)

        assert player._state == PlaybackState.PLAYING

    async def test_another_backend_is_left_to_its_own_poll_loop(self) -> None:
        player, backend = self._player(keeps_playing=False, playing=self._playing())
        await player._detach_locked()

        await player._retarget_locked("10.0.0.9", 1400)

        backend.adopt.assert_not_called()
        assert player._state == PlaybackState.LOADING


class TestSeekAndTrackChangeFromTheSonosSide:
    async def test_a_position_that_jumps_is_reported_at_once(self) -> None:
        player, _ = _playing_player()
        player.enqueue = lambda fn, **k: calls.append(fn)  # type: ignore[method-assign, assignment]
        calls: list = []

        player._on_position_update(player.current_position_ms + 120_000)  # a seek forward

        assert player._send_state_update in calls

    async def test_ordinary_drift_is_not_a_jump(self) -> None:
        player, _ = _playing_player()
        calls: list = []
        player.enqueue = lambda fn, **k: calls.append(fn)  # type: ignore[method-assign, assignment]

        player._on_position_update(player.current_position_ms + 900)

        assert player._send_state_update not in calls

    async def test_a_position_while_not_playing_is_not_a_jump(self) -> None:
        player, _ = _playing_player()
        player._state = PlaybackState.PAUSED
        calls: list = []
        player.enqueue = lambda fn, **k: calls.append(fn)  # type: ignore[method-assign, assignment]

        player._on_position_update(player.current_position_ms + 120_000)

        assert player._send_state_update not in calls

    async def test_the_app_is_not_told_stopped_while_the_device_moves_between_entries(self) -> None:
        player, _ = _playing_player()
        player._get_track_metadata = AsyncMock(return_value={"duration_ms": 1000})  # type: ignore[method-assign]
        player._intended_playing = True
        player._state = PlaybackState.LOADING  # TRANSITIONING seen by the poll

        await player._handle_item_changed("444", 4, None)

        assert player._state == PlaybackState.PLAYING

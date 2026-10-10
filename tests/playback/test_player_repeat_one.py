"""Repeat-one and the gapless path: while repeat-one is on nothing may be
armed as the next track (the device would otherwise play it instead of
stopping so the current track can be restarted), and switching it on or off
must clear or restore the arming. See docs/queue-and-shuffle-wire-observations.md,
O11, and Player._rearm_gapless_for_repeat_change.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState

from tests.playback.test_player import _make_player as _make_gapless_player

NEXT = {"trackId": "222", "queueItemId": 9}
REPEAT_OFF, REPEAT_ONE, REPEAT_ALL = 1, 2, 3  # protocol LoopMode values


def _make_player(next_track_info=None):  # type: ignore[no-untyped-def]
    player, backend = _make_gapless_player(next_track_info=next_track_info)
    player.queue.set_repeat_mode = AsyncMock()  # the shared helper's queue is a plain mock
    backend.set_repeat_mode = AsyncMock()  # and so is its backend
    return player, backend


class TestRepeatOneAndGapless:
    async def test_nothing_is_armed_while_repeat_one_is_on(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        player._state = PlaybackState.PLAYING
        await player.set_loop_mode(REPEAT_ONE)
        backend.set_next_track.reset_mock()

        await player._prepare_next_track_for_gapless()

        backend.set_next_track.assert_not_called()
        assert player._gapless_armed is False

    async def test_switching_repeat_one_on_clears_the_armed_next_track(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        player._state = PlaybackState.PLAYING
        await player._prepare_next_track_for_gapless()
        assert player._gapless_armed is True
        backend.clear_next_track.reset_mock()
        backend.set_next_track.reset_mock()

        await player.set_loop_mode(REPEAT_ONE)

        backend.clear_next_track.assert_awaited_once()
        backend.set_next_track.assert_not_called()
        assert player._gapless_armed is False

    async def test_switching_repeat_one_off_arms_the_next_track_again(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        player._state = PlaybackState.PLAYING
        await player.set_loop_mode(REPEAT_ONE)
        backend.set_next_track.reset_mock()

        await player.set_loop_mode(REPEAT_OFF)

        backend.set_next_track.assert_awaited_once()
        assert player._gapless_armed is True

    async def test_not_playing_it_only_clears_on_the_way_out(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        player._state = PlaybackState.PAUSED
        await player.set_loop_mode(REPEAT_ONE)
        backend.set_next_track.reset_mock()

        await player.set_loop_mode(REPEAT_OFF)

        backend.set_next_track.assert_not_called()  # re-armed when playback resumes

    async def test_repeat_all_and_off_do_not_touch_the_arming(self) -> None:
        """The server wraps its own next pointer for repeat-all (O17)."""
        player, backend = _make_player(next_track_info=NEXT)
        player._state = PlaybackState.PLAYING
        await player._prepare_next_track_for_gapless()
        backend.clear_next_track.reset_mock()

        await player.set_loop_mode(REPEAT_ALL)
        await player.set_loop_mode(REPEAT_OFF)

        backend.clear_next_track.assert_not_called()
        assert player._gapless_armed is True

    async def test_repeating_the_same_announcement_is_idempotent(self) -> None:
        """Type 97 arrives twice per change."""
        player, backend = _make_player(next_track_info=NEXT)
        player._state = PlaybackState.PLAYING
        await player._prepare_next_track_for_gapless()
        backend.clear_next_track.reset_mock()

        await player.set_loop_mode(REPEAT_ONE)
        await player.set_loop_mode(REPEAT_ONE)

        backend.clear_next_track.assert_awaited_once()

    async def test_a_backend_without_gapless_is_left_alone(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        backend.supports_gapless = False
        backend.clear_next_track = AsyncMock()

        await player.set_loop_mode(REPEAT_ONE)

        backend.clear_next_track.assert_not_called()


class TestRepeatModeIsRelayedToTheBackend:
    async def test_a_change_reaches_the_backend(self) -> None:
        player, backend = _make_player()

        await player.set_loop_mode(REPEAT_ONE)
        await player.set_loop_mode(REPEAT_ALL)
        await player.set_loop_mode(REPEAT_OFF)

        assert [c.args[0] for c in backend.set_repeat_mode.await_args_list] == ["one", "all", "off"]

    async def test_the_same_announcement_twice_is_relayed_once(self) -> None:
        """Type 97 arrives twice per change."""
        player, backend = _make_player()

        await player.set_loop_mode(REPEAT_ONE)
        await player.set_loop_mode(REPEAT_ONE)

        backend.set_repeat_mode.assert_awaited_once_with("one")

    async def test_a_backend_failure_does_not_break_the_mode_change(self) -> None:
        player, backend = _make_player()
        backend.set_repeat_mode = AsyncMock(side_effect=RuntimeError("device gone"))

        await player.set_loop_mode(REPEAT_ONE)  # must not raise

        player.queue.set_repeat_mode.assert_awaited_once()


class TestNoArmingWithoutGaplessSupport:
    """A backend that does not arm tracks must never be asked to, whichever
    path wants a next track armed (a changed next pointer included)."""

    async def test_a_changed_next_pointer_arms_nothing(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        backend.supports_gapless = False
        player._state = PlaybackState.PLAYING

        await player.on_next_track_info_changed()

        backend.set_next_track.assert_not_called()
        assert player._gapless_armed is False

    async def test_the_direct_arm_path_is_guarded_too(self) -> None:
        player, backend = _make_player(next_track_info=NEXT)
        backend.supports_gapless = False

        async with player._gapless_arm_lock:
            await player._prepare_next_track_locked()

        backend.set_next_track.assert_not_called()

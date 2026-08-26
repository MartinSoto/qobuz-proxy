"""Regression tests for a "cold pause": the renderer is made active while the
track is already paused elsewhere (e.g. on the phone), so a track gets loaded
here but playback never actually starts on this backend before the pause is
applied.

Bug: selecting this renderer while paused, then pressing play, restarted the
track from 0ms instead of resuming where the other renderer left off. Root
cause — apply_remote_state's seek()/`_pause_locked()` calls both silently
no-op on a freshly loaded (STOPPED) track, so the paused position was dropped
on the floor; the following play() then started fresh at position 0. See
player.py's `_backend_engaged` flag and `_pause_locked`/`_play_locked`.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState

from tests.playback.test_player_serialization import _make_player


class TestColdPauseRemembersPosition:
    async def test_loading_paused_remembers_position_without_touching_backend(self) -> None:
        player, backend = _make_player()
        backend.pause = AsyncMock()  # type: ignore[method-assign]

        await player.apply_remote_state(
            track_id="222",
            queue_item_id=9,
            position_ms=90_000,
            playing_state=3,  # PAUSED
        )

        assert player._state == PlaybackState.PAUSED
        assert player.current_position_ms == 90_000
        # Nothing was ever loaded/started on the actual device — a plain
        # pause of a real live device would be wrong here.
        backend.pause.assert_not_awaited()

    async def test_playing_after_cold_pause_resumes_at_remembered_position(self) -> None:
        player, backend = _make_player()
        backend.pause = AsyncMock()  # type: ignore[method-assign]
        backend.resume = AsyncMock()  # type: ignore[method-assign]
        backend.seek = AsyncMock()  # type: ignore[method-assign]

        await player.apply_remote_state(
            track_id="222",
            queue_item_id=9,
            position_ms=90_000,
            playing_state=3,  # PAUSED, as reported by the phone at selection time
        )

        # The follow-up "press play" command carries no explicit position —
        # the app expects the renderer to already know where it paused.
        await player.apply_remote_state(
            track_id="222",
            queue_item_id=9,
            position_ms=None,
            playing_state=2,  # PLAYING
        )

        assert player._state == PlaybackState.PLAYING
        assert player.current_position_ms >= 90_000
        assert backend.played == ["222"]
        # A genuine (never-engaged) resume must start fresh + seek, not call
        # resume() on a transport that never had anything loaded.
        backend.resume.assert_not_awaited()
        backend.seek.assert_awaited_once_with(90_000)

    async def test_a_genuine_pause_still_resumes_via_backend_resume(self) -> None:
        """Sanity check: a real pause (backend already engaged) must keep
        using backend.resume(), not the cold-pause fresh-start path."""
        player, backend = _make_player()
        backend.pause = AsyncMock()  # type: ignore[method-assign]
        backend.resume = AsyncMock(return_value=True)  # type: ignore[method-assign]

        await player.play_track(queue_item_id=1, track_id="42", position_ms=0)
        assert player._state == PlaybackState.PLAYING

        await player.pause()
        assert player._state == PlaybackState.PAUSED
        backend.pause.assert_awaited_once()

        await player.play()

        assert player._state == PlaybackState.PLAYING
        backend.resume.assert_awaited_once()
        # No second play() on the backend — it just resumed the existing one.
        assert backend.played == ["42"]

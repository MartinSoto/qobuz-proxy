"""Tests for external-takeover ("hijack") detection during playback.

A shared DLNA/Sonos renderer can be handed to a completely different
source (another app, someone grouping into it via the Sonos app) while
still reporting PLAYING throughout — get_state() alone can't tell that
apart from us still playing. AudioBackend.is_playing_our_content() is the
extra signal _playback_monitor_loop uses to catch it.
"""

import asyncio
from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.player import _HIJACK_CHECK_INTERVAL_POLLS

from tests.playback.test_player_serialization import _make_player


async def _run_one_monitor_cycle(player) -> None:  # type: ignore[no-untyped-def]
    """Run _playback_monitor_loop for exactly one 0.5s poll cycle."""
    player._is_running = True
    task = asyncio.create_task(player._playback_monitor_loop())
    await asyncio.sleep(0.6)
    player._is_running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class TestHijackDetection:
    async def test_takeover_stops_the_player(self) -> None:
        player, backend = _make_player()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.get_position = AsyncMock(return_value=30_000)
        backend.is_playing_our_content = AsyncMock(return_value=False)
        player._state = PlaybackState.PLAYING
        player._hijack_check_countdown = 1  # force the check on the first cycle

        await _run_one_monitor_cycle(player)

        assert player.state == PlaybackState.STOPPED

    async def test_takeover_invokes_the_hijack_callback(self) -> None:
        # A plain STOPPED report leaves the app still believing it's
        # connected to this renderer — there's no protocol message to
        # tell it otherwise (confirmed against two independent Qobuz
        # Connect reverse-engineering efforts), so the closest available
        # signal is forcing a real WebSocket reconnect.
        player, backend = _make_player()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.get_position = AsyncMock(return_value=30_000)
        backend.is_playing_our_content = AsyncMock(return_value=False)
        player._state = PlaybackState.PLAYING
        player._hijack_check_countdown = 1
        callback = AsyncMock()
        player.set_hijack_detected_callback(callback)

        await _run_one_monitor_cycle(player)

        callback.assert_awaited_once_with("external takeover detected")

    async def test_no_takeover_does_not_invoke_the_hijack_callback(self) -> None:
        player, backend = _make_player()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.get_position = AsyncMock(return_value=30_000)
        backend.is_playing_our_content = AsyncMock(return_value=True)
        player._state = PlaybackState.PLAYING
        player._hijack_check_countdown = 1
        callback = AsyncMock()
        player.set_hijack_detected_callback(callback)

        await _run_one_monitor_cycle(player)

        callback.assert_not_called()

    async def test_a_failing_hijack_callback_does_not_raise(self) -> None:
        player, backend = _make_player()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.get_position = AsyncMock(return_value=30_000)
        backend.is_playing_our_content = AsyncMock(return_value=False)
        player._state = PlaybackState.PLAYING
        player._hijack_check_countdown = 1
        player.set_hijack_detected_callback(AsyncMock(side_effect=OSError("no connection")))

        await _run_one_monitor_cycle(player)  # must not raise/crash the loop

        assert player.state == PlaybackState.STOPPED

    async def test_no_takeover_leaves_player_playing(self) -> None:
        player, backend = _make_player()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.get_position = AsyncMock(return_value=30_000)
        backend.is_playing_our_content = AsyncMock(return_value=True)
        player._state = PlaybackState.PLAYING
        player._hijack_check_countdown = 1

        await _run_one_monitor_cycle(player)

        assert player.state == PlaybackState.PLAYING
        backend.is_playing_our_content.assert_awaited_once()

    async def test_check_is_throttled_not_run_every_cycle(self) -> None:
        player, backend = _make_player()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.get_position = AsyncMock(return_value=30_000)
        backend.is_playing_our_content = AsyncMock(return_value=True)
        player._state = PlaybackState.PLAYING
        player._hijack_check_countdown = _HIJACK_CHECK_INTERVAL_POLLS + 5  # not due yet

        await _run_one_monitor_cycle(player)

        backend.is_playing_our_content.assert_not_called()

    async def test_not_checked_while_paused(self) -> None:
        player, backend = _make_player()
        backend._state = PlaybackState.PAUSED
        backend.get_state = AsyncMock(return_value=PlaybackState.PAUSED)
        backend.is_playing_our_content = AsyncMock(return_value=False)
        player._state = PlaybackState.PAUSED

        await _run_one_monitor_cycle(player)

        backend.is_playing_our_content.assert_not_called()

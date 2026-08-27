"""Tests for the player's reaction to an external takeover ("hijack") of
its renderer.

A shared DLNA/Sonos renderer can be handed to a completely different
source (another app, someone grouping into it via the Sonos app) while
still reporting PLAYING throughout — get_state() alone can't tell that
apart from us still playing. Detecting it (AudioBackend.is_playing_our_content(),
throttled) is DLNABackend._poll_state_loop's job — see
tests/backends/test_dlna_backend.py. This file only covers what the player
does once the backend calls back via on_external_takeover().
"""

import asyncio
from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState

from tests.playback.test_player_serialization import _make_player


class TestHijackDetection:
    async def test_takeover_stops_the_player(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING

        backend._notify_external_takeover()
        await asyncio.sleep(0.05)  # let the scheduled handler task run

        assert player.state == PlaybackState.STOPPED

    async def test_takeover_zeroes_position(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING
        player._position_value_ms = 30_000

        backend._notify_external_takeover()
        await asyncio.sleep(0.05)

        assert player.current_position_ms == 0

    async def test_takeover_invokes_the_hijack_callback(self) -> None:
        # A plain STOPPED report leaves the app still believing it's
        # connected to this renderer — there's no protocol message to
        # tell it otherwise (confirmed against two independent Qobuz
        # Connect reverse-engineering efforts), so the closest available
        # signal is forcing a real WebSocket reconnect.
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING
        callback = AsyncMock()
        player.set_hijack_detected_callback(callback)

        backend._notify_external_takeover()
        await asyncio.sleep(0.05)

        callback.assert_awaited_once_with("external takeover detected")

    async def test_a_failing_hijack_callback_does_not_raise(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING
        player.set_hijack_detected_callback(AsyncMock(side_effect=OSError("no connection")))

        backend._notify_external_takeover()
        await asyncio.sleep(0.05)  # must not raise/crash

        assert player.state == PlaybackState.STOPPED

    async def test_no_takeover_leaves_player_playing(self) -> None:
        """Baseline: without a callback firing, nothing changes."""
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING

        await asyncio.sleep(0.05)

        assert player.state == PlaybackState.PLAYING

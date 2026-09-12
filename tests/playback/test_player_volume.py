"""Tests for QobuzPlayer.set_volume()'s same-value no-op guard.

Regression for a live-confirmed bug (2026-09-12): while this proxy's
Controller role used a second, synthetic deviceUuid distinct from its
Renderer role, a server confused about which identity owned the
connection echoed our own volume-change report straight back as a fresh
SET_VOLUME command — every report became another command to handle, a
self-sustaining loop (~4000 calls in 27s in one capture) that starved
the single command queue badly enough to also block natural-track-end
detection for the rest of that session. Joining the Controller role
under the Renderer's own deviceUuid fixed the identity confusion this
stemmed from, but a same-value command is always safe to no-op
regardless of cause — nothing is lost since nothing changes — so this
guard stays as a second, independent line of defense against the same
failure mode however it might recur.
"""

from unittest.mock import AsyncMock

from tests.playback.test_player_serialization import _make_player


class TestSetVolumeNoOp:
    async def test_same_value_is_a_noop(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 50

        result = await player.set_volume(50)

        assert result == 50
        backend.set_volume.assert_not_awaited()
        report_callback.assert_not_awaited()

    async def test_different_value_still_applies_and_reports(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 50

        result = await player.set_volume(60)

        assert result == 60
        assert player._volume == 60
        backend.set_volume.assert_awaited_once_with(60)
        report_callback.assert_awaited_once_with(60)

    async def test_repeated_same_value_command_only_ever_applies_once(self) -> None:
        """Reproduces the actual failure shape: the same value arriving
        again and again (as an echoed command would) must not keep
        re-triggering backend calls or reports."""
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 50

        await player.set_volume(23)
        for _ in range(50):
            await player.set_volume(23)

        backend.set_volume.assert_awaited_once_with(23)
        report_callback.assert_awaited_once_with(23)

"""Tests for VolumeCommandHandler.

Regression for a live-confirmed bug (2026-09-13, "the volume knob jumping
back and forth"): _handle_volume_changed (SRVR_CTRL_VOLUME_CHANGED, type
87) used to re-apply the broadcast volume unconditionally, including our
own — since this proxy joins Renderer and Controller on the same socket,
every one of our own outbound RNDR_SRVR_VOLUME_CHANGED reports gets
broadcast straight back to us as this exact message. Re-applying it
turned our own report into another command, which produced another
report, which came back again: confirmed directly in a capture as a
repeating oscillation between two adjacent real slider values (e.g.
13/14), each one actually reaching the physical device before the next
overwrote it. Fixed by making this handler observation-only — see its
own docstring for the full reasoning.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.playback.volume_handler import VolumeCommandHandler
from qobuz_proxy.proto import qconnect_payload_pb2 as pb


def _set_volume_msg(
    *, volume: int | None = None, volume_delta: int | None = None
) -> pb.QConnectMessage:
    """Build a server->renderer SRVR_RNDR_SET_VOLUME (type 42) message."""
    msg = pb.QConnectMessage()
    msg.messageType = 42
    if volume is not None:
        msg.srvrRndrSetVolume.volume = volume
    if volume_delta is not None:
        msg.srvrRndrSetVolume.volumeDelta = volume_delta
    return msg


def _volume_changed_msg(*, renderer_id: int, volume: int) -> pb.QConnectMessage:
    """Build a server->controller SRVR_CTRL_VOLUME_CHANGED (type 87) broadcast."""
    msg = pb.QConnectMessage()
    msg.messageType = 87
    msg.srvrCtrlVolumeChanged.rendererId = renderer_id
    msg.srvrCtrlVolumeChanged.volume = volume
    return msg


class TestSetVolumeCommand:
    """SRVR_RNDR_SET_VOLUME (type 42) — the actual command a Renderer
    must act on."""

    async def test_absolute_volume_applies(self) -> None:
        player = AsyncMock()
        handler = VolumeCommandHandler(player)

        await handler.handle_message(42, _set_volume_msg(volume=30))

        player.set_volume.assert_awaited_once_with(30)
        player.set_volume_delta.assert_not_awaited()

    async def test_volume_delta_applies(self) -> None:
        player = AsyncMock()
        handler = VolumeCommandHandler(player)

        await handler.handle_message(42, _set_volume_msg(volume_delta=-5))

        player.set_volume_delta.assert_awaited_once_with(-5)
        player.set_volume.assert_not_awaited()

    async def test_missing_field_is_a_noop(self) -> None:
        player = AsyncMock()
        handler = VolumeCommandHandler(player)
        msg = pb.QConnectMessage()
        msg.messageType = 42

        await handler.handle_message(42, msg)

        player.set_volume.assert_not_awaited()
        player.set_volume_delta.assert_not_awaited()


class TestVolumeChangedBroadcastIsObservationOnly:
    """SRVR_CTRL_VOLUME_CHANGED (type 87) — a Controller-facing
    broadcast, not a command. Must never drive Player, regardless of
    whose rendererId it names — see _handle_volume_changed's own
    docstring for why even our own id must not be re-applied."""

    async def test_broadcast_for_our_own_renderer_is_not_reapplied(self) -> None:
        """The exact failure shape: an echo of our own just-sent report,
        broadcast back to us because of the dual Renderer+Controller
        join, must not be treated as a fresh command."""
        player = AsyncMock()
        handler = VolumeCommandHandler(player)

        await handler.handle_message(87, _volume_changed_msg(renderer_id=1234, volume=14))

        player.set_volume.assert_not_awaited()

    async def test_broadcast_for_a_different_renderer_is_also_not_reapplied(self) -> None:
        player = AsyncMock()
        handler = VolumeCommandHandler(player)

        await handler.handle_message(87, _volume_changed_msg(renderer_id=9999, volume=80))

        player.set_volume.assert_not_awaited()

    async def test_missing_field_is_a_noop(self) -> None:
        player = AsyncMock()
        handler = VolumeCommandHandler(player)
        msg = pb.QConnectMessage()
        msg.messageType = 87

        await handler.handle_message(87, msg)  # must not raise

        player.set_volume.assert_not_awaited()

    async def test_repeated_broadcasts_never_cause_a_command(self) -> None:
        """Reproduces the actual oscillation shape observed live: a
        repeating pair of broadcast values must produce zero set_volume
        calls, not just a bounded few."""
        player = AsyncMock()
        handler = VolumeCommandHandler(player)

        for volume in (13, 14, 13, 14, 13, 14):
            await handler.handle_message(87, _volume_changed_msg(renderer_id=1234, volume=volume))

        player.set_volume.assert_not_awaited()


class TestUnhandledMessageType:
    async def test_unknown_type_does_not_raise(self) -> None:
        player = AsyncMock()
        handler = VolumeCommandHandler(player)

        await handler.handle_message(999, pb.QConnectMessage())  # must not raise

        player.set_volume.assert_not_awaited()

    async def test_get_message_types_lists_both(self) -> None:
        handler = VolumeCommandHandler(AsyncMock())

        assert handler.get_message_types() == [42, 87]

"""Tests for SET_ACTIVE handling via PlaybackCommandHandler.

SrvrRndrSetActive is the Qobuz server's authoritative signal for whether
*this* renderer is the one the app is actually driving right now — a
signal previously only observed, never actually tracked.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.command_handler import PlaybackCommandHandler
from qobuz_proxy.playback.queue import QueueTrack
from qobuz_proxy.proto import qconnect_payload_pb2 as pb

from tests.playback.test_player_serialization import _make_player


def _set_active_msg(active: bool):  # type: ignore[no-untyped-def]
    """Build a server->renderer SET_ACTIVE (type 43) protobuf message."""
    msg = pb.QConnectMessage()
    msg.messageType = 43
    msg.srvrRndrSetActive.active = active
    return msg


class TestSetActiveHandling:
    async def test_active_true_marks_the_player_active(self) -> None:
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        player.broadcast_current_volume = AsyncMock()  # type: ignore[method-assign]
        backend.stop = AsyncMock()  # type: ignore[method-assign]

        assert player.is_active_renderer is False  # default, before any message

        await handler._handle_set_active(_set_active_msg(True))

        assert player.is_active_renderer is True
        player.broadcast_current_volume.assert_awaited_once()

    async def test_active_true_claims_the_device(self) -> None:
        # A shared DLNA/Sonos renderer may already be playing something
        # from a completely different source when selected — claim a
        # silent, ready state (Spotify-Connect-style) instead of leaving
        # it running until the app actually picks a track.
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        player.broadcast_current_volume = AsyncMock()  # type: ignore[method-assign]
        backend.stop = AsyncMock()  # type: ignore[method-assign]

        await handler._handle_set_active(_set_active_msg(True))

        backend.stop.assert_awaited_once()

    async def test_active_true_does_not_stop_our_own_already_playing_content(self) -> None:
        """SET_ACTIVE(true) fires again whenever the app's control
        WebSocket reconnects (a token refresh, a brief network blip) —
        not only on a genuinely fresh selection — and our own content can
        have been playing the whole time underneath a control channel
        that happened to be down. claim_device()'s silence-a-shared-
        device precaution must not kill playback that's already
        legitimately ours."""
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        player.broadcast_current_volume = AsyncMock()  # type: ignore[method-assign]
        backend.stop = AsyncMock()  # type: ignore[method-assign]
        player._state = PlaybackState.PLAYING
        player._current_track = QueueTrack(queue_item_id=0, track_id="1")

        await handler._handle_set_active(_set_active_msg(True))

        backend.stop.assert_not_awaited()
        assert player.is_active_renderer is True  # activation itself still applies

    async def test_a_failing_device_stop_does_not_block_activation(self) -> None:
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        player.broadcast_current_volume = AsyncMock()  # type: ignore[method-assign]
        backend.stop = AsyncMock(side_effect=OSError("unreachable"))  # type: ignore[method-assign]

        await handler._handle_set_active(_set_active_msg(True))  # must not raise

        assert player.is_active_renderer is True
        player.broadcast_current_volume.assert_awaited_once()

    async def test_active_false_marks_the_player_inactive_and_stops_playback(self) -> None:
        player, _backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        await player.set_active_renderer(True)
        player.stop_playback = AsyncMock()  # type: ignore[method-assign]

        await handler._handle_set_active(_set_active_msg(False))

        assert player.is_active_renderer is False
        player.stop_playback.assert_awaited_once()

    async def test_active_state_is_relayed_to_the_backend(self) -> None:
        """See AudioBackend.set_active — a Sonos auto-discovery household
        polls every discovered room's device continuously regardless of
        which one Qobuz is actually driving; the backend needs this signal
        to know when its own hijack detection is actually meaningful."""
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        player.broadcast_current_volume = AsyncMock()  # type: ignore[method-assign]
        backend.stop = AsyncMock()  # type: ignore[method-assign]

        assert backend._active is True  # default

        await handler._handle_set_active(_set_active_msg(False))
        assert backend._active is False

        await handler._handle_set_active(_set_active_msg(True))
        assert backend._active is True

    async def test_missing_field_is_ignored(self) -> None:
        player, _backend = _make_player()
        handler = PlaybackCommandHandler(player, queue=None)
        await player.set_active_renderer(True)

        msg = pb.QConnectMessage()
        msg.messageType = 43  # no srvrRndrSetActive payload set

        await handler._handle_set_active(msg)

        assert player.is_active_renderer is True  # untouched

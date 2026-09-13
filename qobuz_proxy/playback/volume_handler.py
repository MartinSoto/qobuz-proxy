"""
Volume command handler for WebSocket integration.

Processes volume commands from the Qobuz app via WsManager.
"""

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .player import QobuzPlayer

logger = logging.getLogger(__name__)

# QConnect message types for volume commands
MSG_TYPE_SRVR_RNDR_SET_VOLUME = 42  # Server -> Renderer: set volume
MSG_TYPE_SRVR_CTRL_VOLUME_CHANGED = 87  # Server broadcast: volume changed


class VolumeCommandHandler:
    """
    Handles volume commands from WebSocket.

    Processes both absolute volume and volume delta commands.
    """

    def __init__(self, player: "QobuzPlayer"):
        """Initialize handler."""
        self.player = player

    def get_message_types(self) -> list[int]:
        """Get list of message types this handler processes."""
        return [
            MSG_TYPE_SRVR_RNDR_SET_VOLUME,
            MSG_TYPE_SRVR_CTRL_VOLUME_CHANGED,
        ]

    async def handle_message(self, msg_type: int, message: Any) -> None:
        """Handle a volume command message."""
        try:
            if msg_type == MSG_TYPE_SRVR_RNDR_SET_VOLUME:
                await self._handle_set_volume(message)
            elif msg_type == MSG_TYPE_SRVR_CTRL_VOLUME_CHANGED:
                await self._handle_volume_changed(message)
            else:
                logger.warning(f"Unhandled volume message type: {msg_type}")
        except Exception as e:
            logger.error(f"Error handling volume command {msg_type}: {e}", exc_info=True)

    async def _handle_set_volume(self, message: Any) -> None:
        """
        Handle SRVR_RNDR_SET_VOLUME message.

        Supports both absolute volume and volume delta.
        """
        if not message.HasField("srvrRndrSetVolume"):
            return

        vol_msg = message.srvrRndrSetVolume

        # Check for absolute volume first
        if vol_msg.HasField("volume"):
            volume = vol_msg.volume
            logger.debug(f"Received set volume: {volume}")
            await self.player.set_volume(volume)

        # Check for volume delta
        elif vol_msg.HasField("volumeDelta"):
            delta = vol_msg.volumeDelta
            logger.debug(f"Received volume delta: {delta}")
            await self.player.set_volume_delta(delta)

    async def _handle_volume_changed(self, message: Any) -> None:
        """
        Handle SRVR_CTRL_VOLUME_CHANGED (type 87).

        Observation only — never re-applies the volume. This is a
        Controller-facing broadcast ("some renderer's volume is now X",
        for a UI to display), not a command; the actual command a
        Renderer must act on is SRVR_RNDR_SET_VOLUME (type 42), handled
        above. This proxy's own Controller join is deliberately
        listen-only (see ws_manager.py's _send_ctrl_join_session).

        Regression (2026-09-13, live-observed "volume knob jittering"):
        this used to call player.set_volume(volume) unconditionally for
        *any* rendererId — including our own. Since this proxy joins
        Renderer and Controller on the same socket, every one of our own
        outbound RNDR_SRVR_VOLUME_CHANGED reports gets broadcast straight
        back to us on the Controller channel as this exact message,
        carrying our own rendererId. Re-applying it as if it were a
        fresh command turned our own report into another volume change,
        which produced another report, which came back again — an
        unbounded loop in principle, visibly bounded in practice only by
        the same-value no-op guard and coalescing (see set_volume()'s own
        comments) once two real, adjacent slider values were in flight:
        confirmed directly in a capture as a repeating A/B oscillation
        (e.g. 13/14, later 23/27/31) between recently-seen real values,
        each one actually reaching the physical device before the next
        overwrote it — exactly the "knob jumping back and forth"
        symptom, and structurally the same shape as the original
        SET_VOLUME echo/flood bug this proxy has already fixed once
        under a different cause (see this module's sibling history and
        docs/gapless-arming-and-token-expiry.md-adjacent volume notes).
        """
        if not message.HasField("srvrCtrlVolumeChanged"):
            return

        vol_msg = message.srvrCtrlVolumeChanged
        if vol_msg.HasField("volume"):
            logger.debug(
                f"Observed SRVR_CTRL_VOLUME_CHANGED (rendererId={vol_msg.rendererId}, "
                f"volume={vol_msg.volume}) — not applied, see this handler's own docstring"
            )

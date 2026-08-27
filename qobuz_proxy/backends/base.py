"""
Abstract audio backend interface.

Defines the contract that all audio backends must implement.
"""

import logging
from abc import ABC, abstractmethod
from typing import Callable, Optional

from .types import (
    BackendInfo,
    BackendTrackMetadata,
    BufferStatus,
    PlaybackState,
)

logger = logging.getLogger(__name__)

# Event callback types
StateChangeCallback = Callable[[PlaybackState], None]
PositionUpdateCallback = Callable[[int], None]  # position_ms
BufferStatusCallback = Callable[[BufferStatus], None]
TrackEndedCallback = Callable[[], None]
PlaybackErrorCallback = Callable[[str], None]  # error_message
NextTrackStartedCallback = Callable[[], None]
ExternalTakeoverCallback = Callable[[], None]


class AudioBackend(ABC):
    """
    Abstract base class for audio output backends.

    Backends must implement all abstract methods. Backends may optionally
    override the default implementations of lifecycle and event methods.

    Two primary backend types are supported:
    - URL-streaming: Backend handles URL (DLNA - passes URL to renderer)
    - Sample-feeding: Backend receives audio samples (local audio - future)

    Phase 1 only implements URL-streaming for DLNA.
    """

    def __init__(self, name: str = "AudioBackend"):
        """Initialize backend."""
        self.name = name
        self._volume: int = 50  # 0-100
        self._state: PlaybackState = PlaybackState.STOPPED
        self._is_connected: bool = False
        # Whether the Qobuz server currently considers this backend's
        # renderer the active playback target — see set_active(). Defaults
        # True: most backends (a manually configured single speaker, local
        # output) never get told otherwise, and should keep behaving as
        # they always have.
        self._active: bool = True

        # Event callbacks
        self._on_state_change: Optional[StateChangeCallback] = None
        self._on_position_update: Optional[PositionUpdateCallback] = None
        self._on_buffer_status: Optional[BufferStatusCallback] = None
        self._on_track_ended: Optional[TrackEndedCallback] = None
        self._on_playback_error: Optional[PlaybackErrorCallback] = None
        self._on_next_track_started: Optional[NextTrackStartedCallback] = None
        self._on_external_takeover: Optional[ExternalTakeoverCallback] = None

    # =========================================================================
    # Playback Control - Required
    # =========================================================================

    @abstractmethod
    async def play(self, url: str, metadata: BackendTrackMetadata) -> None:
        """Start playback of a track."""
        pass

    @abstractmethod
    async def pause(self) -> None:
        """Pause current playback."""
        pass

    @abstractmethod
    async def resume(self) -> bool:
        """Resume paused playback.

        Returns:
            True if the backend resumed, False if it could not (e.g. the
            renderer rejected the command).
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop playback completely."""
        pass

    # =========================================================================
    # Position Control - Required
    # =========================================================================

    @abstractmethod
    async def seek(self, position_ms: int) -> None:
        """Seek to position in current track."""
        pass

    @abstractmethod
    async def get_position(self) -> int:
        """Get current playback position in milliseconds."""
        pass

    # =========================================================================
    # Volume Control - Required
    # =========================================================================

    @abstractmethod
    async def set_volume(self, level: int) -> None:
        """Set playback volume (0-100)."""
        pass

    @abstractmethod
    async def get_volume(self) -> int:
        """Get current volume level (0-100)."""
        pass

    async def set_volume_delta(self, delta: int) -> int:
        """Adjust volume relatively."""
        current = await self.get_volume()
        new_level = max(0, min(100, current + delta))
        await self.set_volume(new_level)
        return new_level

    # =========================================================================
    # State - Required
    # =========================================================================

    @abstractmethod
    async def get_state(self) -> PlaybackState:
        """Get current playback state."""
        pass

    async def get_buffer_status(self) -> BufferStatus:
        """Get buffer status. Default returns OK."""
        return BufferStatus.OK

    async def is_playing_our_content(self) -> bool:
        """
        Whether the device is actually still playing what we told it to,
        as opposed to something else having taken it over — e.g. another
        app grouping into or playing directly to a shared DLNA/Sonos
        renderer. get_state() alone can't tell the two apart: the device
        reports PLAYING either way. False means the player should treat
        this exactly like an external stop.

        Default True: a backend nothing else can interfere with (e.g.
        local output, which owns its own audio stream outright) never
        needs to override this.
        """
        return True

    # =========================================================================
    # Gapless Playback - Optional
    # =========================================================================

    @property
    def supports_gapless(self) -> bool:
        """Whether this backend supports gapless playback. Default: False."""
        return False

    async def set_next_track(
        self, url: str, metadata: BackendTrackMetadata, queue_item_id: int = 0
    ) -> bool:
        """Prepare next track for gapless transition. Default: returns False."""
        return False

    async def clear_next_track(self) -> None:
        """Cancel prepared next track. Default: no-op."""
        pass

    def on_next_track_started(self, callback: Optional[NextTrackStartedCallback]) -> None:
        """Register callback for gapless transition events."""
        self._on_next_track_started = callback

    # =========================================================================
    # Lifecycle - Required
    # =========================================================================

    @abstractmethod
    async def connect(self) -> bool:
        """Initialize connection to backend. Returns True if successful."""
        pass

    @abstractmethod
    async def disconnect(self, send_device_stop: bool = True) -> None:
        """
        Disconnect and clean up backend resources.

        Args:
            send_device_stop: Whether to send the underlying device an
                explicit stop command first. Set False when the device
                itself isn't actually going anywhere and shouldn't be
                interrupted — e.g. it's already being driven by something
                else and a stop here would just interrupt that.
        """
        pass

    def is_connected(self) -> bool:
        """Check if backend is connected."""
        return self._is_connected

    def set_active(self, active: bool) -> None:
        """Record whether the Qobuz server currently considers this
        renderer the active playback target (see Player.set_active_renderer
        / SrvrRndrSetActive) — the narrow signal in the opposite direction
        from set_backend_attached()'s Speaker->Player one, letting a
        backend that watches the physical device on its own (see
        DLNABackend's hijack detection) know when doing so is actually
        meaningful.

        In a Sonos auto-discovery household every discovered room gets its
        own Speaker/Player/backend, polling continuously, whether or not
        it's the one Qobuz is actually driving right now — a mismatch
        between what an *inactive* renderer reports and what we last set
        isn't evidence of anything (no session is being driven there; it
        may simply be someone using that room directly, or nothing may
        have ever played there at all). Default: no-op. Backends without a
        notion of watching a device they don't control (local output) or
        that never receive this signal at all (a manually configured
        single speaker) need not override it — see the True default on
        self._active.
        """
        self._active = active

    async def retarget(self, ip: str, port: int, description_url: Optional[str] = None) -> bool:
        """
        Repoint this backend at a different physical device, in place —
        without dropping whatever session (e.g. a Qobuz Connect join) is
        built on top of it. Used when the renderer actually driving
        playback changes without the app-level session changing (e.g. a
        DHCP address change, or a group coordinator handoff on a backend
        that groups renderers together).

        Default: not supported, returns False. Backends without a
        meaningful notion of "the same session, different device" (e.g.
        local audio output) never need to override this.

        Returns:
            True if the retarget succeeded.
        """
        return False

    # =========================================================================
    # Event Callbacks
    # =========================================================================

    def on_state_change(self, callback: Optional[StateChangeCallback]) -> None:
        """Register callback for state changes."""
        self._on_state_change = callback

    def on_position_update(self, callback: Optional[PositionUpdateCallback]) -> None:
        """Register callback for position updates."""
        self._on_position_update = callback

    def on_buffer_status(self, callback: Optional[BufferStatusCallback]) -> None:
        """Register callback for buffer status changes."""
        self._on_buffer_status = callback

    def on_track_ended(self, callback: Optional[TrackEndedCallback]) -> None:
        """Register callback for natural track end (not stop command)."""
        self._on_track_ended = callback

    def on_playback_error(self, callback: Optional[PlaybackErrorCallback]) -> None:
        """Register callback for playback errors."""
        self._on_playback_error = callback

    def on_external_takeover(self, callback: Optional[ExternalTakeoverCallback]) -> None:
        """Register callback for an external takeover of this renderer —
        another source now driving it instead of us (see
        is_playing_our_content()). Default: never fired — a backend
        without a way to be shared out from under us (e.g. local audio
        output) never calls _notify_external_takeover()."""
        self._on_external_takeover = callback

    # =========================================================================
    # Event Notification Helpers
    # =========================================================================

    def _notify_state_change(self, state: PlaybackState) -> None:
        """Notify listeners of state change."""
        old_state = self._state
        self._state = state
        if old_state != state and self._on_state_change:
            try:
                self._on_state_change(state)
            except Exception as e:
                logger.error(f"State change callback error: {e}")

    def _notify_position_update(self, position_ms: int) -> None:
        """Notify listeners of position update."""
        if self._on_position_update:
            try:
                self._on_position_update(position_ms)
            except Exception as e:
                logger.error(f"Position update callback error: {e}")

    def _notify_buffer_status(self, status: BufferStatus) -> None:
        """Notify listeners of buffer status change."""
        if self._on_buffer_status:
            try:
                self._on_buffer_status(status)
            except Exception as e:
                logger.error(f"Buffer status callback error: {e}")

    def _notify_track_ended(self) -> None:
        """Notify listeners that track ended naturally."""
        if self._on_track_ended:
            try:
                self._on_track_ended()
            except Exception as e:
                logger.error(f"Track ended callback error: {e}")

    def _notify_playback_error(self, message: str) -> None:
        """Notify listeners of playback error."""
        if self._on_playback_error:
            try:
                self._on_playback_error(message)
            except Exception as e:
                logger.error(f"Playback error callback error: {e}")

    def _notify_next_track_started(self) -> None:
        """Notify listeners that a gapless transition to the next track occurred."""
        if self._on_next_track_started:
            try:
                self._on_next_track_started()
            except Exception as e:
                logger.error(f"Next track started callback error: {e}")

    def _notify_external_takeover(self) -> None:
        """Notify listeners that another source has taken over this renderer."""
        if self._on_external_takeover:
            try:
                self._on_external_takeover()
            except Exception as e:
                logger.error(f"External takeover callback error: {e}")

    # =========================================================================
    # Info
    # =========================================================================

    def get_info(self) -> BackendInfo:
        """Get information about this backend."""
        return BackendInfo(
            backend_type="unknown",
            name=self.name,
            device_id="",
        )

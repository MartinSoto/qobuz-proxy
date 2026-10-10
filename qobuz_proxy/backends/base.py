"""
Abstract audio backend interface.

Defines the contract that all audio backends must implement.
"""

import logging
from abc import ABC, abstractmethod
from typing import Callable, Optional

from .types import (
    AdoptedPlayback,
    BackendInfo,
    BackendTrackMetadata,
    BufferStatus,
    PlaybackState,
    PlayResult,
)

logger = logging.getLogger(__name__)

# Event callback types
StateChangeCallback = Callable[[PlaybackState], None]
PositionUpdateCallback = Callable[[int], None]  # position_ms
BufferStatusCallback = Callable[[BufferStatus], None]
TrackEndedCallback = Callable[[], None]
PlaybackErrorCallback = Callable[[str], None]  # error_message
NextTrackStartedCallback = Callable[[], None]
# The device moved on to another queue entry by itself: (track id, queue item id,
# what was resolved for it, if known). See AudioBackend.on_item_changed.
ItemChangedCallback = Callable[[str, int, Optional[PlayResult]], None]
ExternalTakeoverCallback = Callable[[], None]
VolumeUpdateCallback = Callable[[int], None]  # volume (0-100)


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
        self._on_item_changed: Optional[ItemChangedCallback] = None
        self._on_external_takeover: Optional[ExternalTakeoverCallback] = None
        self._on_volume_update: Optional[VolumeUpdateCallback] = None

    # =========================================================================
    # Playback Control - Required
    # =========================================================================

    @abstractmethod
    async def play(self, metadata: BackendTrackMetadata) -> PlayResult:
        """Start playback of a track.

        The backend resolves its own streaming URL (via a shared
        QobuzStreamResolver) rather than being handed one — the format/
        quality decision (native passthrough vs. local downsampling for
        DLNA, a fixed quality for local output) is backend-specific
        knowledge the caller shouldn't need. Returns the blob/format_id
        Qobuz actually served, for the caller's play-reporting.
        """
        pass

    @abstractmethod
    async def pause(self) -> bool:
        """Pause current playback.

        Returns:
            True if the backend paused, False if it could not (e.g. the
            renderer rejected the command, or was already stopped —
            observed directly: a device that had already finished
            playing on its own rejects Pause with a UPnP "transition not
            available" error; the caller must not report a successful
            pause over that).
        """
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
        self, metadata: BackendTrackMetadata, queue_item_id: int = 0
    ) -> Optional[PlayResult]:
        """Prepare next track for gapless transition. Default: returns None
        (unsupported). A non-None result means the track was armed; its
        blob/format_id are surfaced by the caller once the gapless
        transition actually happens (see AudioBackend.on_next_track_started).
        """
        return None

    async def clear_next_track(self) -> None:
        """Cancel prepared next track. Default: no-op."""
        pass

    def on_next_track_started(self, callback: Optional[NextTrackStartedCallback]) -> None:
        """Register callback for gapless transition events."""
        self._on_next_track_started = callback

    # =========================================================================
    # Queue handover - Optional
    # =========================================================================

    def on_item_changed(self, callback: Optional[ItemChangedCallback]) -> None:
        """Register callback for the device advancing to another entry of the
        queue it holds, on its own (a natural transition, a skipped entry, a
        wrap-around). Default: never fired — only a backend that hands the
        whole queue to the device (see SonosQueueBackend) has anything to
        report; every other backend sequences the queue itself."""
        self._on_item_changed = callback

    async def set_repeat_mode(self, mode: str) -> None:
        """The repeat mode changed: "off", "one" or "all". A backend that
        lets the device run through the queue on its own relays it to the
        device (SonosQueueBackend); every other backend sequences the queue
        itself, so the default does nothing."""
        return None

    async def playing_entry(self) -> Optional[AdoptedPlayback]:
        """The queue entry of ours the device is already playing or has paused,
        if it can tell (a backend that holds the queue on the device). Default:
        None."""
        return None

    async def adopt(self, playing: AdoptedPlayback, duration_ms: int) -> None:
        """Carry on from `playing` instead of starting anew: take it as the
        current entry and learn what the device queue holds. Default: nothing."""
        return None

    async def next_pointer_changed(self, info: Optional[dict]) -> None:
        """The server's next pointer (a dict with trackId, queueItemId, ...,
        or None for no next track) is now this. A backend that holds the whole
        queue on the device needs it for the one case the queue does not
        cover: the pointer leaving the queue into the autoplay list. Default:
        nothing to do."""
        return None

    async def queue_changed(self) -> None:
        """The Qobuz queue's order or contents changed within the queue
        generation the backend was loaded with (an edit, a shuffle toggle).
        A backend that holds the queue on the device brings it up to date;
        default: nothing to do."""
        return None

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

    async def set_active(self, active: bool) -> None:
        """Record whether the Qobuz server currently considers this
        renderer the active playback target (see Player.set_active_renderer
        / SrvrRndrSetActive) — the narrow signal in the opposite direction
        from set_backend_attached()'s Speaker->Player one, letting a
        backend that watches the physical device on its own (see
        DLNABackend's hijack detection) know when doing so is actually
        meaningful.

        In a Sonos auto-discovery household every discovered room gets its
        own Speaker/Player/backend — DLNABackend overrides this to actually
        stop polling the physical device while inactive (see there), not
        merely record the flag for one check to consult; a backend without
        a notion of watching a device it doesn't control (local output) or
        that never receives this signal at all (a manually configured
        single speaker) can just keep this default no-op — see the True
        default on self._active. Async so an overriding backend can
        actually wait for its poll loop to stop before this returns.
        """
        self._active = active

    def set_quality_override(self, format_id: Optional[int]) -> None:
        """Force a specific Qobuz quality tier for every subsequent play(),
        or None to let the backend decide dynamically (DLNA: resolve the
        actual per-track native format against device capabilities each
        time — see DLNABackend/resolve_track). Set from config's
        max_quality (when not "auto") and from a live quality-change
        request from the Qobuz app (Speaker._on_quality_change). Default:
        no-op — a backend with no such notion (none exist without
        overriding this) simply ignores it.
        """
        pass

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

    def on_volume_update(self, callback: Optional[VolumeUpdateCallback]) -> None:
        """Register callback for a volume change this backend didn't
        itself just cause — the physical device's own controls, the
        Sonos app, another DLNA controller, etc. Default: never fired —
        a backend with no way to detect this (e.g. local audio output,
        which owns the volume outright) never calls
        _notify_volume_update()."""
        self._on_volume_update = callback

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

    def _notify_item_changed(
        self, track_id: str, queue_item_id: int, result: Optional[PlayResult] = None
    ) -> None:
        """Notify listeners that the device moved on to another queue entry."""
        if self._on_item_changed:
            try:
                self._on_item_changed(track_id, queue_item_id, result)
            except Exception as e:
                logger.error(f"Item changed callback error: {e}")

    def _notify_external_takeover(self) -> None:
        """Notify listeners that another source has taken over this renderer."""
        if self._on_external_takeover:
            try:
                self._on_external_takeover()
            except Exception as e:
                logger.error(f"External takeover callback error: {e}")

    def _notify_volume_update(self, volume: int) -> None:
        """Notify listeners of a volume change this backend didn't itself
        just cause."""
        if self._on_volume_update:
            try:
                self._on_volume_update(volume)
            except Exception as e:
                logger.error(f"Volume update callback error: {e}")

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

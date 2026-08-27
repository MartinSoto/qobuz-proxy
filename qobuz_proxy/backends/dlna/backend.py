"""
DLNA audio backend.

Implements AudioBackend interface for DLNA/UPnP renderers.
"""

import asyncio
import logging
import time
from typing import Optional, TYPE_CHECKING

from qobuz_proxy.backends.base import AudioBackend
from qobuz_proxy.backends.types import (
    BackendInfo,
    BackendTrackMetadata,
    BufferStatus,
    PlaybackState,
)
from .client import DLNAClient, DLNAClientError, SoapResult
from .capabilities import (
    DLNACapabilities,
    CapabilityCache,
    parse_protocol_info_sink,
    apply_device_overrides,
    build_protocol_info,
)

if TYPE_CHECKING:
    from .proxy_server import AudioProxyServer

logger = logging.getLogger(__name__)

# State polling interval. Also the cadence of hijack detection and paused-
# external-stop confirmation (see _HIJACK_CHECK_INTERVAL_POLLS and
# _PAUSED_STOP_CONFIRMATIONS below) — this loop is the only thing that ever
# reads transport state/position from the physical device, so it runs at
# the faster of the two cadences that used to poll independently rather
# than the slower one, to avoid regressing external-takeover/stop-detection
# latency.
STATE_POLL_INTERVAL_SECONDS = 0.5

# While playing, how many poll cycles between hijack checks (is another
# source now playing to this renderer instead of us — see
# is_playing_our_content()). Costs an extra device round trip, unlike the
# state/position poll every cycle already does, so this is throttled
# independently — detection within a few seconds is plenty.
_HIJACK_CHECK_INTERVAL_POLLS = 6

# While paused, a confirmed external stop (the renderer stopped on its own,
# e.g. someone else's timeout/command) is reported after this many
# consecutive STOPPED reads — get_state() collapses transient read
# failures (and unrecognized device state strings) to STOPPED, so one bad
# poll must not end a normal paused listen or lose its resume position.
_PAUSED_STOP_CONFIRMATIONS = 3

# Grace period during which transport state is expected to be transiently
# unreliable (seconds) — covers both a track just starting (prevents false
# track-ended events while the device is loading) and a coordinator
# retarget just having happened (see retarget()): a Sonos handoff, and any
# membership change that follows moments later as part of the same user
# action, can leave the new coordinator's own reported state/TrackURI
# briefly wrong while Sonos settles internally — without this, that reads
# as an external takeover and forces an unnecessary WebSocket reconnect
# (observed directly: a hijack false-positive ~3s after a clean retarget,
# coincident with a departing member being stopped).
PLAYBACK_START_GRACE_PERIOD_SECONDS = 5.0

# How long a retarget waits to actually see our content playing on the new
# coordinator before giving up and rejoining normal hijack detection
# (seconds) — see retarget()'s _awaiting_retarget_confirmation. A Sonos
# room-move ("move currently playing audio to another room") is not an
# atomic handoff: it's two separate topology edits (add destination to the
# group, then remove the source), and the *source* room can audibly stop
# well over ten seconds before the destination's topology entry — let alone
# its actual playback — catches up (observed directly). PLAYBACK_START_
# GRACE_PERIOD_SECONDS's fixed 5s window is nowhere near enough for that;
# this waits for the actual signal (the new coordinator reporting our
# proxy URL) instead of guessing a duration, bounded only as a safety net
# against a handoff that genuinely never completes.
RETARGET_CONFIRMATION_TIMEOUT_SECONDS = 30.0

# How long play() waits for a detached backend to reconnect before giving
# up (seconds). A Sonos group_id going pending (see SonosDiscoveryManager)
# detaches this backend for what's normally a few seconds while the
# topology resolves (see Speaker.detach()) — a play() call landing in that
# window (observed directly: a track's natural end auto-advancing to the
# next one, racing a detach that happened the same moment) must not just
# raise; the caller has no way to retry it once the backend reconnects.
# Comfortably inside PENDING_GRACE_SECONDS so a genuine loss still fails
# in reasonable time instead of hanging out the full pending window.
RECONNECT_WAIT_SECONDS = 8.0
_RECONNECT_POLL_INTERVAL_SECONDS = 0.2

# Class-level capability cache (shared across instances)
_capability_cache = CapabilityCache()


class DLNABackend(AudioBackend):
    """
    DLNA/UPnP audio backend.

    Connects to DLNA renderers and controls playback via SOAP commands.
    Uses polling to monitor device state.

    Note: This backend expects URLs to be provided by the Audio Proxy Server.
    It does not handle URL proxying itself.

    Subclassed by sonos.backend.SonosBackend for Sonos's queue-based
    playback — this class itself only ever speaks standard DLNA
    (SetAVTransportURI/SetNextAVTransportURI). _client_class is the seam
    a subclass overrides to get its own DLNAClient subclass constructed by
    connect()/retarget() without either needing to know about the other.
    """

    _client_class: type[DLNAClient] = DLNAClient

    def __init__(
        self,
        ip: str,
        port: int = 1400,
        fixed_volume: bool = False,
        name: Optional[str] = None,
        description_url: Optional[str] = None,
        hires_downsampling: bool = False,
    ):
        """
        Initialize DLNA backend.

        Args:
            ip: DLNA device IP address
            port: DLNA device port (default 1400 for Sonos)
            fixed_volume: If True, ignore volume commands
            name: Display name (auto-detected if not provided)
            description_url: Full URL to UPnP device description XML
            hires_downsampling: Experimental, opt-in. When True, a device
                with real 24-bit support (see DLNACapabilities.max_quality)
                gets Hi-Res requested from Qobuz even below its own 96k
                tier, and any track exceeding its actual sample-rate cap is
                downsampled on the fly instead of failing or getting stuck
                at CD quality (see _transcode_sample_rate_for). False (the
                default) keeps the old, conservative behavior: nothing is
                ever transcoded.
        """
        super().__init__(name or f"DLNA ({ip})")
        self._ip = ip
        self._port = port
        self._fixed_volume = fixed_volume
        self._description_url = description_url
        self._hires_downsampling = hires_downsampling

        self._client: Optional[DLNAClient] = None
        self._poll_task: Optional[asyncio.Task] = None

        self._current_metadata: Optional[BackendTrackMetadata] = None
        self._position_ms: int = 0
        self._duration_ms: int = 0

        # Audio proxy server for URL handling
        self._proxy_server: Optional["AudioProxyServer"] = None

        # Device capabilities
        self._capabilities: Optional[DLNACapabilities] = None

        # Start of the current grace period (see PLAYBACK_START_GRACE_PERIOD_SECONDS)
        # — reset both when playback starts and on a successful retarget().
        self._playback_started_at: float = 0.0

        # Gapless playback state
        self._next_track_proxy_url: Optional[str] = None
        self._next_track_metadata: Optional[BackendTrackMetadata] = None
        # Only ever set by SonosBackend's queue-based _arm_next_track — a
        # generic renderer's SetNextAVTransportURI has no queue position.
        self._next_track_queue_nr: Optional[int] = None
        self._gapless_supported: bool = True
        self._current_proxy_url: Optional[str] = None

        # Consecutive STOPPED polls seen while paused (external-stop
        # detection — see _PAUSED_STOP_CONFIRMATIONS).
        self._paused_stop_polls: int = 0

        # Cycles since the last hijack check (external-takeover detection
        # while PLAYING — see _HIJACK_CHECK_INTERVAL_POLLS).
        self._hijack_check_countdown: int = 0

        # Whether an ongoing external takeover has already been notified —
        # see _poll_state_loop. Prevents re-notifying every single poll
        # cycle for as long as the takeover persists (while something's
        # armed for gapless, the read that also feeds this check runs
        # unthrottled) — Player only needs to hear about a takeover once
        # to react to it, not once per 0.5s.
        self._external_takeover_notified: bool = False

        # Set by a successful retarget(), cleared once the new coordinator
        # actually reports playing our content — see
        # RETARGET_CONFIRMATION_TIMEOUT_SECONDS. While True, _poll_state_loop
        # treats this exactly like the ordinary grace period (no hijack
        # detection, no STOPPED-triggered track-ended/paused-stop
        # confirmation): none of the new coordinator's reported state can be
        # trusted as a real signal until this resolves one way or the other.
        self._awaiting_retarget_confirmation: bool = False
        self._retarget_confirmation_deadline: float = 0.0

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def set_proxy_server(self, proxy: "AudioProxyServer") -> None:
        """
        Set the audio proxy server for URL proxying.

        When set, all playback URLs will be routed through the proxy,
        which handles Qobuz URL expiration transparently.

        Args:
            proxy: AudioProxyServer instance
        """
        self._proxy_server = proxy
        logger.info("Audio proxy server configured for DLNA backend")

    async def connect(self) -> bool:
        """Connect to DLNA device."""
        try:
            self._client = self._client_class(
                self._ip, self._port, description_url=self._description_url
            )
            device_info = await self._client.connect()

            # Update name from device
            if device_info.friendly_name:
                self.name = device_info.friendly_name

            # Query device capabilities
            await self._discover_capabilities(device_info)

            self._is_connected = True

            # Start state polling
            self._poll_task = asyncio.create_task(self._poll_state_loop())

            logger.info(f"Connected to DLNA device: {self.name}")
            return True

        except DLNAClientError as e:
            logger.error(f"Failed to connect to DLNA device: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to DLNA: {e}", exc_info=True)
            return False

    async def retarget(self, ip: str, port: int, description_url: Optional[str] = None) -> bool:
        """
        Repoint this backend at a different DLNA renderer, in place —
        without dropping whatever session this backend is part of. Used
        for a group-coordinator handoff or a changed device address: the
        underlying audio source doesn't need to restart, only where
        *future* commands go and where state gets polled from.

        Also used to reconnect a backend that was previously detached (see
        detach()) — self._client is None in that case, so there's no old
        connection to release, just a fresh one to establish and the poll
        loop to restart.

        On failure, keeps talking to the previous target — the caller
        should retry.

        Returns:
            True if the new target connected successfully.
        """
        if ip == self._ip and port == self._port and self._is_connected:
            return True

        new_client = self._client_class(ip, port, description_url=description_url)
        try:
            device_info = await new_client.connect()
        except Exception as e:
            logger.warning(
                f"Retarget to {ip}:{port} failed, staying on {self._ip}:{self._port}: {e}"
            )
            try:
                await new_client.disconnect()
            except Exception:
                pass
            return False

        old_client = self._client

        # Gapless state referenced the *old* device's own queue — invalid
        # on a different physical player.
        self._next_track_proxy_url = None
        self._next_track_metadata = None
        self._next_track_queue_nr = None
        self._gapless_supported = True
        self._external_takeover_notified = False

        self._ip = ip
        self._port = port
        self._description_url = description_url
        self._client = new_client
        self._is_connected = True
        # New coordinator's own reported state/TrackURI can be transiently
        # wrong while Sonos settles internally — see
        # PLAYBACK_START_GRACE_PERIOD_SECONDS and is_playing_our_content().
        self._playback_started_at = time.monotonic()
        # A room-move handoff can take much longer than that fixed window
        # to actually converge — see RETARGET_CONFIRMATION_TIMEOUT_SECONDS.
        # Only meaningful if we actually had content playing before this
        # retarget; retargeting a backend that was never playing anything
        # (e.g. reconnecting after a pending resolve with nothing loaded
        # yet) has nothing to wait to see confirmed.
        if self._current_proxy_url is not None:
            self._awaiting_retarget_confirmation = True
            self._retarget_confirmation_deadline = (
                time.monotonic() + RETARGET_CONFIRMATION_TIMEOUT_SECONDS
            )
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_state_loop())
        if device_info.friendly_name:
            self.name = device_info.friendly_name

        # Capabilities can differ between devices (e.g. a different model) —
        # re-detect rather than carry the old target's forward.
        self._capabilities = None
        await self._discover_capabilities(device_info)

        if old_client is not None:
            # Just release the connection here — whether the old device
            # should also be told to stop is a decision this backend has
            # no visibility into (that depends on state above it, e.g.
            # whether it's still the one actually being driven).
            try:
                await old_client.disconnect()
            except Exception:
                # Anything still in flight on it just fails — the same
                # class of transient error existing retry/error-handling
                # already tolerates, and it no longer matters: we're not
                # talking to that device anymore.
                pass

        logger.info(f"Retargeted DLNA backend to {self.name} ({ip}:{port})")
        return True

    async def _discover_capabilities(self, device_info) -> None:
        """Query and parse device capabilities."""
        # Check cache first
        device_id = device_info.udn or self._ip
        cached = _capability_cache.get(device_id)
        if cached:
            logger.debug(f"Using cached capabilities for {device_id}")
            self._capabilities = cached
            return

        # Query GetProtocolInfo
        try:
            if not self._client:
                return
            sink = await self._client.get_protocol_info()
            if sink:
                self._capabilities = parse_protocol_info_sink(sink)
                # Apply device-specific overrides
                apply_device_overrides(
                    self._capabilities,
                    device_info.manufacturer,
                    device_info.model_name,
                    hires_downsampling=self._hires_downsampling,
                )
                # Cache the result
                _capability_cache.set(device_id, self._capabilities)
            else:
                logger.debug("GetProtocolInfo not supported, using defaults")
                self._capabilities = None
        except Exception as e:
            logger.warning(f"Failed to discover capabilities: {e}")
            self._capabilities = None

    async def disconnect(self, send_device_stop: bool = True) -> None:
        """Disconnect from DLNA device.

        Args:
            send_device_stop: If False, skip the live Stop command — use
                this when the device isn't actually being stopped, only
                given up on (e.g. it's already being driven by something
                else and a Stop sent here would just interrupt that).
        """
        self._is_connected = False

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._client:
            if send_device_stop:
                try:
                    await self._client.stop()
                except Exception:
                    pass
            await self._client.disconnect()
            self._client = None

        logger.info(f"Disconnected from DLNA device: {self.name}")

    # =========================================================================
    # Playback Control
    # =========================================================================

    async def _wait_for_reconnect(self, timeout: float = RECONNECT_WAIT_SECONDS) -> bool:
        """Wait briefly for self._client to reappear — see
        RECONNECT_WAIT_SECONDS. Returns False (immediately, no wait) if
        this backend was never detached in the first place."""
        if self._client:
            return True
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(_RECONNECT_POLL_INTERVAL_SECONDS)
            if self._client:
                return True
        return False

    async def play(self, url: str, metadata: BackendTrackMetadata) -> None:
        """Start playback of track."""
        if not self._client and not await self._wait_for_reconnect():
            raise RuntimeError("Not connected")

        # Clear gapless state — explicit play invalidates armed next track
        # (no queue removal needed: Sonos play clears the whole queue)
        self._next_track_proxy_url = None
        self._next_track_metadata = None
        self._next_track_queue_nr = None

        self._current_metadata = metadata
        self._duration_ms = metadata.duration_ms

        content_type, transcode_rate = self._resolve_content_type_and_transcode(url, metadata)

        # Register with proxy server if available
        actual_url = url
        if self._proxy_server:
            actual_url = self._proxy_server.register_track(
                track_id=metadata.track_id,
                qobuz_url=url,
                content_type=content_type,
                transcode_to_sample_rate=transcode_rate,
            )
            logger.debug(f"Using proxy URL: {actual_url}")

        # Build DIDL-Lite metadata
        didl = self._build_didl(actual_url, metadata, content_type)

        success = await self._start_transport(actual_url, didl)

        if success:
            self._position_ms = 0
            self._current_proxy_url = actual_url
            self._playback_started_at = time.monotonic()
            self._notify_state_change(PlaybackState.PLAYING)
            logger.info(f"Playing: {metadata.artist} - {metadata.title}")

    async def _start_transport(self, url: str, didl: str) -> bool:
        """Actually start playback of an already-registered/DIDL-built URL.

        Standard DLNA: SetAVTransportURI + Play. SonosBackend overrides
        this to use its AVTransport queue instead (see _play_via_queue).
        """
        return await self._play_via_transport(url, didl)

    async def _play_via_transport(self, url: str, didl: str) -> bool:
        """Start playback using SetAVTransportURI (standard DLNA).

        If the first attempt fails (e.g. the renderer is wedged after a rapid
        track switch), recover the connection and reset the device transport,
        then retry once before reporting an error. This lets a stuck renderer
        recover without restarting the addon or power-cycling the device.
        """
        assert self._client
        stage = await self._try_transport_sequence(url, didl)
        if stage is None:
            return True

        logger.warning(f"Playback {stage} failed; attempting transport recovery")
        # Recover the HTTP session and clear the device's stuck transport.
        await self._client.reset_session()
        try:
            await self._client.stop()
        except Exception:
            pass

        stage = await self._try_transport_sequence(url, didl)
        if stage is None:
            logger.info("Transport recovery succeeded")
            return True

        if stage == "play":
            self._notify_playback_error("Failed to start playback")
        else:
            self._notify_playback_error("Failed to set transport URI")
        return False

    async def _try_transport_sequence(self, url: str, didl: str) -> Optional[str]:
        """Run SetAVTransportURI + Play once.

        Returns None on success, or the name of the failed stage
        ("set_uri" or "play").
        """
        assert self._client
        if not await self._client.set_av_transport_uri(url, didl):
            return "set_uri"
        if not await self._client.play():
            return "play"
        return None

    async def pause(self) -> None:
        """Pause playback."""
        if self._client and await self._client.pause():
            self._notify_state_change(PlaybackState.PAUSED)

    async def resume(self) -> bool:
        """Resume playback.

        Returns True only when the renderer accepted the play command — a
        failed SOAP call must not be reported as a successful resume.
        """
        if self._client and await self._client.play():
            self._notify_state_change(PlaybackState.PLAYING)
            return True
        return False

    async def stop(self) -> None:
        """Stop playback."""
        # Clear gapless state
        self._next_track_proxy_url = None
        self._next_track_metadata = None
        self._next_track_queue_nr = None

        if self._client and await self._client.stop():
            self._position_ms = 0
            self._playback_started_at = 0.0  # Clear grace period
            self._notify_state_change(PlaybackState.STOPPED)

    # =========================================================================
    # Position Control
    # =========================================================================

    async def seek(self, position_ms: int) -> None:
        """Seek to position."""
        if self._client and await self._client.seek(position_ms):
            self._position_ms = position_ms
            self._notify_position_update(position_ms)

    async def get_position(self) -> int:
        """Get current position."""
        if self._client:
            pos = await self._client.get_position_info()
            if pos is not None:
                self._position_ms = pos
                logger.debug(f"DLNA position: {pos}ms")
            else:
                logger.debug("DLNA position: None returned")
        return self._position_ms

    # =========================================================================
    # Volume Control
    # =========================================================================

    async def set_volume(self, level: int) -> None:
        """Set volume (0-100)."""
        if self._fixed_volume:
            logger.debug("Fixed volume mode: ignoring set_volume")
            return

        clamped = max(0, min(100, level))
        if self._client:
            await self._client.set_volume(clamped)
            self._volume = clamped

    async def get_volume(self) -> int:
        """Get current volume (0-100)."""
        if self._fixed_volume:
            return 100

        if self._client:
            vol = await self._client.get_volume()
            if vol is not None:
                self._volume = vol
        return self._volume

    # =========================================================================
    # State
    # =========================================================================

    async def get_state(self) -> PlaybackState:
        """Get current playback state from device."""
        if not self._client:
            return PlaybackState.STOPPED

        state_str = await self._client.get_transport_info()
        if state_str:
            if state_str == "PLAYING":
                return PlaybackState.PLAYING
            elif state_str == "PAUSED_PLAYBACK":
                return PlaybackState.PAUSED
            elif state_str == "TRANSITIONING":
                return PlaybackState.LOADING

        return PlaybackState.STOPPED

    async def get_buffer_status(self) -> BufferStatus:
        """Get buffer status (always OK for DLNA)."""
        return BufferStatus.OK

    async def is_playing_our_content(self) -> bool:
        """Compare the device's actual current track URI against the one we
        last set — a shared DLNA renderer can be handed to a completely
        different source (another app, someone grouping into it) while
        still reporting PLAYING throughout, so get_state() alone can't
        detect this."""
        if not self._client or not self._current_proxy_url or not self._active:
            return True  # nothing of ours to have been displaced yet (or Qobuz isn't driving this renderer right now)

        if time.monotonic() - self._playback_started_at < PLAYBACK_START_GRACE_PERIOD_SECONDS:
            # Track just started, or we just retargeted to a new
            # coordinator — its own reported TrackURI can be transiently
            # wrong while Sonos settles internally, and a departing
            # member's own stop shortly after a handoff can perturb it
            # too. Not a real signal either way during this window.
            return True

        return self._is_playing_our_content_given(await self._get_current_transport_uri())

    def _is_playing_our_content_given(self, current_uri: Optional[str]) -> bool:
        """The classification half of is_playing_our_content(), given a
        transport URI already read this cycle — split out so
        _poll_state_loop can reuse a single read for both gapless-
        transition detection and hijack detection when a cycle needs
        both, instead of each doing its own separate device round trip.
        Doesn't itself check self._client/self._current_proxy_url/the
        grace period — is_playing_our_content() still does, for its own
        callers; _poll_state_loop does the equivalent checks itself
        before ever reading."""
        if not current_uri:
            # None (transient read failure — no signal either way) or ""
            # (device confirms nothing is loaded at all). Neither is
            # evidence that something *else* is now driving this renderer
            # — an empty URI means it's idle, not hijacked; see
            # _device_confirms_stopped() for where that's actually turned
            # into a stop/track-ended signal instead.
            return True

        if self._is_own_proxy_url(current_uri):
            # Still being served by our own proxy — some track of ours,
            # whichever one exactly. "Hijacked" means an *external* source
            # took over; our own bookkeeping of exactly which track is
            # current can legitimately lag a real transition (a device
            # advancing to something already in its queue that we didn't
            # separately re-arm — see sonos-retarget-gapless-desync) without
            # that ever being evidence of anything external happening. The
            # narrower "is this specifically the track/next-track we
            # expect" question belongs to gapless-transition detection
            # (_poll_state_loop), which needs to know *which* track to
            # update metadata correctly — this method doesn't.
            return True

        return current_uri in (self._current_proxy_url, self._next_track_proxy_url)

    def _is_own_proxy_url(self, uri: str) -> bool:
        """Whether `uri` is served by this backend's own proxy — any track,
        not necessarily the one we currently think is playing. Each Speaker
        owns a distinct proxy host:port, so a prefix match is unambiguous.
        False (not just uncertain) when no proxy is configured — callers
        that reach here already know `uri` is non-empty and want a real
        answer, and the exact-match fallback they run next still catches
        the case where it happens to equal what we're tracking."""
        return self._proxy_server is not None and uri.startswith(self._proxy_server.base_url)

    async def _get_current_transport_uri(self) -> Optional[str]:
        """The URI this device reports as its current source — used both
        for gapless-transition detection (_poll_state_loop) and external-
        takeover detection (is_playing_our_content). Standard DLNA:
        GetMediaInfo.CurrentURI is the actual playing track URL.
        SonosBackend overrides this — Sonos queue playback's
        GetMediaInfo.CurrentURI is the *queue* URI, not the track URL."""
        assert self._client
        return await self._client.get_media_info()

    async def _device_confirms_stopped(self) -> bool:
        """Whether the device's own reported URI backs up a STOPPED
        transport-state read as a genuine stop, rather than a
        transient/mid-transition glitch — used before trusting either
        STOPPED-transition path in _poll_state_loop (natural track-ended,
        paused-stop confirmation).

        A bare "STOPPED" transport-state string isn't enough evidence on
        its own: get_state() already collapses a transient SOAP failure to
        that same value, and Sonos in particular can report STOPPED for a
        read or two while it's disturbing this device for reasons that have
        nothing to do with our own playback — e.g. another room joining or
        leaving its group (observed directly: a "track ended" fired after
        only 0.5% of the file had streamed, immediately followed by that
        same device's queue getting rebuilt out from under an in-progress
        Sonos handoff — see RETARGET_CONFIRMATION_TIMEOUT_SECONDS). If the
        device still shows *our* content loaded — any track our own proxy
        is serving, not necessarily the specific one we currently think is
        current (see _is_own_proxy_url) — a STOPPED read is almost
        certainly one of those — not evidence of anything. Only an
        empty URI, or one genuinely outside our own proxy, counts as real
        confirmation.
        """
        if not self._client or not self._current_proxy_url:
            return True  # nothing of ours was loaded to begin with
        current_uri = await self._get_current_transport_uri()
        logger.debug(
            f"[{self.name}] Polled to confirm STOPPED: device reports {current_uri!r}, "
            f"expecting {self._current_proxy_url!r}"
        )
        if current_uri is None:
            return False  # read failed — no evidence either way, not yet
        if self._is_own_proxy_url(current_uri) or current_uri in (
            self._current_proxy_url,
            self._next_track_proxy_url,
        ):
            return False  # still shows our content (some track of ours) — not really stopped
        return True  # empty, or something else entirely

    # =========================================================================
    # Info
    # =========================================================================

    def get_info(self) -> BackendInfo:
        """Get backend information."""
        info = BackendInfo(
            backend_type="dlna",
            name=self.name,
            device_id=(
                self._client.device_info.udn if self._client and self._client.device_info else ""
            ),
            ip=self._ip,
            port=self._port,
        )
        if self._client and self._client.device_info:
            info.model = self._client.device_info.model_name
            info.manufacturer = self._client.device_info.manufacturer
        return info

    # =========================================================================
    # Capabilities
    # =========================================================================

    def get_capabilities(self) -> Optional[DLNACapabilities]:
        """
        Get discovered device capabilities.

        Returns:
            DLNACapabilities if discovered, None otherwise
        """
        return self._capabilities

    def get_recommended_quality(self) -> Optional[int]:
        """
        Get recommended Qobuz quality level based on device capabilities.

        Returns:
            Qobuz quality level (5, 6, 7, or 27), or None if not available
        """
        if self._capabilities:
            return self._capabilities.max_quality
        return None

    @property
    def quality_detection_confirmed(self) -> bool:
        """Whether the recommended quality came from explicit device format info.

        False means the device advertised FLAC without stating sample rates or
        bit depths, so the recommendation is just a conservative CD default.
        """
        return self._capabilities is not None and self._capabilities.format_info_confirmed

    def _resolve_content_type_and_transcode(
        self, url: str, metadata: BackendTrackMetadata
    ) -> tuple[str, Optional[int]]:
        """The MIME type to advertise for this track, and — when its native
        format exceeds what this device can actually handle — the sample
        rate to downsample it to on the fly (see _transcode_sample_rate_for
        and TranscodingFlacReader).

        Also logs which path was taken at INFO, for every track — so it's
        always visible in the logs whether a given track streamed at
        Qobuz's own sample rate or got downsampled, not just the latter.
        """
        content_type = "audio/flac"
        if ".mp3" in url.lower() or "format=5" in url.lower():
            content_type = "audio/mpeg"

        transcode_rate = self._transcode_sample_rate_for(metadata)
        if transcode_rate is not None:
            content_type = "audio/wav"
            logger.info(
                f"Track {metadata.track_id}: downsampling {metadata.sample_rate}Hz "
                f"({metadata.bit_depth}-bit, as served by Qobuz) -> {transcode_rate}Hz "
                f"— exceeds this device's cap"
            )
        elif metadata.sample_rate:
            logger.info(
                f"Track {metadata.track_id}: keeping Qobuz's "
                f"{metadata.sample_rate}Hz/{metadata.bit_depth}-bit stream as-is"
            )
        else:
            logger.info(f"Track {metadata.track_id}: streaming as-is (quality info unavailable)")
        return content_type, transcode_rate

    def _transcode_sample_rate_for(self, metadata: BackendTrackMetadata) -> Optional[int]:
        """Target sample rate to downsample this track to, or None if it
        already fits the device's real capability.

        Only ever fires for a device with confirmed 24-bit support at a
        sample rate below what we deliberately still request Hi-Res for
        (see DLNACapabilities.max_quality) — e.g. a 24-bit device capped at
        48kHz by a device-specific override. A device without real 24-bit
        support never receives a track that could exceed its cap in the
        first place, since max_quality falls back to CD for it.

        Gated on self._hires_downsampling directly too (not just relying on
        capability overrides never producing max_bit_depth >= 24 when it's
        off) — keeps the experimental feature's on/off boundary obvious and
        self-contained at the one place that actually decides to transcode,
        rather than an indirect consequence of capability detection
        elsewhere.
        """
        if not self._hires_downsampling:
            return None
        caps = self._capabilities
        if caps is None or caps.max_bit_depth < 24:
            return None
        if metadata.sample_rate and metadata.sample_rate > caps.max_sample_rate:
            return caps.max_sample_rate
        return None

    # =========================================================================
    # Gapless Playback
    # =========================================================================

    @property
    def supports_gapless(self) -> bool:
        """Whether this backend supports gapless playback."""
        return self._gapless_supported

    async def set_next_track(
        self, url: str, metadata: BackendTrackMetadata, queue_item_id: int = 0
    ) -> bool:
        """Prepare the next track for gapless transition."""
        if not self._client or not self._gapless_supported:
            return False

        content_type, transcode_rate = self._resolve_content_type_and_transcode(url, metadata)

        # Register with proxy server using unique key
        actual_url = url
        if self._proxy_server:
            proxy_key = f"{metadata.track_id}_{queue_item_id}"
            actual_url = self._proxy_server.register_track(
                track_id=metadata.track_id,
                qobuz_url=url,
                content_type=content_type,
                proxy_key=proxy_key,
                transcode_to_sample_rate=transcode_rate,
            )
            logger.debug(f"Gapless: registered next track proxy URL: {actual_url}")

        # Build DIDL-Lite metadata
        didl = self._build_didl(actual_url, metadata, content_type)

        return await self._arm_next_track(actual_url, didl, metadata)

    async def _arm_next_track(
        self, actual_url: str, didl: str, metadata: BackendTrackMetadata
    ) -> bool:
        """Actually arm the next track on the device. Standard DLNA:
        SetNextAVTransportURI. SonosBackend overrides this to append to
        its AVTransport queue instead."""
        assert self._client
        result: SoapResult = await self._client.set_next_av_transport_uri(actual_url, didl)

        if result.success:
            self._next_track_proxy_url = actual_url
            self._next_track_metadata = metadata
            logger.info(f"Gapless: armed next track: {metadata.artist} - {metadata.title}")
            return True

        if result.is_permanent_failure:
            self._gapless_supported = False
            logger.warning(
                f"Gapless: disabled — device does not support SetNextAVTransportURI "
                f"(error {result.error_code}: {result.error_description})"
            )
        else:
            logger.warning(
                "Gapless: failed to arm next track (transient error), will retry on next poll cycle"
            )
        return False

    async def clear_next_track(self) -> None:
        """Clear prepared next track."""
        self._next_track_proxy_url = None
        self._next_track_metadata = None
        self._next_track_queue_nr = None

    # =========================================================================
    # Internal
    # =========================================================================

    async def _poll_state_loop(self) -> None:
        """Poll device state periodically.

        The only thing that ever reads transport state/position from the
        physical device — also owns gapless-transition detection,
        track-ended/state-change notification, hijack detection (an
        external source now playing to this renderer instead of us), and
        paused-external-stop confirmation, all pushed out via callbacks
        rather than a caller polling this backend independently.
        """
        while self._is_connected:
            try:
                await asyncio.sleep(STATE_POLL_INTERVAL_SECONDS)

                if not self._is_connected:
                    break

                # Get state from device
                new_state = await self.get_state()

                # A retarget is awaiting confirmation that the new
                # coordinator is actually playing our content (see
                # _awaiting_retarget_confirmation) — checked every cycle,
                # independent of new_state/hijack throttling, since we're
                # specifically waiting for the one signal that resolves it.
                # Falls through to normal handling once confirmed or timed
                # out; still counts as being in the grace period below for
                # *this* cycle either way, so nothing downstream double-
                # reads or fires off a stale/premature signal on the same
                # pass that just resolved it.
                if self._awaiting_retarget_confirmation:
                    if time.monotonic() >= self._retarget_confirmation_deadline:
                        logger.warning(
                            f"[{self.name}] Retarget confirmation timed out after "
                            f"{RETARGET_CONFIRMATION_TIMEOUT_SECONDS:.0f}s — resuming normal "
                            "hijack detection without ever seeing our content play "
                            f"(still expected {self._current_proxy_url!r})"
                        )
                        self._awaiting_retarget_confirmation = False
                    elif self._client:
                        current_uri = await self._get_current_transport_uri()
                        logger.debug(
                            f"[{self.name}] Polled while awaiting retarget confirmation: "
                            f"device reports {current_uri!r}, expecting {self._current_proxy_url!r}"
                        )
                        if current_uri == self._current_proxy_url:
                            logger.info(
                                f"[{self.name}] Retarget confirmed — device is now playing "
                                "our content"
                            )
                            self._awaiting_retarget_confirmation = False
                            self._playback_started_at = time.monotonic()

                # Check if we're in the grace period after starting playback
                # (or a retarget still awaiting confirmation — see above).
                in_grace_period = (
                    time.monotonic() - self._playback_started_at
                    < PLAYBACK_START_GRACE_PERIOD_SECONDS
                ) or self._awaiting_retarget_confirmation

                if new_state != PlaybackState.PLAYING:
                    self._hijack_check_countdown = 0
                    self._external_takeover_notified = False

                # Gapless-transition detection and hijack detection are
                # both ultimately answered by the same question — does the
                # device's actual current-source URI match what we expect?
                # (see is_playing_our_content()/_is_playing_our_content_given)
                # — so a cycle that needs both only ever pays for one
                # device round trip, and a read that happens for gapless
                # reasons doubles as a hijack check for free (resetting the
                # throttle countdown either way) instead of waiting for its
                # own separately-throttled turn.
                # self._current_proxy_url is None until this backend has
                # actually been told to play something — before that (e.g.
                # right after startup, before any Qobuz session exists,
                # while the physical device may already be playing
                # something of its own) there is nothing of ours to have
                # been displaced, so neither gapless-transition nor hijack
                # detection has anything meaningful to compare against.
                # is_playing_our_content() (the public method) already
                # guards on this; this inline poll-loop path used to skip
                # it, which is exactly the "hijack detected before there
                # was ever an active session" false positive observed
                # directly right after startup. self._active (see
                # AudioBackend.set_active) covers the same gap once a
                # session has existed but Qobuz isn't driving this renderer
                # right now — a household has one Speaker/backend polling
                # per discovered Sonos room, and a mismatch on a room
                # that's simply not the active one isn't evidence of
                # anything either.
                if (
                    new_state == PlaybackState.PLAYING
                    and self._client
                    and self._current_proxy_url is not None
                    and self._active
                ):
                    self._hijack_check_countdown -= 1
                    hijack_check_due = self._hijack_check_countdown <= 0
                    if hijack_check_due:
                        self._hijack_check_countdown = _HIJACK_CHECK_INTERVAL_POLLS
                    armed = bool(self._next_track_proxy_url)

                    if armed or (hijack_check_due and not in_grace_period):
                        current_uri = await self._get_current_transport_uri()
                        logger.debug(
                            f"[{self.name}] Polled for gapless/hijack check: device reports "
                            f"{current_uri!r}, expecting current={self._current_proxy_url!r}"
                            + (f", next={self._next_track_proxy_url!r}" if armed else "")
                        )

                        if armed and current_uri == self._next_track_proxy_url:
                            logger.info(
                                f"[{self.name}] Gapless: transition detected — device moved "
                                "to next track"
                            )
                            # Update state to reflect the new track
                            self._current_metadata = self._next_track_metadata
                            self._current_proxy_url = self._next_track_proxy_url
                            if self._next_track_metadata:
                                self._duration_ms = self._next_track_metadata.duration_ms
                            self._position_ms = 0
                            self._playback_started_at = time.monotonic()
                            # Clear gapless state — the armed entry is now
                            # the playing one, so don't remove it from the
                            # queue
                            self._next_track_proxy_url = None
                            self._next_track_metadata = None
                            self._next_track_queue_nr = None
                            # Notify player
                            self._notify_next_track_started()
                            self._notify_position_update(0)
                            self._external_takeover_notified = False
                            continue

                        if not in_grace_period and not self._is_playing_our_content_given(
                            current_uri
                        ):
                            # While something's armed (see `armed` above),
                            # this read runs every single poll cycle rather
                            # than the throttled hijack-check cadence — a
                            # genuine, ongoing takeover would otherwise fire
                            # this on every one of those cycles (observed
                            # directly: ~20 seconds of back-to-back
                            # notifications, each independently forcing its
                            # own WebSocket reconnect and racing the others,
                            # eventually desyncing the connection's own
                            # message counter). Only the first read of a
                            # given takeover is worth telling Player about;
                            # _external_takeover_notified is cleared the
                            # moment a read confirms the content is ours
                            # again (or the device leaves PLAYING) so a
                            # later, separate takeover still notifies fresh.
                            if not self._external_takeover_notified:
                                logger.info(
                                    f"[{self.name}] External takeover detected on this "
                                    f"renderer: device reports {current_uri!r}, expected "
                                    f"{self._current_proxy_url!r}"
                                )
                                self._notify_external_takeover()
                                self._external_takeover_notified = True
                            continue
                        elif not in_grace_period:
                            # Content confirmed ours — any previously-
                            # notified takeover has resolved.
                            self._external_takeover_notified = False
                        # else: in_grace_period — no evidence either way
                        # this cycle, leave the flag as it was and fall
                        # through to the rest of the loop as normal.

                # Paused -> confirmed external stop: don't trust a single
                # STOPPED read (transient failures collapse to STOPPED too;
                # a "cold" pause — nothing ever started on this device —
                # never gets here since self._state stays whatever it was
                # before, never PAUSED, in that case).
                if self._state == PlaybackState.PAUSED and new_state == PlaybackState.STOPPED:
                    if in_grace_period or not await self._device_confirms_stopped():
                        # Exactly as untrustworthy as a mismatched hijack
                        # read while the device is still settling (or, per
                        # _device_confirms_stopped, still shows our content
                        # loaded despite the STOPPED read) — reset the count
                        # instead of accumulating toward a confirmation, and
                        # skip straight past the general state-change notify
                        # below too (same `continue` the confirmed-count
                        # path already uses).
                        self._paused_stop_polls = 0
                        continue
                    self._paused_stop_polls += 1
                    if self._paused_stop_polls >= _PAUSED_STOP_CONFIRMATIONS:
                        self._paused_stop_polls = 0
                        self._notify_state_change(PlaybackState.STOPPED)
                    continue
                self._paused_stop_polls = 0

                # Detect state changes
                if new_state != self._state:
                    logger.debug(f"State changed: {self._state} -> {new_state}")

                    # Check for track end before updating state
                    if self._state == PlaybackState.PLAYING and new_state == PlaybackState.STOPPED:
                        # If gapless was armed but device stopped, clear and fall through
                        if self._next_track_proxy_url:
                            logger.debug(
                                "Gapless: device stopped despite armed next track, "
                                "falling through to normal track-ended"
                            )
                            self._next_track_proxy_url = None
                            self._next_track_metadata = None

                        if in_grace_period or not await self._device_confirms_stopped():
                            # Ignore the STOPPED read entirely — either
                            # still within the ordinary grace window, or
                            # the device itself still shows our content
                            # loaded (see _device_confirms_stopped), which
                            # means this STOPPED string isn't trustworthy
                            # regardless of the timer. Prevents false
                            # track-ended events either way.
                            logger.debug(
                                f"[{self.name}] Ignoring unconfirmed STOPPED state "
                                f"(started {time.monotonic() - self._playback_started_at:.1f}s ago)"
                            )
                            continue  # Skip state update entirely
                        else:
                            self._notify_track_ended()

                    self._notify_state_change(new_state)

                # Update position while playing
                if new_state == PlaybackState.PLAYING:
                    pos = await self.get_position()
                    self._notify_position_update(pos)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"State poll error: {e}")

    def _build_didl(
        self,
        url: str,
        metadata: BackendTrackMetadata,
        content_type: str = "audio/flac",
    ) -> str:
        """Build DIDL-Lite metadata XML."""

        def escape(s: str) -> str:
            return (
                s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )

        # Build protocol info string based on capabilities
        if self._capabilities:
            protocol_info = build_protocol_info(self._capabilities, content_type)
        else:
            protocol_info = f"http-get:*:{content_type}:*"

        # Format duration as H:MM:SS for the res element
        duration_attr = ""
        if metadata.duration_ms > 0:
            total_s = metadata.duration_ms // 1000
            h = total_s // 3600
            m = (total_s % 3600) // 60
            s = total_s % 60
            duration_attr = f' duration="{h}:{m:02d}:{s:02d}"'

        didl = f"""<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/"
                   xmlns:dc="http://purl.org/dc/elements/1.1/"
                   xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">
    <item id="1" parentID="0" restricted="1">
        <dc:title>{escape(metadata.title)}</dc:title>
        <dc:creator>{escape(metadata.artist)}</dc:creator>
        <upnp:artist>{escape(metadata.artist)}</upnp:artist>
        <upnp:album>{escape(metadata.album)}</upnp:album>
        <upnp:class>object.item.audioItem.musicTrack</upnp:class>"""

        if metadata.artwork_url:
            didl += f"\n        <upnp:albumArtURI>{escape(metadata.artwork_url)}</upnp:albumArtURI>"

        audio_attrs = ""
        if metadata.sample_rate:
            audio_attrs += f' sampleFrequency="{metadata.sample_rate}"'
        if metadata.bit_depth:
            audio_attrs += f' bitsPerSample="{metadata.bit_depth}"'

        didl += f"""
        <res protocolInfo="{escape(protocol_info)}"{duration_attr}{audio_attrs}>{escape(url)}</res>
    </item>
</DIDL-Lite>"""

        return didl

"""
QobuzProxy Player.

Core playback controller that orchestrates queue, metadata, and audio backend.
"""

import asyncio
import functools
import logging
import time
from typing import Awaitable, Callable, Dict, Optional, TypeVar, TYPE_CHECKING

from qobuz_proxy.backends import (
    AdoptedPlayback,
    AudioBackend,
    BackendTrackMetadata,
    PlaybackState,
    PlayResult,
    BufferStatus,
)
from qobuz_proxy.connect.join_state import JoinSnapshot

from qobuz_proxy.timing import slow_step

from .queue import QobuzQueue, QueueTrack, RepeatMode
from .metadata import MetadataService

if TYPE_CHECKING:
    from .state_reporter import StateReporter
    from .play_reporter import PlayReporter

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# When we begin a track at a position beyond this, treat it as adopted mid-play
# from the controlling app (a Connect handoff) rather than a play we initiated.
# The app already reported/scrobbled that play, so we suppress our own
# reportStreamingStart to avoid a duplicate Last.fm scrobble.
_HANDOFF_POSITION_THRESHOLD_MS = 5000

# After a WebSocket reconnect, the Qobuz server replays its last-known session
# snapshot via SET_STATE — typically PAUSED at a position from before the drop.
# If the renderer is actually still playing further along the same track, treat
# that as a stale replay and ignore the pause/seek. This is the minimum gap
# (renderer ahead of server) at which we suppress.
#
# Calibrated for a WS that had been down for a while before reconnecting
# (a gap of minutes) — too generous now that the control token
# self-refreshes on its own (see docs/gapless-arming-and-token-expiry.md)
# and reconnects in well under a second: the app's cached position is
# barely stale at all, so the gap this catches on its own can be a lot
# smaller than 5s and still be exactly the same replay (observed
# directly: a genuine spurious pause with a ~300ms-old snapshot,
# nowhere near this threshold). _RECONNECT_STALE_GRACE_SECONDS below
# covers that case by time instead of position gap.
_STALE_SNAPSHOT_THRESHOLD_MS = 5000

# However small the position gap, a PAUSED-at-current-track SET_STATE
# arriving this soon after our own last claim_device() (SET_ACTIVE(true),
# i.e. a reconnect) is still almost certainly the same stale-replay
# phenomenon _STALE_SNAPSHOT_THRESHOLD_MS exists for, just too recent a
# reconnect for the position to have drifted far. Same order of magnitude
# as PLAYBACK_START_GRACE_PERIOD_SECONDS/_DEACTIVATION_GRACE_SECONDS for
# the same underlying reason — a signal can be transiently wrong right
# after a transition even once it's normally trustworthy.
_RECONNECT_STALE_GRACE_SECONDS = 5.0

# A polled position this far from where the interpolated clock says the track
# should be is a seek, not drift (RelTime has one-second resolution).
_POSITION_JUMP_MS = 2500

# How long after claiming the device the first replayed state may still be
# answered by carrying on with what the device is already playing.
_ADOPTION_WINDOW_SECONDS = 30.0

# How long playback can sit PAUSED or STOPPED before it's no longer worth
# proactively keeping the control WebSocket alive for (see
# is_worth_keeping_ws_alive(), queried by WsManager's own self-refresh —
# docs/gapless-arming-and-token-expiry.md). Deliberately generous: this
# only ever lets an *idle* session's WS lapse naturally (falling back to
# the pre-self-refresh behavior of waiting for the app) rather than
# refreshing forever in the background with nobody listening — a normal
# listening pause (stepped away, a phone call) must never trip this.
_WS_IDLE_TIMEOUT_SECONDS = 3600.0

# How long the command queue's consumer waits for the backend to become
# attached again (see set_backend_attached()) before dispatching a
# playback-directing item anyway. Mirrors DLNABackend's own
# RECONNECT_WAIT_SECONDS value (kept independent rather than imported —
# Player has no business knowing about a specific backend implementation)
# — comfortably inside SonosDiscoveryManager's PENDING_GRACE_SECONDS so a
# genuine handoff resolves well before this fires, while a truly-gone
# device still fails in reasonable time rather than holding the queue
# forever.
_BACKEND_ATTACH_WAIT_SECONDS = 8.0

# How long a SET_ACTIVE(false) waits before actually stopping playback — see
# deactivate_renderer()'s own docstring for the race this absorbs. Same
# order of magnitude as DLNABackend's own PLAYBACK_START_GRACE_PERIOD_SECONDS
# for the same underlying reason: a signal that's meaningful once it settles
# can still be transiently wrong right after a transition.
_DEACTIVATION_GRACE_SECONDS = 5.0

# enqueue()'s default coalesce_group — every existing coalesce=True call
# site (SET_STATE, natural track-end, an explicit stop_playback) shares
# this one, matching the original single-slot behavior: any one of them
# supersedes an earlier still-pending one of any of the others, which is
# the intended "the goal is always the latest desired playback state"
# behavior _superseded_by_newer_command() exists for.
_PLAYBACK_COALESCE_GROUP = "playback"

# A separate coalesce group for volume commands — see VolumeCommandHandler's
# call site in speaker.py. Volume floods (a server echoing our own
# RndrSrvrVolumeChanged report back as a fresh SET_VOLUME command, live-
# confirmed 2026-09-12/13 to cycle through several recently-seen values
# rather than repeat one, defeating a same-value check alone) must not
# share a slot with _PLAYBACK_COALESCE_GROUP — an unrelated queued volume
# command superseding a pending track-change command (or vice versa)
# would be a real, unrelated correctness bug, not just a missed
# optimization.
VOLUME_COALESCE_GROUP = "volume"


class _CommandQueueItem:
    """One entry in Player._command_queue — see Player.enqueue().

    Identity-compared (no custom __eq__/__hash__), which is exactly what
    enqueue()'s coalescing sweep and the consumer's "was this coalesced
    away" check need: two items are never "equal" just because they
    happen to wrap the same bound method.
    """

    __slots__ = ("coro_fn", "coalesce", "coalesce_group", "cancelled")

    def __init__(
        self, coro_fn: Callable[[], Awaitable[None]], coalesce: bool, coalesce_group: str
    ) -> None:
        self.coro_fn = coro_fn
        self.coalesce = coalesce
        self.coalesce_group = coalesce_group
        self.cancelled = False


class QobuzPlayer:
    """
    Main playback controller.

    Coordinates:
    - Queue: Track ordering, shuffle, repeat
    - MetadataService: Track info and streaming URLs
    - AudioBackend: Actual audio playback
    - WsManager: State reporting to app

    State machine:
        STOPPED -> LOADING (on play)
        LOADING -> PLAYING (when ready)
        LOADING -> ERROR (on failure)
        PLAYING -> PAUSED (on pause)
        PAUSED -> PLAYING (on play/resume)
        PLAYING -> STOPPED (on stop or track end)
        PAUSED -> STOPPED (on stop)
    """

    def __init__(
        self,
        queue: QobuzQueue,
        metadata_service: MetadataService,
        backend: AudioBackend,
        play_reporter: Optional["PlayReporter"] = None,
    ):
        """Initialize player."""
        self.queue = queue
        self.metadata = metadata_service
        self.backend = backend
        # Optional: reports plays to Qobuz (listening history / Last.fm scrobbling).
        self._play_reporter = play_reporter

        # Current track
        self._current_track: Optional[QueueTrack] = None
        self._current_duration_ms: int = 0

        # Position tracking (timestamp-based like C++ implementation)
        self._position_timestamp_ms: int = 0
        self._position_value_ms: int = 0

        # State
        self._state: PlaybackState = PlaybackState.STOPPED

        # Whether the last real play/pause/stop decision was "play" —
        # distinct from self._state, which _load_track_locked transiently
        # sets to STOPPED as a normal, necessary part of loading a new
        # track regardless of whether playback resumes right after. A
        # command superseded mid-load (see Player._superseded_by_newer_
        # command) bails out before ever reaching _play_locked, leaving
        # self._state sitting at that transient STOPPED — a later command
        # in the same rapid-swipe burst reading self._state to decide
        # whether to auto-continue playing would then wrongly conclude
        # nothing was playing (observed directly, test1.log 2026-08-28: a
        # second rapid swipe correctly skipped the now-stale first target,
        # but then sat loaded-and-silent for 7+ seconds instead of
        # auto-continuing, because the skipped command had already flipped
        # self._state to STOPPED before bailing). This field is updated
        # only by real play/pause/stop decisions and genuine backend-
        # reported state changes, never by a load in progress, so it
        # survives a skipped intermediate command intact.
        self._intended_playing: bool = False

        # Whether the backend actually has the current track loaded/started —
        # False right after a fresh load (e.g. we were just made the active
        # renderer while the track was already paused on another renderer),
        # True once _start_playback() has actually called backend.play().
        # PAUSED can happen either way: a real pause (True, backend has
        # something to resume) or a "cold" pause carried over from a load
        # (False, nothing to resume — see _pause_locked and _play_locked).
        self._backend_engaged: bool = False

        # Whether the Qobuz server currently considers *this* renderer the
        # active playback target (SrvrRndrSetActive) — the one authoritative
        # signal for "is this the renderer the app is actually driving right
        # now". False until the server says otherwise — a freshly started
        # renderer has no controller attached yet.
        self._is_active_renderer: bool = False
        # A pending, debounced stop from deactivate_renderer() — see its
        # own docstring. None whenever nothing is currently scheduled.
        self._pending_deactivation_task: Optional[asyncio.Task] = None
        # When claim_device() (SET_ACTIVE(true)) last ran — see
        # _is_stale_pause_snapshot_locked's use of this. 0.0 is
        # deliberately "long ago" relative to any real time.monotonic()
        # reading, never mistaken for "just now".
        self._last_activated_at: float = 0.0
        # What the device was found playing when we claimed it (see claim_device).
        self._adoptable: Optional[AdoptedPlayback] = None

        # When the state last left PLAYING (PAUSED or STOPPED), or None while
        # actually PLAYING — see is_worth_keeping_ws_alive().
        self._non_playing_since: Optional[float] = None

        # Whether the backend is currently reachable — see
        # set_backend_attached(). True for the vast majority of backends,
        # which never toggle it at all; Speaker.detach()/retarget() (see
        # speaker.py) set it False/True around a Sonos group_id going
        # pending — see SonosDiscoveryManager's pending state. The command
        # queue holds a playback-directing item at the front while False
        # (see _command_consumer_loop) instead of dispatching into
        # backend.play()'s own reconnect wait.
        self._backend_attached: bool = True
        self._backend_attached_event: asyncio.Event = asyncio.Event()
        self._backend_attached_event.set()

        # Command queue: the single entry point for everything that drives
        # playback — WsManager-dispatched commands (via enqueue(), wired up
        # in speaker.py) and the backend-driven natural-track-end
        # continuation (_on_track_ended) alike. One consumer
        # (_command_consumer_loop) runs items strictly one at a time, which
        # is what gives them mutual exclusion now: a track switch in the
        # Qobuz app sends a burst of SET_STATE messages, and without this,
        # the resulting load/play/stop calls would overlap and fire
        # concurrent SOAP control requests, wedging DLNA AVTransport
        # renderers — the same problem a lock used to guard against here,
        # solved instead by nothing ever running concurrently in the first
        # place. coalesce=True (see enqueue()) reproduces the old
        # generation-counter's "a newer command supersedes an older one
        # still waiting to run" behavior for playback-directing commands
        # specifically.
        self._command_queue: "asyncio.Queue[_CommandQueueItem]" = asyncio.Queue()
        self._command_task: Optional[asyncio.Task] = None
        # At most one pending (not yet started) coalesce=True item per
        # coalesce_group — what enqueue() replaces/cancels when a new one
        # in the same group supersedes it. Keyed by group rather than a
        # single shared slot so unrelated coalesced command streams (e.g.
        # playback-directing SET_STATE/track-ended/stop vs. volume, see
        # VOLUME_COALESCE_GROUP) never cancel each other out.
        self._pending_coalescable: Dict[str, _CommandQueueItem] = {}
        # True for the duration of _command_consumer_loop actually awaiting
        # a dequeued item — replaces _playback_lock.locked() as the "is a
        # command currently running" signal _on_state_change discriminates
        # on (an unprompted backend notification can't otherwise be told
        # apart from one acking a command we just issued, since
        # AudioBackend._notify_state_change fires synchronously inside the
        # awaited backend call, before the running command's own explicit
        # self._state assignment).
        self._processing_command: bool = False

        # State reporting - supports both callback and StateReporter
        self._state_update_callback: Optional[Callable[[], asyncio.Future]] = None
        self._state_reporter: Optional["StateReporter"] = None

        # Called when an external takeover is detected (see
        # is_playing_our_content()). The Qobuz Connect protocol has no
        # message for voluntarily giving up "active" status, so the best
        # available signal is forcing a real WebSocket reconnect — see
        # WsManager.force_reconnect() and set_hijack_detected_callback().
        self._hijack_detected_callback: Optional[Callable[[str], Awaitable[None]]] = None

        # Volume
        self._volume: int = 50  # Cached volume level (0-100)
        self._fixed_volume: bool = False  # From config
        self._volume_report_callback: Optional[Callable[[int], asyncio.Future]] = None

        # File quality report callback - called when track starts playing
        self._file_quality_report_callback: Optional[Callable[[int], asyncio.Future]] = None

        # Next track callback - used when track ends to get the next track from SET_STATE
        self._get_next_track_callback: Optional[Callable[[], Optional[dict]]] = None
        self._clear_next_track_callback: Optional[Callable[[], None]] = None

        # Gapless playback state
        self._pending_next_track: Optional[dict] = None
        self._gapless_armed: bool = False
        self._transition_generation: int = 0
        self._gapless_arm_lock: asyncio.Lock = asyncio.Lock()
        # The repeat mode as last announced (see set_loop_mode). Kept here as
        # well as on the queue because the gapless path consults it on every
        # arm attempt: with repeat-one on, nothing must be armed, or the
        # device would advance past the current track instead of stopping
        # for _handle_track_ended to restart it.
        self._repeat_mode: RepeatMode = RepeatMode.OFF
        # Whether _prepare_next_track_locked has already warned about having
        # no next-track info to arm with, since the last time info was
        # actually available — see its own docstring. Cleared as soon as
        # info reappears, so a later gap warns again instead of going quiet
        # forever after the first one.
        self._next_track_info_unavailable_warned: bool = False

        # Callback for next track info changes (from command handler)
        self._on_next_track_changed_callback: Optional[Callable[[], None]] = None

        self._is_running: bool = False

        # Wire up queue callbacks to metadata service
        self.queue.set_metadata_callback(self._get_track_metadata)

        # Wire up backend callbacks
        self.backend.on_track_ended(self._on_track_ended)
        self.backend.on_playback_error(self._on_playback_error)
        self.backend.on_position_update(self._on_position_update)
        self.backend.on_next_track_started(self._on_next_track_started)
        self.backend.on_item_changed(self._on_item_changed)
        self.backend.on_resync(self._on_backend_resync)
        self.backend.on_state_change(self._on_state_change)
        self.backend.on_external_takeover(self._on_external_takeover)
        self.backend.on_volume_update(self._on_volume_update)

        logger.info("QobuzPlayer initialized")

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Start the player and its components."""
        if self._is_running:
            return

        self._is_running = True

        # Connect backend
        if not self.backend.is_connected():
            await self.backend.connect()

        # Start the command queue's single consumer
        self._ensure_command_consumer_running()

        logger.info("Player started")

    def _ensure_command_consumer_running(self) -> None:
        """Start the command queue's consumer if it isn't already running —
        idempotent, and independent of the rest of start()'s work (queue
        preloading, backend connect).

        detach()/retarget() can legitimately be called before start() —
        e.g. a Sonos group_id going pending or getting retargeted before
        the Qobuz app has ever connected to this speaker, see Speaker.
        detach()/retarget() — and both need somewhere for their enqueued
        item to actually be picked up rather than waiting forever on a
        consumer nothing ever started. Called from both start() and
        _enqueue_and_wait() for that reason, rather than each managing
        _command_task itself."""
        if self._command_task is None or self._command_task.done():
            self._command_task = asyncio.create_task(self._command_consumer_loop())

    async def stop(self, send_device_stop: bool = True) -> None:
        """Stop the player and clean up.

        Args:
            send_device_stop: Forwarded to backend.disconnect() — set False
                when the device shouldn't actually be told to stop (see
                AudioBackend.disconnect()).
        """
        self._is_running = False
        self._cancel_pending_deactivation()

        if self._command_task:
            self._command_task.cancel()
            try:
                await self._command_task
            except asyncio.CancelledError:
                pass
            self._command_task = None

        # Close any open play report (incl. a paused listen, which no longer
        # closes on pause) so a shutdown mid-listen still lands in history.
        await self._report_stopped()

        # Disconnect backend
        await self.backend.disconnect(send_device_stop=send_device_stop)

        logger.info("Player stopped")

    def set_state_update_callback(self, callback: Callable[[], asyncio.Future]) -> None:
        """Set callback to send state updates to app (legacy method)."""
        self._state_update_callback = callback

    def set_hijack_detected_callback(self, callback: Callable[[str], Awaitable[None]]) -> None:
        """Set callback invoked (with a reason string) when an external
        takeover of this renderer is detected."""
        self._hijack_detected_callback = callback

    def set_state_reporter(self, reporter: "StateReporter") -> None:
        """
        Set the StateReporter for this player.

        When set, the StateReporter handles all state reporting including
        the periodic heartbeat and immediate updates.
        """
        self._state_reporter = reporter

    def set_volume_report_callback(self, callback: Callable[[int], asyncio.Future]) -> None:
        """Set callback to report volume changes to app."""
        self._volume_report_callback = callback

    def set_file_quality_report_callback(self, callback: Callable[[int], asyncio.Future]) -> None:
        """Set callback to report file quality when track starts playing."""
        self._file_quality_report_callback = callback

    def set_fixed_volume_mode(self, enabled: bool) -> None:
        """Enable or disable fixed volume mode."""
        self._fixed_volume = enabled
        logger.info(f"Fixed volume mode: {enabled}")

    async def set_active_renderer(self, active: bool) -> None:
        """Record whether the Qobuz server currently considers this
        renderer the active playback target (see SrvrRndrSetActive).

        Also relayed to the backend (see AudioBackend.set_active) — a
        backend that watches its physical device on its own (DLNABackend's
        poll loop) has no way to know "active" is a Qobuz Connect concept
        it can't see for itself without this; DLNABackend actually stops
        polling while inactive rather than merely recording the flag, so
        this must be awaited (called from within a command-queue item —
        see PlaybackCommandHandler._handle_set_active — so awaiting here
        never overlaps whatever else the queue is doing)."""
        self._is_active_renderer = active
        await self.backend.set_active(active)

    async def set_backend_attached(self, attached: bool) -> None:
        """Record whether the backend is currently reachable — called by
        this Player's own detach()/retarget() (themselves invoked from
        Speaker.detach()/retarget(), see there) around a Sonos group_id
        going pending (see SonosDiscoveryManager's pending state). Going False also
        nudges self._state toward an honest transitional state and relays
        it immediately, so the app sees a real signal during a handoff
        instead of either silence or a stale, still-ticking PLAYING
        position — the same "discrete event -> immediate send" path every
        other real transition already uses (see StateReporter).
        """
        was_attached = self._backend_attached
        self._backend_attached = attached
        if attached:
            self._backend_attached_event.set()
            # A retarget just succeeded — this is Player.retarget()'s own
            # sole call site. The backend clears its own next-track
            # bookkeeping whenever it actually retargets to a different
            # device (DLNABackend.retarget() — the physical device's own
            # queue can't be assumed to carry over what we'd armed on the
            # old one), but nothing told *this* flag its own armed state
            # might now be stale. Left alone, it stayed True forever
            # (observed directly: a gapless transition Sonos itself
            # carried through anyway on the new device went undetected as
            # gapless — the backend had nothing armed to compare against —
            # and read as an external takeover instead), permanently
            # blocking the ordinary per-position-tick re-arm retry (see
            # _on_position_update) from ever running again. Cleared here
            # unconditionally, not just on a detached->attached
            # transition, since Player.retarget() calls this on every
            # successful retarget regardless of whether backend_attached
            # ever actually went False — costs at most one harmless
            # re-arm attempt when nothing actually changed (the backend's
            # own set_next_track dedups an identical still-armed URL).
            self._clear_gapless_state()
            return
        self._backend_attached_event.clear()
        if was_attached and self._state == PlaybackState.PLAYING:
            self._state = PlaybackState.LOADING
            await self._send_state_update()

    async def detach(self) -> None:
        """Give up the current backend target without tearing down the
        Qobuz Connect session — see Speaker.detach(), the sole caller.

        Runs as an ordinary command-queue item (see enqueue()) so it can
        never overlap whatever backend call the consumer is currently in
        the middle of — the same hazard (two concurrent SOAP calls
        wedging the renderer) the command queue exists to prevent for
        play/pause/seek in the first place applies just as much to a
        retarget/detach, and previously nothing prevented it since this
        used to run directly on the Sonos discovery manager's own task
        instead of through this queue. Unlike a playback-directing
        command, detach/retarget are never coalesced away — a detach must
        always actually run, not be silently dropped because something
        newer landed behind it in the meantime; it simply waits its turn.
        """
        await self._enqueue_and_wait(self._detach_locked)

    async def _detach_locked(self) -> None:
        # A device playing a queue it holds itself carries on through a handoff
        # (the queue moves to the new coordinator): stopping it is the one thing
        # that would interrupt it.
        await self.backend.disconnect(send_device_stop=not self.backend.keeps_playing_on_shutdown)
        await self.set_backend_attached(False)

    async def retarget(self, ip: str, port: int) -> bool:
        """Repoint the backend at a different physical device, in place —
        see Speaker.retarget(), the sole caller. Same command-queue
        rationale as detach().

        Returns:
            True if the retarget succeeded.
        """
        return await self._enqueue_and_wait(functools.partial(self._retarget_locked, ip, port))

    async def _retarget_locked(self, ip: str, port: int) -> bool:
        ok = await self.backend.retarget(ip, port)
        if ok:
            await self.set_backend_attached(True)
            if self.backend.keeps_playing_on_shutdown and self._current_track is not None:
                await self._resync_after_retarget()
        return ok

    async def _enqueue_and_wait(self, coro_fn: Callable[[], Awaitable[_T]]) -> _T:
        """Enqueue coro_fn (plain FIFO, never coalesced — see enqueue())
        and wait for it to actually run, returning its result.

        For callers (detach()/retarget()) that need "the operation has
        happened by the time this returns" — the same calling convention
        Speaker.detach()/retarget() already had when they ran outside
        this queue entirely — while still gaining the queue's mutual
        exclusion against every other backend-directed call. Since
        neither detach nor retarget is ever coalesced away (see
        enqueue()), the wrapped item is guaranteed to eventually run as
        long as the command consumer keeps making progress, so this never
        waits on a future nothing will ever resolve — as long as a
        consumer is actually running to make that progress, which is why
        this starts one if start() hasn't been called yet (see
        _ensure_command_consumer_running()).
        """
        self._ensure_command_consumer_running()
        loop = asyncio.get_running_loop()
        done: "asyncio.Future[_T]" = loop.create_future()

        async def _wrapped() -> None:
            try:
                result = await coro_fn()
            except Exception as exc:
                if not done.done():
                    done.set_exception(exc)
                raise
            else:
                if not done.done():
                    done.set_result(result)

        self.enqueue(_wrapped)
        return await done

    def set_next_track_callbacks(
        self,
        get_callback: Callable[[], Optional[dict]],
        clear_callback: Callable[[], None],
    ) -> None:
        """
        Set callbacks for getting next track info from command handler.

        This is used for auto-advance when the current track ends.
        The get_callback should return track info dict with queueItemId and trackId,
        or None if no next track is available.
        """
        self._get_next_track_callback = get_callback
        self._clear_next_track_callback = clear_callback

    # =========================================================================
    # Volume Controls
    # =========================================================================

    async def set_volume(self, level: int) -> int:
        """
        Set absolute volume level.

        Args:
            level: Volume level (0-100), will be clamped to valid range

        Returns:
            Actual volume level after clamping
        """
        # Clamp to valid range
        clamped = max(0, min(100, level))

        if self._fixed_volume:
            logger.debug(f"Fixed volume mode: ignoring set_volume({level})")
            return self._volume  # Return current (ignored)

        # A command re-asserting the volume we already have is a genuine
        # no-op — skip the backend call and, critically, the app-facing
        # report too. Confirmed live (2026-09-12): while this proxy's
        # Controller role used a second, synthetic deviceUuid distinct
        # from its Renderer role, a server confused about which identity
        # owned this connection echoed our own RndrSrvrVolumeChanged
        # report straight back as a fresh SrvrRndrSetVolume command —
        # every report became another command to handle, a self-
        # sustaining loop with no natural end (~4000 calls in 27s in one
        # capture), which starved the single command queue badly enough
        # to also block natural-track-end detection (the same queue) for
        # the rest of that session. The Controller role now joins under
        # the Renderer's own deviceUuid instead (see ws_manager.py's
        # _send_ctrl_join_session), which fixed the identity confusion
        # this stemmed from — but skipping a same-value command is safe
        # regardless of cause, since nothing is lost when nothing
        # changes, so this stays as a second, independent line of
        # defense against the same failure mode however it might recur.
        if clamped == self._volume:
            logger.debug(f"set_volume({clamped}): already at this level, no-op")
            return clamped

        await self.backend.set_volume(clamped)
        self._volume = clamped

        # Report change to app
        await self._report_volume_change()

        logger.info(f"Volume set to {clamped}")
        return clamped

    async def set_volume_delta(self, delta: int) -> int:
        """
        Adjust volume by relative amount.

        Args:
            delta: Amount to adjust (+/- value)

        Returns:
            New volume level after adjustment
        """
        current = await self.get_volume()
        new_level = current + delta
        return await self.set_volume(new_level)

    async def get_volume(self) -> int:
        """
        Get current volume level.

        Returns:
            Volume level (0-100)
        """
        if self._fixed_volume:
            return 100  # Fixed volume always reports 100

        # Get from backend (authoritative source)
        self._volume = await self.backend.get_volume()
        return self._volume

    async def _report_volume_change(self) -> None:
        """Send volume change notification to app.

        Gated on `is_active_renderer` for the same reason StateReporter is
        (see its `_send_state_update` docstring): a stray post-deactivation
        volume poll must not send a report the server will just reject.
        Safe for `broadcast_current_volume()`'s own legitimate call too —
        `_handle_set_active` always sets `_is_active_renderer = True` before
        calling it, so this never blocks that case.
        """
        if not self._volume_report_callback:
            return
        if not self._is_active_renderer:
            logger.debug("Not the active renderer, skipping volume report")
            return

        try:
            await self._volume_report_callback(self._volume)
        except Exception as e:
            logger.error(f"Failed to report volume change: {e}")

    async def broadcast_current_volume(self) -> None:
        """Refresh volume from the backend and re-emit it to the controller.

        Used when a controller (re)attaches — e.g. on `SrvrRndrSetActive(active=true)`
        — because the Qobuz cloud does not seem to replay our last
        `RndrSrvrVolumeChanged` to a freshly-subscribed controller, leaving the
        device picker without a volume bar until we send it again.
        """
        try:
            volume = await self.get_volume()
            await self._report_volume_change()
            logger.info(f"Re-broadcast current volume to app: {volume}%")
        except Exception as e:
            logger.warning(f"Failed to re-broadcast volume: {e}")

    async def claim_device(self) -> None:
        """Send the physical device a plain stop, without touching our own
        playback state/reporting (there's nothing of ours to report — we
        were never playing anything before this).

        Used when we're freshly selected as the active renderer
        (`SrvrRndrSetActive(active=true)`): a shared DLNA/Sonos renderer
        may already be playing something from a completely different
        source (Spotify via AirPlay, the Sonos app, ...) when the Qobuz
        app selects it. Silencing it on selection — rather than letting
        that keep going until the app actually picks a track — claims a
        silent, ready state the same way Spotify Connect does.

        But SET_ACTIVE(true) doesn't only fire on a genuinely fresh
        selection — it fires just as much when the app's control
        WebSocket reconnects (a token refresh, a brief network blip) while
        *our own* content has been playing the whole time, uninterrupted,
        underneath a control channel that happened to be down (see
        docs/gapless-arming-and-token-expiry.md). This method's own
        assumption ("nothing of ours to report") is only true in the
        first case; in the second, self._current_track is already ours
        and still legitimately playing — sending a raw stop here would
        kill it outright (observed directly: the device went silent, and
        Player's own state/current_track were left claiming PLAYING with
        nothing to back it up, so a later seek/play from the app no
        longer did anything until an unrelated track load resynced the
        two). So: only actually silence the device when we don't already
        have something of our own active — that's the only case this was
        ever meant to guard.
        """
        self._cancel_pending_deactivation()
        # Marks "we just (re)joined as active" for _is_stale_pause_snapshot_locked —
        # see its own docstring for why a reconnect this recent makes even a
        # *small* position gap suspect.
        self._last_activated_at = time.monotonic()
        if self._current_track is not None and self._state != PlaybackState.STOPPED:
            logger.debug("claim_device: already have active content of our own, not stopping it")
            return
        # A proxy that (re)started while the device kept playing one of its own
        # entries: that is not something to silence but to carry on with. The
        # state the server replays right after this (what it remembers from
        # before) is then not applied, see _adopt_playing_device.
        self._adoptable = await self._find_adoptable()
        if self._adoptable is not None:
            logger.info(
                f"claim_device: the device is already playing our entry "
                f"{self._adoptable.track_id} (item {self._adoptable.queue_item_id}), not stopping it"
            )
            return
        try:
            await self.backend.stop()
        except Exception as e:
            logger.warning(f"Failed to stop device on activation: {e}")

    async def _find_adoptable(self) -> Optional[AdoptedPlayback]:
        """What the device plays of ours, if it belongs to the current session
        (the queue's version is not known yet when this runs)."""
        try:
            playing = await self.backend.playing_entry()
        except Exception as e:
            logger.warning(f"Could not look at what the device is playing: {e}")
            return None
        if playing is None:
            return None
        if playing.session != self.queue.generation[0]:
            return None  # another session's entry: not ours to carry on with
        return playing

    async def _adopt_playing_device(self) -> bool:
        """Take over what the device is playing as the current track, instead
        of applying the replayed state that follows a rejoin. True if adopted."""
        playing, self._adoptable = self._adoptable, None
        if playing is None or time.monotonic() - self._last_activated_at > _ADOPTION_WINDOW_SECONDS:
            return False
        if playing.major != self.queue.generation[1]:
            # An older queue than the one the server holds: not a match.
            logger.info("The device plays an entry of another queue: stopping it")
            try:
                await self.backend.stop()
            except Exception as e:
                logger.warning(f"Failed to stop the device: {e}")
            return False
        await self._take_over(playing)
        return True

    async def _take_over(self, playing: AdoptedPlayback) -> None:
        """Make what the device plays the current track, position and state,
        and tell the app."""
        previous = self._current_track
        meta = await self._get_track_metadata(playing.track_id)
        track = QueueTrack(
            queue_item_id=playing.queue_item_id,
            track_id=playing.track_id,
            context_uuid=previous.context_uuid if previous else None,
            metadata=meta or {},
            duration_ms=meta.get("duration_ms", 0) if meta else 0,
        )
        self._current_track = track
        self._current_duration_ms = track.duration_ms
        self._position_value_ms = playing.position_ms
        self._position_timestamp_ms = int(time.time() * 1000)
        self._state = PlaybackState.PAUSED if playing.paused else PlaybackState.PLAYING
        self._backend_engaged = True
        self._intended_playing = not playing.paused
        await self.backend.adopt(playing, track.duration_ms)
        logger.info(
            f"Carrying on with track {playing.track_id} (queue item {playing.queue_item_id}) "
            f"found on the device at {playing.position_ms}ms"
        )
        await self._send_state_update()
        if not playing.paused:
            await self._report_playing(playing.position_ms)

    def _on_backend_resync(self) -> None:
        """The retarget settled (see AudioBackend.on_resync): enqueued like any
        other backend-driven change."""
        self.enqueue(functools.partial(self._resync_after_retarget, True))

    async def _resync_after_retarget(self, settled: bool = False) -> None:
        """A device that plays its own queue carried on through the handoff
        (a new coordinator took the queue over): report what it is really doing
        instead of the 'loading' left by the detach."""
        playing = await self._find_adoptable()
        if playing is not None and playing.major == self.queue.generation[1]:
            await self._take_over(playing)
            return
        if settled and self._state == PlaybackState.LOADING:
            self._state = PlaybackState.STOPPED  # nothing of ours plays there
            await self._send_state_update()

    async def deactivate_renderer(self) -> None:
        """Handle SET_ACTIVE(false) — debounced, not an immediate stop.

        Every WS reconnect — including our own token self-refresh, now
        happening every ~10-20 minutes instead of rarely (see
        docs/gapless-arming-and-token-expiry.md) — races the server's own
        bookkeeping for "is this renderer active": observed directly,
        reconnecting sends SET_ACTIVE(true) via JOIN_SESSION, the server
        sometimes hasn't caught up to that yet when our very next message
        arrives, rejects it ("Renderer state updated message received
        from non active renderer"), and immediately follows with
        SET_ACTIVE(false) — then flips back to SET_ACTIVE(true) again
        within anywhere from under a second to a few minutes once it
        settles. Stopping playback on that raw signal, unconditionally,
        turned a transient race into a real interruption every single
        refresh cycle. Waiting a few seconds for a reactivation that
        cancels this — see claim_device() — before actually stopping
        keeps genuine deactivation (a different device/session actually
        taking over) working, just no longer instant.
        """
        self._cancel_pending_deactivation()
        self._pending_deactivation_task = asyncio.create_task(self._deactivate_after_grace())

    async def _deactivate_after_grace(self) -> None:
        try:
            await asyncio.sleep(_DEACTIVATION_GRACE_SECONDS)
        except asyncio.CancelledError:
            return
        await self.stop_playback()

    def _cancel_pending_deactivation(self) -> None:
        task = self._pending_deactivation_task
        if task is not None and not task.done():
            task.cancel()
        self._pending_deactivation_task = None

    def is_worth_keeping_ws_alive(self) -> bool:
        """Whether the control WebSocket is currently worth proactively
        keeping alive via self-refresh — polled periodically by WsManager
        (see WsManager._refresh_token_from_api,
        docs/gapless-arming-and-token-expiry.md).

        PLAYING always counts. PAUSED/STOPPED count too, but only for up to
        _WS_IDLE_TIMEOUT_SECONDS of continuous non-playing time — genuinely
        idle (nobody's about to resume) shouldn't keep refreshing a session
        forever in the background. Once that grace runs out we simply stop
        renewing and let the current token lapse naturally, exactly like
        before self-refresh existed; a resume from either side (the app, or
        the device's own controls — see _handle_unprompted_state_change)
        flips the state back to PLAYING and this starts counting again from
        scratch.
        """
        if self._state == PlaybackState.PLAYING:
            self._non_playing_since = None
            return True
        if self._non_playing_since is None:
            self._non_playing_since = time.monotonic()
        return time.monotonic() - self._non_playing_since < _WS_IDLE_TIMEOUT_SECONDS

    # =========================================================================
    # Seek Control
    # =========================================================================

    async def seek(self, position_ms: int) -> bool:
        """
        Seek to position in current track.

        Args:
            position_ms: Target position in milliseconds

        Returns:
            True if seek successful, False if rejected (no track loaded)
        """
        # Reject if no track loaded
        if self._state == PlaybackState.STOPPED or not self._current_track:
            logger.warning("Cannot seek: no track loaded")
            return False

        # Get track duration
        duration = self._current_duration_ms
        if duration <= 0:
            logger.warning("Cannot seek: unknown track duration")
            return False

        # Clamp position to valid range
        # Leave 1 second buffer at end to avoid triggering track end
        max_position = max(0, duration - 1000)
        clamped_position = max(0, min(position_ms, max_position))

        if clamped_position != position_ms:
            logger.debug(f"Seek position clamped: {position_ms}ms -> {clamped_position}ms")

        logger.info(f"Seeking to {clamped_position}ms (duration: {duration}ms)")

        try:
            # Send seek to backend
            await self.backend.seek(clamped_position)

            # Update position tracking
            self._set_position(clamped_position)

            # Send state update (immediate, not waiting for heartbeat)
            await self._send_state_update()

            logger.info(f"Seek complete to {clamped_position}ms")
            return True

        except Exception as e:
            logger.error(f"Seek failed: {e}", exc_info=True)
            return False

    async def seek_seconds(self, position_seconds: float) -> bool:
        """
        Seek to position in seconds (convenience method).

        Args:
            position_seconds: Target position in seconds

        Returns:
            True if seek successful
        """
        position_ms = int(position_seconds * 1000)
        return await self.seek(position_ms)

    # =========================================================================
    # Command Queue
    # =========================================================================

    def enqueue(
        self,
        coro_fn: Callable[[], Awaitable[None]],
        *,
        coalesce: bool = False,
        coalesce_group: str = _PLAYBACK_COALESCE_GROUP,
    ) -> None:
        """Queue an action to run on the single command consumer, in FIFO
        order, one at a time. Synchronous — callable from a sync context,
        e.g. a WsManager handler lambda (see speaker.py) or a backend
        callback that can't itself be async (see _on_track_ended).

        coalesce=True marks this as a command that supersedes an earlier
        one of the same coalesce_group still sitting in the queue (not
        yet started running) — that earlier one is dropped first, so a
        burst of these only ever runs its last one. The default group
        (_PLAYBACK_COALESCE_GROUP) covers playback-directing commands —
        e.g. an aggressive seek-bar scrub sending a SET_STATE per pixel,
        or a natural track end racing a user's explicit skip — matching
        what the old generation-based supersede mechanism did. An item
        already running can't be preempted outright (nothing interrupts
        an in-flight device round trip), but apply_remote_state()
        cooperatively checks _superseded_by_newer_command() at its own
        natural checkpoints and bails out early rather than committing
        more device work toward a target a newer command has already
        moved past — see there for why that matters (rapid swiping can
        otherwise visibly land on a track in the middle of the swipe
        instead of the final one, on a renderer slow enough that commands
        stop overlapping cleanly).

        A different coalesce_group (e.g. VOLUME_COALESCE_GROUP) gets the
        same flood protection without cross-cancelling an unrelated
        group's pending command — an incoming volume adjustment must
        never supersede a playback-directing one already queued, or vice
        versa. Plain FIFO commands (coalesce=False, the default) stay
        uncoalesced and are never affected by any of this.
        """
        if coalesce:
            prior = self._pending_coalescable.get(coalesce_group)
            if prior is not None:
                prior.cancelled = True
        item = _CommandQueueItem(coro_fn, coalesce, coalesce_group)
        if coalesce:
            self._pending_coalescable[coalesce_group] = item
        self._command_queue.put_nowait(item)

    def _superseded_by_newer_command(self) -> bool:
        """Whether a newer coalescible *playback-directing* command is
        already queued behind whatever's currently running — see
        enqueue(). apply_remote_state() checks this at its own
        checkpoints, between awaits, to bail out of driving the device
        toward a target that's already known to be stale rather than
        running it to completion first: the goal is always the *latest*
        desired state, not every intermediate one a burst of rapid
        commands happened to name along the way — the queued command
        that will run next does its own complete job regardless, so
        nothing here is lost by stopping early. Deliberately scoped to
        _PLAYBACK_COALESCE_GROUP only — an unrelated pending volume
        command must never make playback-directing code bail out."""
        return self._pending_coalescable.get(_PLAYBACK_COALESCE_GROUP) is not None

    async def _command_consumer_loop(self) -> None:
        """The only thing that ever runs a queued command — one at a time,
        strictly in FIFO order. Nothing else pulls from _command_queue, so
        this is what gives commands their mutual exclusion (see the
        _command_queue field comment).

        Calls task_done() for every item once handled (run, skipped as
        coalesced-away, or failed) — this is what makes
        ``await self._command_queue.join()`` a valid "wait for everything
        currently queued to finish" barrier (used in tests).

        Runs until cancelled (stop()'s own termination mechanism —
        self._command_task.cancel()), not gated on self._is_running: this
        loop can be started by _ensure_command_consumer_running() before
        start() has ever run (see there), and self._is_running only
        toggles True once the rest of start()'s work has also happened.
        """
        while True:
            try:
                item = await self._command_queue.get()
            except asyncio.CancelledError:
                break
            try:
                if item.coalesce:
                    # No longer pending: about to run, or already
                    # superseded by a later command in the same group
                    # (which has since taken this slot for itself — only
                    # clear it if it's still this exact item).
                    if self._pending_coalescable.get(item.coalesce_group) is item:
                        del self._pending_coalescable[item.coalesce_group]
                if item.cancelled:
                    logger.debug("Command queue: skipping a coalesced-away item")
                    continue
                if item.coalesce and not self._backend_attached:
                    # Hold this (and everything behind it) at the front of
                    # the queue until the backend reattaches — see
                    # set_backend_attached() — rather than dispatching into
                    # a per-call wait buried in the backend itself.
                    logger.info("Command queue: backend detached, waiting to dispatch")
                    try:
                        await asyncio.wait_for(
                            self._backend_attached_event.wait(),
                            timeout=_BACKEND_ATTACH_WAIT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Command queue: backend still detached after "
                            f"{_BACKEND_ATTACH_WAIT_SECONDS}s; dispatching anyway"
                        )
                self._processing_command = True
                try:
                    await item.coro_fn()
                finally:
                    self._processing_command = False
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Command queue item failed: {e}", exc_info=True)
            finally:
                self._command_queue.task_done()

    async def apply_remote_state(
        self,
        *,
        track_id: Optional[str],
        queue_item_id: Optional[int],
        position_ms: Optional[int],
        playing_state: Optional[int],
        context_uuid: Optional[bytes] = None,
        force_reload: bool = False,
    ) -> None:
        """Apply a full SET_STATE intent from the app atomically.

        A SET_STATE is a multi-step intent (load this track, seek here, then
        play/pause/stop). Each SET_STATE message is enqueued as its own
        command-queue item (see speaker.py's registration lambdas and
        Player.enqueue(..., coalesce=True)); running the whole sequence
        as one queue item, on the queue's single consumer, makes the
        newest SET_STATE win as a unit — an earlier one still waiting to
        run is dropped by coalescing before it can interleave with this
        one or play a stale track after a newer load already superseded it.

        Args:
            track_id: Target track id, or None if the message had no currentQueueItem.
            queue_item_id: Queue item id for the target track (if any).
            position_ms: Target position, or None if no currentPosition was sent.
            playing_state: Proto playing state (1=STOPPED, 2=PLAYING, 3=PAUSED),
                or None if the message had no playingState.
            context_uuid: Album/playlist context bytes for the target track, used
                for play reporting (listening history / scrobbles).
            force_reload: Load the track even if it is already the current one.
                Set for an explicit queue load (QUEUE_TRACKS_LOADED): a load
                starts a new queue generation, the stream URL the device
                holds for the playing track belongs to the previous one
                (the Sonos proxy refuses those, see QobuzQueue.generation)
                and the app expects a tapped track to restart anyway.
        """
        # Detect a stale session-restore snapshot (server replays an old
        # PAUSED position after a reconnect while we're still playing). Done
        # before any mutation so a replayed snapshot can't overwrite live
        # state (position or context) with its outdated values.
        if self._adoptable is not None and await self._adopt_playing_device():
            return  # what the server replays is older than what the device does

        stale = self._is_stale_pause_snapshot_locked(
            track_id, position_ms, playing_state
        ) or self._is_restore_of_live_playback_locked(track_id, playing_state)

        # Read from _intended_playing, not self._state — _load_track_locked
        # always drops self._state to STOPPED as a normal part of loading,
        # and a command superseded mid-load (see
        # _superseded_by_newer_command) bails out before ever reaching
        # _play_locked, leaving self._state sitting at that transient
        # STOPPED. _intended_playing survives that untouched (observed
        # directly, test1.log 2026-08-28: see its own docstring for the
        # exact failure this caused — a second rapid swipe correctly
        # skipped the stale first target, but then never auto-continued
        # playing because the skipped command had already flipped
        # self._state to STOPPED before bailing).
        was_playing = self._intended_playing

        # Load if a track is specified and differs from the loaded one.
        loaded_new_track = False
        if track_id is not None:
            cur = self._current_track
            if cur is None or cur.track_id != track_id or force_reload:
                if self._superseded_by_newer_command():
                    # A newer coalescible command is already queued right
                    # behind this one (see enqueue()) — e.g. a burst of
                    # rapid next/previous swipes outpacing how fast the
                    # renderer can actually be driven. Coalescing already
                    # guarantees only the *last* one will run, but that
                    # guarantee only covers commands still waiting — once
                    # this one started running, nothing stopped it from
                    # driving the device through a track the user has
                    # already swiped past by the time it gets there
                    # (observed directly: rapid swiping visibly landing on
                    # a track in the middle of the swipe instead of the
                    # final one). The desired end state is whatever the
                    # newest command wants, not every intermediate one on
                    # the way there — so don't bother loading (let alone
                    # the actual device round trip below) a track this
                    # message wants when we already know something newer
                    # superseded it; the queued command will do its own
                    # complete load once it runs.
                    logger.debug(
                        f"Skipping stale load of track {track_id} — a newer command "
                        "is already queued"
                    )
                    return
                logger.info(f"Loading new track: {track_id}")
                if not await self._load_track_locked(
                    queue_item_id or 0,
                    track_id,
                    context_uuid,
                    # A play/pause/stop for the new track is coming right
                    # after this load either way — either this message
                    # says so explicitly, or (see was_playing) we were
                    # already playing and will auto-continue below — so
                    # the interim state update _load_track_locked would
                    # otherwise send is redundant noise, not the genuine
                    # gap-filler it is for a load that might sit unplayed
                    # for a while.
                    expect_immediate_continuation=playing_state is not None or was_playing,
                ):
                    return
                loaded_new_track = True
            elif (
                not stale
                and queue_item_id
                and cur.queue_item_id
                and (cur.queue_item_id != queue_item_id)
            ):
                # Same track but a different known queue occurrence id. Adopt
                # it. Only split the play report when not currently playing:
                # a paused/stopped track re-armed from a different slot is a
                # distinct play, so end the prior report and let the
                # subsequent play report fresh. While PLAYING the audio is
                # continuous (e.g. a queue reorder reassigned the id), so it
                # stays one listen — splitting it would double-scrobble.
                cur.queue_item_id = queue_item_id
                if context_uuid is not None:
                    cur.context_uuid = context_uuid
                if self._state != PlaybackState.PLAYING:
                    await self._report_stopped()
            elif not stale:
                # Same play. Fill in a late queue item id if we never had a
                # real one, and adopt a changed/late context. Only overwrite
                # context with a real value so a context-less SET_STATE can't
                # wipe a known context.
                if queue_item_id and not cur.queue_item_id:
                    cur.queue_item_id = queue_item_id
                if context_uuid is not None and cur.context_uuid != context_uuid:
                    cur.context_uuid = context_uuid
                    # The play may already be active in the reporter (we
                    # return early from _play_locked while PLAYING), so
                    # re-sync its session or the end report keeps the old
                    # context.
                    if self._play_reporter:
                        self._play_reporter.update_context(
                            track_id=track_id,
                            context_uuid=self._format_context_uuid(context_uuid),
                        )

        # A newer coalescible command already queued behind this one (see
        # enqueue()) means the desired end state has already moved on —
        # don't bother seeking or committing a play/pause/stop for a
        # target this message wants when we already know it's stale; the
        # queued command will do its own complete job once it runs. Same
        # rapid-swiping symptom as the load check above (observed
        # directly), covering the seek/play device round trip specifically
        # — the single most expensive step, and the one most likely to
        # still be worth skipping even if the load above already ran.
        if self._superseded_by_newer_command():
            logger.debug("Skipping stale seek/play/pause/stop — a newer command is already queued")
            return

        # Position, then play/pause/stop — same order as the app expects.
        if position_ms is not None and not stale:
            await self.seek(position_ms)

        if playing_state is not None and not stale:
            # Proto: 1=STOPPED, 2=PLAYING, 3=PAUSED
            if playing_state == 2:
                await self._play_locked(position_ms or 0)
            elif playing_state == 3:
                await self._pause_locked(position_ms)
            elif playing_state == 1:
                await self._stop_playback_locked()
        elif loaded_new_track and was_playing and not stale:
            # No playingState in this message at all — same as every other
            # optional field here (currentPosition, nextQueueItem), absence
            # means "no change," not "stop." We were actively playing going
            # into this track change; nothing told us otherwise, so silently
            # dropping to STOPPED and waiting for a possibly much-delayed
            # (or, on some swipe-back gestures, never-arriving — observed
            # directly) separate playingState message is wrong. Continue
            # playing the new track instead — this is what "the app is
            # already playing and the user just navigated to a different
            # track" actually means.
            await self._play_locked(position_ms or 0)

    async def join_snapshot(self) -> Optional[JoinSnapshot]:
        """What to tell the server when the control channel (re)joins: the
        playback actually in progress, or None when nothing of ours is playing
        or paused (the join then says stopped)."""
        track = self._current_track
        if (
            track is None
            or not self._backend_engaged
            or self._state not in (PlaybackState.PLAYING, PlaybackState.PAUSED)
        ):
            return None
        index: Optional[int] = None
        order = await self.queue.play_order()
        if order:
            for i, entry in enumerate(order):
                if entry.queue_item_id == track.queue_item_id and entry.track_id == track.track_id:
                    index = i
                    break
        return JoinSnapshot(
            playing_state=int(self._state),
            position_ms=self.current_position_ms,
            duration_ms=self._current_duration_ms,
            queue_index=index,
        )

    def _is_restore_of_live_playback_locked(
        self, track_id: Optional[str], playing_state: Optional[int]
    ) -> bool:
        """A SET_STATE naming the track already playing, right after we
        (re)claimed active status, with a state of playing or stopped: the
        server replaying what it remembers from before the control channel
        dropped, not a command. Applying it would restart the track from the
        remembered position, or stop it, while it plays on undisturbed."""
        if playing_state not in (1, 2) or self._state != PlaybackState.PLAYING:
            return False
        cur = self._current_track
        if cur is None or track_id is None or cur.track_id != track_id:
            return False
        if time.monotonic() - self._last_activated_at >= _RECONNECT_STALE_GRACE_SECONDS:
            return False
        logger.info(
            "Ignoring SET_STATE (state %d) for the playing track right after rejoining: "
            "a session-restore replay; keeping playback.",
            playing_state,
        )
        return True

    def _is_stale_pause_snapshot_locked(
        self,
        track_id: Optional[str],
        position_ms: Optional[int],
        playing_state: Optional[int],
    ) -> bool:
        """Decide whether an inbound SET_STATE is a stale session-restore replay.

        Must be called from within ``apply_remote_state``'s own command-queue
        item so the live player state it reads is consistent with the
        surrounding mutation (nothing else can run concurrently with a
        queued item — see Player.enqueue()). Returns True when server says
        PAUSED, the renderer is actually still PLAYING the same track, and
        EITHER:
          - server position is more than _STALE_SNAPSHOT_THRESHOLD_MS behind
            the renderer's actual position (a slow reconnect — the app's
            cached position had a while to fall behind), or
          - we (re)claimed active status less than
            _RECONNECT_STALE_GRACE_SECONDS ago (a fast reconnect — barely
            any gap yet, but still the same replay).
        """
        if playing_state != 3:
            return False
        if self._state != PlaybackState.PLAYING:
            return False
        if position_ms is None:
            return False
        # A different target track is a real command (track change), not a replay.
        cur = self._current_track
        if track_id is not None and (cur is None or cur.track_id != track_id):
            return False

        actual_pos = self.current_position_ms
        gap_ms = actual_pos - position_ms
        just_reconnected = (
            time.monotonic() - self._last_activated_at < _RECONNECT_STALE_GRACE_SECONDS
        )
        if gap_ms <= _STALE_SNAPSHOT_THRESHOLD_MS and not just_reconnected:
            return False

        logger.info(
            "Ignoring stale SET_STATE: server says PAUSED at %dms, renderer is "
            "PLAYING at %dms (%.1fs ahead%s) on same track — likely a session-"
            "restore replay after WebSocket reconnect; keeping playback.",
            position_ms,
            actual_pos,
            gap_ms / 1000.0,
            ", just reconnected" if just_reconnected else "",
        )
        return True

    async def play(self, position_ms: int = 0) -> bool:
        """
        Start or resume playback.

        Args:
            position_ms: Optional starting position (only used when starting new playback)

        Returns:
            True if playback started/resumed successfully
        """
        return await self._play_locked(position_ms)

    async def _play_locked(self, position_ms: int = 0) -> bool:
        logger.debug(f"Play command, current state: {self._state}")
        # Set on entry, not just on success — see _intended_playing. A
        # later command in the same rapid-swipe burst needs to see "the
        # last real decision was play" even if this particular attempt
        # goes on to fail for unrelated reasons (e.g. an empty queue).
        self._intended_playing = True

        # Resume from pause — only when the backend actually has something to
        # resume. A "cold" pause (loaded but never started; see _pause_locked)
        # falls through to the fresh-start path below instead, using the
        # remembered position rather than backend.resume()-ing an idle,
        # nothing-loaded transport.
        if self._state == PlaybackState.PAUSED and self._backend_engaged:
            if not await self.backend.resume():
                # The renderer rejected the resume (e.g. SOAP failure) — stay
                # PAUSED rather than reporting PLAYING over a silent device, and
                # push the real PAUSED state so the app (which requested PLAY)
                # corrects immediately instead of waiting for the next heartbeat.
                logger.warning("Backend failed to resume; remaining paused")
                await self._send_state_update()
                return False
            self._state = PlaybackState.PLAYING
            self._position_timestamp_ms = int(time.time() * 1000)
            await self._send_state_update()
            # Resume continues an existing listen — pass the current position so
            # we don't re-report a start (and re-scrobble) on every pause/resume.
            await self._report_playing(self._position_value_ms)
            logger.info("Playback resumed")
            return True

        # Already playing — seek if position changed
        if self._state == PlaybackState.PLAYING:
            if position_ms > 0:
                logger.info(f"Scrubbing to {position_ms}ms while playing")
                await self.seek(position_ms)
            return True

        # A bare PLAY with nothing already loaded has no track to fall
        # back on — there's no queue to pull one from (see queue.py's
        # module docstring); every real play path loads a track directly
        # from SET_STATE (_load_track_locked) before this is ever reached.
        if not self._current_track:
            logger.warning("No track to play - nothing loaded")
            # The app flips its play button optimistically: correct it now.
            await self._send_state_update()
            return False

        # A cold pause (see above) already has the resume position recorded
        # in _position_value_ms — a plain PLAY with no explicit position
        # means "continue from there", not "start over at 0".
        if position_ms <= 0 and self._state == PlaybackState.PAUSED:
            position_ms = self._position_value_ms

        # Set starting position
        if position_ms > 0:
            self._position_value_ms = position_ms
            self._position_timestamp_ms = int(time.time() * 1000)

        # Start playback
        success = await self._start_playback(position_ms)

        # Seek if position > 0 and playback started
        if success and position_ms > 0:
            await self.backend.seek(position_ms)

        return success

    async def reload_current_track(self) -> bool:
        """
        Reload the current track (e.g. after quality change).

        Saves position, stops, clears cached URL, and restarts at saved position.

        Returns:
            True if track was reloaded successfully
        """
        return await self._reload_current_track_locked()

    async def _reload_current_track_locked(self) -> bool:
        if not self._current_track:
            return False

        if self._state not in (PlaybackState.PLAYING, PlaybackState.PAUSED):
            # Not actively playing — nothing to do; the next play() call
            # resolves fresh (backend.set_quality_override was already
            # updated by the caller, so the next resolve uses the new tier).
            return True

        was_playing = self._state == PlaybackState.PLAYING

        # Save current position
        saved_position = self.current_position_ms
        logger.info(
            f"Reloading track {self._current_track.track_id} at position {saved_position}ms"
        )

        # Stop current playback
        await self.backend.stop()

        if was_playing:
            # Restart playback from saved position
            success = await self._start_playback(saved_position)
            if success and saved_position > 0:
                await self.backend.seek(saved_position)
            return success
        else:
            # Was paused — just reset state, will re-fetch URL on next play
            self._state = PlaybackState.STOPPED
            self._position_value_ms = saved_position
            self._position_timestamp_ms = int(time.time() * 1000)
            # End the paused play's report: the next play re-fetches at the new
            # quality (new blob/format), so it must report as a fresh play rather
            # than resume this now-stale session.
            await self._report_stopped()
            return True

    async def pause(self) -> bool:
        """
        Pause playback.

        Returns:
            True if paused successfully
        """
        return await self._pause_locked()

    async def _pause_locked(self, position_ms: Optional[int] = None) -> bool:
        # See _intended_playing.
        self._intended_playing = False
        if (
            self._state == PlaybackState.STOPPED
            and self._current_track
            and not self._backend_engaged
        ):
            # A track was just loaded (e.g. we were made the active renderer
            # while it was already paused on another renderer/the phone) but
            # never actually started on this backend — there's nothing on the
            # device to pause. Just remember where to resume from; the next
            # play command starts it fresh at this position (_play_locked)
            # instead of restarting at 0.
            if position_ms is not None:
                self._position_value_ms = position_ms
                self._position_timestamp_ms = int(time.time() * 1000)
            self._state = PlaybackState.PAUSED
            await self._send_state_update()
            logger.info(
                f"Track loaded paused at {self._position_value_ms}ms (not started on backend)"
            )
            return True

        if self._state != PlaybackState.PLAYING:
            logger.debug(f"Cannot pause in state {self._state}")
            return False

        # Capture position before pausing
        self._position_value_ms = self.current_position_ms
        self._position_timestamp_ms = int(time.time() * 1000)

        if not await self.backend.pause():
            # The renderer rejected the pause (e.g. it already finished
            # playing on its own and has nothing left to pause — see
            # AudioBackend.pause()) — don't claim PAUSED over a command
            # the device didn't actually accept, the same reasoning the
            # resume path above already applies. Push the current best-
            # known state so the app corrects immediately rather than
            # waiting on the next heartbeat; if the device really has
            # already stopped, the poll loop's own natural-end detection
            # will catch up shortly and report that properly.
            logger.warning("Backend failed to pause; leaving state unchanged")
            await self._send_state_update()
            return False
        self._state = PlaybackState.PAUSED
        await self._send_state_update()
        # A pause does not end the listen — keeping the play-reporting session
        # open across pause/resume avoids emitting a streaming-end (and a
        # duplicate scrobble) on every pause. The session is closed on a real
        # stop, track change, or track end. note_paused stops the played-time
        # clock so paused time is excluded from the reported duration.
        self._report_paused()

        logger.info("Playback paused")
        return True

    async def stop_playback(self) -> None:
        """
        Stop playback completely.

        Resets position to 0 but keeps queue position.
        """
        await self._stop_playback_locked()

    async def _stop_playback_locked(self) -> None:
        # See _intended_playing.
        self._intended_playing = False
        # Clear gapless state — explicit stop
        self._clear_gapless_state()

        await self.backend.stop()

        self._state = PlaybackState.STOPPED
        self._backend_engaged = False
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)

        await self._send_state_update()
        await self._report_stopped()
        logger.info("Playback stopped")

    async def load_track(
        self,
        queue_item_id: int,
        track_id: str,
    ) -> bool:
        """
        Load a track without starting playback.

        This prepares the track (fetches URL and metadata) so it's ready
        to play immediately when play() is called.

        Args:
            queue_item_id: Queue item identifier
            track_id: Qobuz track ID

        Returns:
            True if track loaded successfully
        """
        return await self._load_track_locked(queue_item_id, track_id)

    async def _load_track_locked(
        self,
        queue_item_id: int,
        track_id: str,
        context_uuid: Optional[bytes] = None,
        expect_immediate_continuation: bool = False,
    ) -> bool:
        """Args:
        expect_immediate_continuation: True when the caller already knows
            a play/pause/stop is coming right after this load returns —
            either the message explicitly says so, or (see
            apply_remote_state's was_playing) we were already playing and
            will auto-continue. In that case the interim state update
            below is pure noise (a STOPPED flash immediately overwritten
            by whatever comes next) rather than the genuine gap-filler it
            is for a load that might sit unplayed for a while — see the
            comment at that update for why it exists at all. Observed
            directly, test1.log 2026-08-28: with the auto-continue fix
            making most swipes fast, this flash started showing up on
            nearly every single one instead of just the slow cases it was
            meant for.
        """
        logger.info(f"Loading track: track_id={track_id}, queue_item_id={queue_item_id}")

        # Stop current playback if playing
        if self._state in (PlaybackState.PLAYING, PlaybackState.PAUSED):
            with slow_step(logger, "stopping the device before a load"):
                await self.backend.stop()
            self._state = PlaybackState.STOPPED
            # End the outgoing track's play report now that it's being replaced.
            # Pause no longer ends the session, so a load-only track change (no
            # immediate play) would otherwise leave the previous play unreported.
            await self._report_stopped()

        # This is a fresh load — the backend has nothing of this track's yet.
        self._backend_engaged = False

        # Create track object. The context UUID identifies the album/playlist the
        # track is played from and is required for Qobuz listening history /
        # Last.fm scrobbles, so it must be carried onto the QueueTrack.
        self._current_track = QueueTrack(
            queue_item_id=queue_item_id,
            track_id=track_id,
            context_uuid=context_uuid,
        )

        # A load isn't always immediately followed by a play in the same
        # SET_STATE — the app can send them as two separate messages, and
        # (observed directly, test1.log 2026-08-28) the gap between them is
        # sometimes very long (13-17s), disproportionately on backward
        # navigation — apparently the app's own timing, not anything this
        # backend does (the delay sits entirely between this load finishing
        # and the next WS message arriving, before any device/audio I/O
        # starts). We can't shorten that wait, but leaving it completely
        # unreported is its own bug: StateReporter's heartbeat only
        # actively repeats position while PLAYING, so it stays silent
        # through this STOPPED/loading gap, leaving the app to keep
        # showing the outgoing track's stale PLAYING position advancing
        # with no sound. Reset the position (this is a different track
        # now, not a resume point) and the stale duration (that of the
        # *outgoing* track — see duration_ms — until the real fetch below
        # overwrites it) before reporting, so the app sees "loading queue
        # item Y" rather than a confusing mix of the new item id with the
        # old track's numbers.
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)
        self._current_duration_ms = 0
        if not expect_immediate_continuation:
            await self._send_state_update()

        # Pre-fetch display metadata via the queue's own cache — see
        # QobuzQueue.get_track_metadata. The streaming URL itself is no
        # longer pre-fetched here: it's resolved by the backend at actual
        # play() time (see _start_playback), since the format decision
        # needs device capabilities this method has no reason to know.
        try:
            with slow_step(logger, f"loading the metadata of track {track_id}"):
                meta = await self.queue.get_track_metadata(self._current_track)
            if meta:
                self._current_duration_ms = self._current_track.duration_ms

            logger.info(f"Track loaded: {track_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to load track {track_id}: {e}")
            return False

    async def play_track(
        self,
        queue_item_id: int,
        track_id: str,
        position_ms: int = 0,
        context_uuid: Optional[bytes] = None,
    ) -> bool:
        """
        Play a specific track from the queue.

        Args:
            queue_item_id: Queue item identifier
            track_id: Qobuz track ID
            position_ms: Starting position in milliseconds
            context_uuid: Album/playlist context bytes, used for play reporting.

        Returns:
            True if playback started successfully
        """
        return await self._play_track_locked(queue_item_id, track_id, position_ms, context_uuid)

    async def _play_track_locked(
        self,
        queue_item_id: int,
        track_id: str,
        position_ms: int = 0,
        context_uuid: Optional[bytes] = None,
    ) -> bool:
        # Clear gapless state — explicit track change
        self._clear_gapless_state()

        logger.info(
            f"Play track requested: track_id={track_id}, queue_item_id={queue_item_id}, pos={position_ms}ms"
        )

        # Load the track first
        if not await self._load_track_locked(queue_item_id, track_id, context_uuid):
            return False

        # Set starting position
        self._position_value_ms = position_ms
        self._position_timestamp_ms = int(time.time() * 1000)

        # Start playback
        success = await self._start_playback(position_ms)

        # Seek if position > 0 and playback started
        if success and position_ms > 0:
            await self.backend.seek(position_ms)

        return success

    async def set_loop_mode(self, mode: int) -> None:
        """
        Set loop/repeat mode.

        Args:
            mode: Protocol LoopMode - 0=UNKNOWN, 1=OFF, 2=REPEAT_ONE, 3=REPEAT_ALL
        """
        logger.debug(f"Set loop mode: {mode}")
        # Map protocol LoopMode to internal RepeatMode
        # Protocol: 0=UNKNOWN, 1=OFF, 2=REPEAT_ONE, 3=REPEAT_ALL
        # Internal: OFF, ONE, ALL
        mode_map = {
            0: RepeatMode.OFF,  # UNKNOWN -> OFF
            1: RepeatMode.OFF,  # OFF
            2: RepeatMode.ONE,  # REPEAT_ONE
            3: RepeatMode.ALL,  # REPEAT_ALL
        }
        repeat_mode = mode_map.get(mode, RepeatMode.OFF)
        previous = self._repeat_mode
        self._repeat_mode = repeat_mode
        await self.queue.set_repeat_mode(repeat_mode)
        if repeat_mode != previous:
            try:
                await self.backend.set_repeat_mode(repeat_mode.value)
            except Exception as e:
                logger.warning(f"Backend could not apply repeat mode {repeat_mode.value}: {e}")
        if (repeat_mode == RepeatMode.ONE) != (previous == RepeatMode.ONE):
            await self._rearm_gapless_for_repeat_change()

    async def _rearm_gapless_for_repeat_change(self) -> None:
        """Repeat-one was switched on or off. On: whatever is armed as the
        next track must go, so the device stops at the end of this one and
        _handle_track_ended restarts it (O11: a pre-armed next track
        otherwise just plays). Off: arm the server's next track again."""
        if not self.backend.supports_gapless:
            return
        async with self._gapless_arm_lock:
            self._transition_generation += 1
            self._gapless_armed = False
            self._pending_next_track = None
            await self.backend.clear_next_track()
            if self._state == PlaybackState.PLAYING:
                await self._prepare_next_track_locked()

    async def set_shuffle_mode(self, enabled: bool) -> None:
        """
        Set shuffle mode.

        Args:
            enabled: True to enable shuffle
        """
        logger.debug(f"Set shuffle mode: {enabled}")
        # There's no track list here to shuffle (see queue.py's module
        # docstring) and shuffle isn't part of the outbound state report
        # either — acknowledged but not implemented, same as autoplay below.

    async def set_autoplay_mode(self, enabled: bool) -> None:
        """
        Set autoplay mode.

        Args:
            enabled: True to enable autoplay (similar content when queue ends)
        """
        logger.debug(f"Set autoplay mode: {enabled}")
        # Autoplay is handled at queue level - just log for now
        # Full implementation would require fetching similar tracks

    # Deliberately no next_track()/previous_track() here. They used to
    # walk QobuzQueue's ordered track list (advance_to_next()/
    # go_to_previous()), which is gone — see queue.py's module docstring:
    # a Renderer session never receives the messages that would populate
    # one, and nothing in this app ever called these two methods either
    # (no inbound message type maps to them — next/previous navigation
    # arrives as just another SET_STATE naming the new track, apparently
    # decided client-side by the app itself). If a real caller ever needs
    # skip-forward/back again, it needs a real design behind it, not a
    # resurrection of the dead track-list machinery this removed.

    # =========================================================================
    # Internal Playback Management
    # =========================================================================

    async def _start_playback(self, start_position_ms: int = 0) -> bool:
        """
        Start playback of current track.

        Args:
            start_position_ms: Position the track begins at. A large value means
                we're adopting an in-progress track from the app (handoff), which
                suppresses our play-start report.

        Returns:
            True if playback started successfully
        """
        # See _intended_playing — set here too, not just in _play_locked:
        # play_track() and friends reach this directly without going
        # through _play_locked.
        self._intended_playing = True

        if not self._current_track:
            return False

        track = self._current_track
        logger.info(f"Starting playback: track {track.track_id}")

        # Set loading state
        self._state = PlaybackState.LOADING
        self._backend_engaged = False
        await self._send_state_update()

        try:
            # Display metadata via the queue's own cache — the streaming
            # URL/format itself is resolved by the backend inside play()
            # (it needs device capabilities this layer doesn't have).
            meta = await self.queue.get_track_metadata(track)

            # Build backend metadata (title/artist/album/artwork only —
            # sample_rate/bit_depth are decided by the backend now)
            backend_meta = BackendTrackMetadata(
                track_id=track.track_id,
                title=(
                    meta.get("title", f"Track {track.track_id}")
                    if meta
                    else f"Track {track.track_id}"
                ),
                artist=meta.get("artist", "") if meta else "",
                album=meta.get("album", "") if meta else "",
                duration_ms=track.duration_ms,
                artwork_url=meta.get("artwork_url", "") if meta else "",
                queue_item_id=track.queue_item_id,
            )

            # Start playback on backend — resolves the actual streaming
            # URL/format itself and hands back what it actually served.
            with slow_step(logger, f"starting track {track.track_id} on the device"):
                result = await self.backend.play(backend_meta)
            self._backend_engaged = True
            self.queue.set_track_stream_info(track, result.blob, result.format_id)

            # Log now playing with actual quality (0 = unavailable)
            self.metadata.log_now_playing_info(backend_meta, result.format_id or None)

            # Report file quality if callback is set. Gated on
            # is_active_renderer for the same reason _report_volume_change
            # and StateReporter are — a report sent while we're not active
            # (e.g. during the deactivation grace window, where playback
            # continues on purpose) just comes back rejected.
            if self._file_quality_report_callback and self._is_active_renderer:
                logger.debug(f"Track {track.track_id} actual_quality={result.format_id}")
                if result.format_id:
                    await self._file_quality_report_callback(result.format_id)
                else:
                    logger.debug(
                        f"No actual_quality for track {track.track_id}, skipping file quality report"
                    )

            # Update state. Report the start position, not 0 — the caller
            # seeks the backend right after, and reporting 0 first makes the
            # app's progress bar snap to 0:00 until the next heartbeat.
            self._state = PlaybackState.PLAYING
            self._current_duration_ms = track.duration_ms
            self._position_value_ms = start_position_ms
            self._position_timestamp_ms = int(time.time() * 1000)

            await self._send_state_update()
            await self._report_playing(start_position_ms)
            await self._sync_next_pointer()
            return True

        except Exception as e:
            logger.error(f"Failed to start playback: {e}", exc_info=True)
            self._state = PlaybackState.ERROR
            await self._send_state_update()
            return False

    # =========================================================================
    # Play Reporting (Qobuz listening history / Last.fm scrobbling)
    # =========================================================================

    async def _report_playing(self, start_position_ms: int = 0) -> None:
        """Tell the play reporter the current track is now playing.

        A start beyond the handoff threshold means the app already owns (and
        scrobbled) this play, so we track it locally but suppress our start
        report to avoid a duplicate scrobble.
        """
        if not self._play_reporter or not self._current_track:
            return
        track = self._current_track
        format_id = track.actual_quality
        blob = track.blob
        report_start = start_position_ms < _HANDOFF_POSITION_THRESHOLD_MS
        await self._play_reporter.note_playing(
            track_id=track.track_id,
            format_id=format_id,
            blob=blob,
            context_uuid=self._format_context_uuid(track.context_uuid),
            report_start=report_start,
        )

    async def _report_stopped(self) -> None:
        """Tell the play reporter playback stopped (pause/stop/track end)."""
        if not self._play_reporter:
            return
        await self._play_reporter.note_stopped()

    def _report_paused(self) -> None:
        """Tell the play reporter playback paused (session stays open)."""
        if self._play_reporter:
            self._play_reporter.note_paused()

    @staticmethod
    def _format_context_uuid(context_uuid: Optional[bytes]) -> Optional[str]:
        """Format the 16-byte queue context UUID as a canonical UUID string."""
        if not context_uuid:
            return None
        try:
            import uuid

            return str(uuid.UUID(bytes=bytes(context_uuid)))
        except (ValueError, TypeError):
            return None

    # =========================================================================
    # Position Tracking
    # =========================================================================

    @property
    def current_position_ms(self) -> int:
        """Get current playback position."""
        if self._state != PlaybackState.PLAYING:
            return self._position_value_ms

        # Calculate elapsed time since last position update
        now_ms = int(time.time() * 1000)
        elapsed = now_ms - self._position_timestamp_ms
        return self._position_value_ms + elapsed

    def _set_position(self, position_ms: int) -> None:
        """Update position tracking."""
        self._position_value_ms = position_ms
        self._position_timestamp_ms = int(time.time() * 1000)
        logger.debug(f"Position set: {position_ms}ms at ts={self._position_timestamp_ms}")

    # =========================================================================
    # Callbacks from Components
    # =========================================================================

    async def _get_track_metadata(self, track_id: str) -> Optional[dict]:
        """Callback for queue to get track metadata."""
        meta = await self.metadata.get_metadata(track_id)
        if meta:
            return meta.to_dict()
        return None

    def _on_track_ended(self) -> None:
        """Callback when backend reports track ended naturally."""
        logger.debug("Track ended callback")
        # Snapshot the track that ended synchronously, before this can
        # actually run — it may sit behind other queued items for a
        # moment, and the automatic repeat restart below is only valid
        # while this exact track is still the active one when it does.
        # coalesce=True: a user command (stop/next/play) enqueued before
        # this runs supersedes it, same as any other playback-directing
        # command — see enqueue().
        self.enqueue(
            functools.partial(self._handle_track_ended, self._current_track), coalesce=True
        )

    async def _handle_track_ended(self, ended_track: Optional[QueueTrack]) -> None:
        """Handle natural track end.

        ``ended_track`` is the track that was playing when the backend
        reported the end. Runs as one command-queue item — nothing else
        can interleave with it (see enqueue()), so by the time it reaches
        the repeat-one branch below, ``ended_track`` is guaranteed to
        still be ``self._current_track``: anything that could have
        changed it either already ran before this item started, or was
        itself coalesced away by this one being enqueued.
        """
        # Clear gapless state — prevents stale gapless callbacks from racing
        self._transition_generation += 1
        self._gapless_armed = False
        self._pending_next_track = None

        logger.info("Track ended naturally")

        # The track finished — report the completed play before advancing.
        await self._report_stopped()

        # Get queue state to check repeat mode
        queue_state = await self.queue.get_state()

        if queue_state.repeat_mode == RepeatMode.ONE and ended_track is not None:
            # Restart the current track from the beginning under repeat-one.
            await self._restart_current_track_locked()
            return

        # Try to get next track from command handler (SET_STATE nextQueueItem)
        if self._get_next_track_callback:
            next_track_info = self._get_next_track_callback()
            if next_track_info:
                logger.info(f"Auto-advancing to next track: {next_track_info['trackId']}")
                # Clear the stored next track info since we're using it
                if self._clear_next_track_callback:
                    self._clear_next_track_callback()

                # Load and play the next track
                await self.play_track(
                    queue_item_id=next_track_info["queueItemId"],
                    track_id=next_track_info["trackId"],
                    position_ms=0,
                    context_uuid=next_track_info.get("contextUuid"),
                )
                return

        # No next track available - stop playback. Must actually tell the
        # backend to stop, not just update our own bookkeeping — left as a
        # bare state update before, this never sent AVTransport.Stop, so a
        # Sonos device left on a non-NORMAL play mode (REPEAT_ONE/ALL, set
        # by anything else that last touched it — the Qobuz protocol layer
        # has no visibility into or control over this) would keep looping
        # the last track forever with nothing here ever telling it to stop.
        # See _intended_playing for why that's reset here too, same as
        # every other real stop path in this class (_stop_playback_locked).
        logger.info("No next track available - playback stopped")
        self._intended_playing = False
        await self.backend.stop()
        self._state = PlaybackState.STOPPED
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)
        # The end of the queue leaves the player on its first item, stopped,
        # the way players do (O2/O22): that is what the app shows, and what a
        # bare PLAY afterwards starts again.
        await self._land_on_first_entry(ended_track)
        await self._send_state_update()

    async def _land_on_first_entry(self, ended_track: Optional[QueueTrack]) -> None:
        """Make the first item of the play order the current track (stopped,
        position 0). With no queue to land on, nothing stays loaded."""
        order = await self.queue.play_order()
        if not order:
            self._current_track = None
            return
        first = order[0]
        meta = await self._get_track_metadata(first.track_id)
        self._current_track = QueueTrack(
            queue_item_id=first.queue_item_id,
            track_id=first.track_id,
            context_uuid=ended_track.context_uuid if ended_track else None,
            metadata=meta or {},
            duration_ms=meta.get("duration_ms", 0) if meta else 0,
        )
        self._current_duration_ms = self._current_track.duration_ms
        logger.info(
            f"Landed on the first queue item: track {first.track_id} "
            f"(queue item {first.queue_item_id})"
        )

    async def _restart_current_track_locked(self) -> None:
        """Restart the current track from the beginning (repeat-one).

        On natural end the backend has already transitioned to STOPPED, so a
        bare seek(0) leaves it silent — we must re-issue play. ``_start_playback``
        re-resolves the streaming URL itself (see backend.play/resolve_track,
        which already applies its own freshness check) and reports the new
        play; the completed one was already reported by the caller.
        """
        if not self._current_track:
            return
        self._set_position(0)
        await self._start_playback()

    def _on_playback_error(self, message: str) -> None:
        """Callback when backend reports playback error.

        Enqueued rather than a bare task (see enqueue()) — this used to
        mutate self._state directly from the backend's own poll-loop
        callback and spawn an unawaited task for the rest, both of which
        could run concurrently with whatever command-queue item was
        already in flight. Routing the whole effect through the queue
        makes Player's command consumer the only thing that ever mutates
        this state, same as every other backend notification."""
        logger.error(f"Playback error: {message}")
        self.enqueue(functools.partial(self._handle_playback_error, message))

    async def _handle_playback_error(self, message: str) -> None:
        """Apply a playback error reported by the backend — see _on_playback_error."""
        self._state = PlaybackState.ERROR
        await self._send_state_update()

    def _on_position_update(self, position_ms: int) -> None:
        """Callback when backend reports position update.

        Only fires while the backend is actually playing (see
        DLNABackend._poll_state_loop), which makes it a convenient,
        already-firing "still playing" tick to also retry arming gapless
        on — the previous track-info-changed event that would normally
        trigger it (on_next_track_info_changed) can arrive before playback
        actually starts, or an earlier arm attempt can fail transiently.
        Enqueued (plain FIFO, not coalesced — this is a low-stakes retry,
        never something a later command needs to supersede) rather than a
        bare task, so it runs properly ordered against everything else
        driving playback instead of racing it — see enqueue().
        """
        # A position that is not where the clock says it should be is a seek
        # made elsewhere (the Sonos app): tell the app now, not at the next
        # heartbeat (up to 5 s later).
        jumped = (
            self._state == PlaybackState.PLAYING
            and self._current_track is not None
            and abs(position_ms - self.current_position_ms) > _POSITION_JUMP_MS
        )
        self._set_position(position_ms)
        if jumped:
            self.enqueue(self._send_state_update)
        if not self._gapless_armed:
            self.enqueue(self._prepare_next_track_for_gapless)

    def _on_volume_update(self, volume: int) -> None:
        """Callback when the backend detects a volume change it didn't
        itself cause — the Sonos app, the device's own physical controls,
        another DLNA controller (see DLNABackend._poll_state_loop and
        VOLUME_POLL_GRACE_SECONDS for how a self-observation race against
        our own commanded changes is avoided). Updates bookkeeping
        immediately (sync, matching _on_position_update's own pattern);
        the actual report is async and gated on is_active_renderer (see
        _report_volume_change), so it's enqueued rather than run inline.
        """
        self._volume = volume
        self.enqueue(self._report_volume_change)

    # =========================================================================
    # Gapless Playback
    # =========================================================================

    def _clear_gapless_state(self) -> None:
        """Clear all gapless state and increment generation."""
        self._transition_generation += 1
        self._gapless_armed = False
        self._pending_next_track = None

    async def _prepare_next_track_for_gapless(self) -> None:
        """Prepare the next track for gapless playback on the backend.

        Serialized via `_gapless_arm_lock`: this has several independent
        legitimate callers (the retry on every position tick, a re-arm on
        next-track-info change, arming the next-next track right after a
        transition) — overlapping calls would each push the next track to
        the backend, and on Sonos that queues the track twice, making it
        play twice. Kept as its own lock rather than relying solely on
        the command-queue's own serialization (see enqueue()): unlike
        e.g. the old natural-track-end restart check, this method is
        meant to be safely callable from more than one place at once by
        design, not just from a single, always-queued entry point.
        """
        if not self.backend.supports_gapless or self._gapless_armed:
            return

        if not self._get_next_track_callback:
            return

        async with self._gapless_arm_lock:
            await self._prepare_next_track_locked()

    async def _prepare_next_track_locked(self) -> None:
        """Arm the next track. Caller must hold `_gapless_arm_lock`."""
        # Re-check after waiting on the lock — a concurrent arm may have won
        if self._gapless_armed or not self._get_next_track_callback:
            return

        # Not every caller went through _prepare_next_track_for_gapless's own
        # check (a changed next pointer arrives via on_next_track_info_changed):
        # a backend that does not arm tracks must never be asked to (one that
        # holds the whole queue on the device would append a duplicate of the
        # next entry to it).
        if not self.backend.supports_gapless:
            return

        if self._repeat_mode == RepeatMode.ONE:
            logger.debug("Gapless: repeat-one is on, not arming a next track")
            return

        next_track_info = self._get_next_track_callback()
        if not next_track_info:
            # Nothing to arm with. Two very different situations look
            # identical here: genuinely at the end of the queue, or the
            # Qobuz app's control WebSocket is down (token expired — see
            # docs/gapless-arming-and-token-expiry.md) so no nextQueueItem
            # has ever arrived to populate this. Warned once per gap
            # (cleared below the moment info reappears) rather than every
            # retry — this is reachable every _on_position_update tick
            # (STATE_POLL_INTERVAL_SECONDS) for as long as it stays
            # unresolved, which would otherwise spam the log.
            if not self._next_track_info_unavailable_warned:
                logger.warning(
                    "Gapless: no next-track info available to arm with — either "
                    "genuinely at the end of the queue, or the Qobuz app's "
                    "connection is down and hasn't told us what's next"
                )
                self._next_track_info_unavailable_warned = True
            return
        self._next_track_info_unavailable_warned = False

        track_id = next_track_info["trackId"]
        queue_item_id = next_track_info["queueItemId"]
        my_generation = self._transition_generation

        try:
            meta = await self._get_track_metadata(track_id)

            backend_meta = BackendTrackMetadata(
                track_id=track_id,
                title=meta.get("title", f"Track {track_id}") if meta else f"Track {track_id}",
                artist=meta.get("artist", "") if meta else "",
                album=meta.get("album", "") if meta else "",
                duration_ms=meta.get("duration_ms", 0) if meta else 0,
                artwork_url=meta.get("artwork_url", "") if meta else "",
                queue_item_id=queue_item_id,
            )

            # Resolves the URL/format itself — see backend.set_next_track.
            result = await self.backend.set_next_track(backend_meta, queue_item_id)

            # State changed while arming (skip, stop, queue edit) — the arm
            # is stale; undo it on the backend instead of marking it armed
            if my_generation != self._transition_generation:
                logger.debug(f"Gapless: discarding stale arm for track {track_id}")
                if result is not None:
                    await self.backend.clear_next_track()
                return

            if result is not None:
                self._pending_next_track = {
                    "trackId": track_id,
                    "queueItemId": queue_item_id,
                    "contextUuid": next_track_info.get("contextUuid"),
                    "metadata": meta,
                    "backend_meta": backend_meta,
                    "blob": result.blob,
                    "formatId": result.format_id,
                }
                self._gapless_armed = True
                logger.info(f"Gapless: armed next track {track_id}")
            else:
                logger.debug(f"Gapless: backend rejected next track {track_id}")

        except Exception as e:
            logger.warning(f"Gapless: failed to prepare next track: {e}")

    def _on_next_track_started(self) -> None:
        """Callback when backend reports gapless transition to next track.

        Enqueued (plain FIFO, not coalesced — see enqueue()) rather than a
        bare task: the transition already happened physically on the
        device by the time this fires, so unlike a still-undecided
        continuation (_on_track_ended's repeat-one/auto-advance), there's
        nothing here a later command should be allowed to preempt away —
        only bookkeeping (current track, position, the play-report swap)
        that must still happen, just correctly ordered against whatever
        else is driving playback instead of racing it.
        """
        logger.debug("Gapless: next track started callback from backend")
        self.enqueue(self._handle_gapless_transition)

    async def _handle_gapless_transition(self) -> None:
        """Handle a gapless transition to the next track."""
        # Capture generation to detect concurrent state changes (e.g. explicit skip/stop)
        my_generation = self._transition_generation

        if not self._pending_next_track or not self._gapless_armed:
            logger.warning("Gapless: transition callback but no pending track")
            return

        # Check generation hasn't changed (guards against concurrent transitions)
        if my_generation != self._transition_generation:
            logger.debug("Gapless: stale transition callback, ignoring")
            return

        next_info = self._pending_next_track
        track_id = next_info["trackId"]
        queue_item_id = next_info["queueItemId"]
        meta = next_info.get("metadata")

        logger.info(f"Gapless: transitioning to track {track_id}")

        # Update current track (no stop/start cycle)
        self._current_track = QueueTrack(
            queue_item_id=queue_item_id,
            track_id=track_id,
            context_uuid=next_info.get("contextUuid"),
            metadata=meta or {},
            duration_ms=meta.get("duration_ms", 0) if meta else 0,
            blob=next_info.get("blob", ""),
            actual_quality=next_info.get("formatId", 0),
        )
        self._current_duration_ms = self._current_track.duration_ms

        # Reset position
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)

        # Clear gapless state
        self._pending_next_track = None
        self._gapless_armed = False

        # Clear next track info from command handler
        if self._clear_next_track_callback:
            self._clear_next_track_callback()

        # Report file quality
        actual_quality = self._current_track.actual_quality
        backend_meta = next_info.get("backend_meta")
        if backend_meta:
            self.metadata.log_now_playing_info(backend_meta, actual_quality or None)
        if self._file_quality_report_callback and actual_quality and self._is_active_renderer:
            await self._file_quality_report_callback(actual_quality)

        # Report the play swap: ends the previous track, starts this one.
        await self._report_playing()

        # Send state update
        await self._send_state_update()

        # Try to arm the next next track
        await self._prepare_next_track_for_gapless()

    def _on_item_changed(
        self, track_id: str, queue_item_id: int, result: Optional[PlayResult]
    ) -> None:
        """Callback when the device moved on to another entry of the queue it
        holds (queue handover, see SonosQueueBackend). Enqueued like a gapless
        transition and for the same reason: it already happened on the device,
        so only bookkeeping is left, and it must be ordered against whatever
        else is driving playback."""
        self.enqueue(functools.partial(self._handle_item_changed, track_id, queue_item_id, result))

    async def _handle_item_changed(
        self, track_id: str, queue_item_id: int, result: Optional[PlayResult]
    ) -> None:
        """The device is playing another queue entry than the one we have as
        current: adopt it (no stop/start cycle) and report it, the way a
        gapless transition is reported."""
        previous = self._current_track
        if (
            previous is not None
            and previous.track_id == track_id
            and previous.queue_item_id == queue_item_id
        ):
            return  # already the current track (e.g. the device confirmed our own load)

        logger.info(f"Device moved on to track {track_id} (queue item {queue_item_id})")
        meta = await self._get_track_metadata(track_id)
        track = QueueTrack(
            queue_item_id=queue_item_id,
            track_id=track_id,
            # Entries of one queue load share their context (album/playlist);
            # the previous track's is the best available guess for this one.
            context_uuid=previous.context_uuid if previous else None,
            metadata=meta or {},
            duration_ms=meta.get("duration_ms", 0) if meta else 0,
        )
        if result is not None:
            self.queue.set_track_stream_info(track, result.blob, result.format_id)
        self._current_track = track
        self._current_duration_ms = track.duration_ms
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)

        # Anything armed or remembered as "next" belonged to the old position.
        self._transition_generation += 1
        self._gapless_armed = False
        self._pending_next_track = None
        if self._clear_next_track_callback:
            self._clear_next_track_callback()

        if self._state == PlaybackState.LOADING and self._intended_playing:
            # The device is between entries (TRANSITIONING) and was playing: the
            # app must not be told "stopped" for the instant it takes.
            self._state = PlaybackState.PLAYING

        if self._file_quality_report_callback and track.actual_quality and self._is_active_renderer:
            await self._file_quality_report_callback(track.actual_quality)
        await self._report_playing()
        await self._send_state_update()

    async def _sync_next_pointer(self) -> None:
        """Tell the backend where the server's next pointer is now (it may
        leave the queue into the autoplay list). Never raises."""
        try:
            info = self._get_next_track_callback() if self._get_next_track_callback else None
            await self.backend.next_pointer_changed(info)
        except Exception as e:
            logger.warning(f"Backend could not follow the next pointer: {e}")

    async def on_queue_changed(self) -> None:
        """The Qobuz queue's order or contents changed (a snapshot after an
        edit or shuffle toggle): let the backend bring the device's copy up to
        date. Never raises into the message handler that called it."""
        try:
            await self.backend.queue_changed()
        except Exception as e:
            logger.warning(f"Backend could not follow the queue change: {e}")

    async def on_next_track_info_changed(self) -> None:
        """Called when command handler reports the next track info has changed."""
        new_info = self._get_next_track_callback() if self._get_next_track_callback else None
        await self._sync_next_pointer()

        # The server resends the same next track in bursts — if it's already
        # armed, re-arming would queue a duplicate on the backend
        pending = self._pending_next_track
        if (
            self._gapless_armed
            and new_info is not None
            and pending is not None
            and new_info["trackId"] == pending["trackId"]
            and new_info["queueItemId"] == pending["queueItemId"]
            and new_info.get("contextUuid") == pending.get("contextUuid")
        ):
            logger.debug("Gapless: next track unchanged, keeping current arming")
            return

        logger.debug("Gapless: next track info changed, re-arming")

        async with self._gapless_arm_lock:
            # Clear current gapless arming
            self._transition_generation += 1
            self._gapless_armed = False
            self._pending_next_track = None
            await self.backend.clear_next_track()

            # Re-arm with new track if playing
            if self._state == PlaybackState.PLAYING:
                await self._prepare_next_track_locked()

    # =========================================================================
    # Backend Callback Handlers
    # =========================================================================

    def _on_state_change(self, state: PlaybackState) -> None:
        """Callback when the backend's own state changes.

        Fires both when acking a command we just issued (backend.play/
        pause/resume/stop, called from within a command-queue item — see
        enqueue()/_command_consumer_loop) and when the backend's own poll
        loop notices a change on its own (external pause, confirmed
        external stop while paused — see DLNABackend._poll_state_loop).
        The command case is already fully handled by the calling
        `_*_locked` method right after it awaits the backend call (sets
        `self._state`, sends the update, reports the play/pause/stop) —
        nothing to do here for that case, so it's skipped via
        `_processing_command` (True for as long as the consumer is
        running that item, which this synchronous check must still catch
        at the instant the backend call returns control to it — see that
        field's own docstring). Everything else — including the guard
        checks below, re-evaluated against *live* state rather than a
        snapshot taken here — is enqueued (see enqueue()) rather than a
        bare task, so it can never run concurrently with whatever the
        command consumer is doing: this used to mutate self._state
        directly from the backend's own poll-loop callback and spawn an
        unawaited task for the rest, either of which could interleave
        with an in-flight command-queue item.
        """
        if self._processing_command:
            return
        self.enqueue(functools.partial(self._handle_unprompted_state_change, state))

    async def _handle_unprompted_state_change(self, state: PlaybackState) -> None:
        """Side effects for a state change the backend noticed on its own —
        see _on_state_change. Re-checks the guard conditions against live
        state rather than trusting what was true when the callback fired —
        by the time this item's turn comes up, an intervening command may
        already have changed self._state."""
        if state == self._state:
            return
        if self._state == PlaybackState.PLAYING and state == PlaybackState.STOPPED:
            # Natural track end — on_track_ended (already wired) owns this
            # transition (repeat-one restart / auto-advance / stop
            # decision); avoid a premature STOPPED flicker before it runs.
            return
        logger.info(
            f"[{self.backend.name}] Unprompted backend state change: {self._state} -> {state}"
        )
        self._state = state
        if state == PlaybackState.PAUSED:
            # See _intended_playing — the device paused itself, so a later
            # swipe's auto-continue must not try to resume through it.
            self._intended_playing = False
            await self._send_state_update()
            # Stop the played-time clock so this pause is excluded from the
            # reported duration, like an app-driven pause.
            self._report_paused()
        elif state == PlaybackState.STOPPED:
            # See _intended_playing.
            self._intended_playing = False
            # Zero the position like _stop_playback_locked: a stale
            # pause-point position makes "previous" try a restart-seek on a
            # stopped renderer (a no-op) instead of navigating.
            self._position_value_ms = 0
            self._position_timestamp_ms = int(time.time() * 1000)
            await self._send_state_update()
            await self._report_stopped()
        elif state == PlaybackState.PLAYING:
            # See _intended_playing.
            self._intended_playing = True
            # Recovering from an unprompted PAUSED/STOPPED that turned out
            # to be transient — e.g. a device-state misread while Sonos
            # settles after a retarget (see PLAYBACK_START_GRACE_PERIOD_
            # SECONDS) that self-corrects on a later poll. self._state was
            # already flipped back a few lines up in this same method, and
            # _position_value_ms/_timestamp_ms already got a fresh reading
            # this same poll cycle before this task even runs (see
            # DLNABackend._poll_state_loop: the position notify always
            # follows the state notify within the same iteration once
            # new_state reads PLAYING) — but without this branch nothing
            # ever told the *app* the mismatch resolved, or resumed the
            # played-time clock _report_paused() had stopped. Left with
            # only the PAUSED/STOPPED branches above, the app was stuck
            # showing that stale report indefinitely — frozen position,
            # "stopped" on screen while the renderer was audibly still
            # playing — until the next heartbeat or an explicit command
            # forced a full resync (observed directly, test1.log,
            # 2026-08-28: fixed by pressing Play, which shouldn't have
            # been necessary).
            await self._send_state_update()
            await self._report_playing(self.current_position_ms)

    def _on_external_takeover(self) -> None:
        """Callback when the backend detects another source now driving
        this renderer instead of us (see
        AudioBackend.is_playing_our_content()). Enqueued rather than a
        bare task (see enqueue()) — see _on_playback_error for why."""
        logger.debug("External takeover callback")
        self.enqueue(self._handle_external_takeover)

    async def _handle_external_takeover(self) -> None:
        logger.info(
            f"[{self.backend.name}] External takeover detected on this renderer — "
            "treating as stopped"
        )
        self._state = PlaybackState.STOPPED
        self._position_value_ms = 0
        self._position_timestamp_ms = int(time.time() * 1000)
        await self._send_state_update()
        await self._report_stopped()
        # A plain STOPPED report leaves the app still believing it's
        # connected to this renderer — there's no protocol message to tell
        # it otherwise, so force a real reconnect instead, the closest
        # thing to "I just came back online" available.
        if self._hijack_detected_callback:
            try:
                await self._hijack_detected_callback("external takeover detected")
            except Exception as e:
                logger.warning(f"Hijack-detected callback failed: {e}")

    async def _send_state_update(self) -> None:
        """Send state update to app via StateReporter or callback."""
        # Prefer StateReporter if set
        if self._state_reporter:
            try:
                await self._state_reporter.report_now()
            except Exception as e:
                logger.error(f"Failed to send state update via reporter: {e}")
            return

        # Fall back to legacy callback
        if not self._state_update_callback:
            return

        try:
            await self._state_update_callback()
        except Exception as e:
            logger.error(f"Failed to send state update: {e}")

    # =========================================================================
    # State Access
    # =========================================================================

    @property
    def state(self) -> PlaybackState:
        """Get current playback state."""
        return self._state

    @property
    def is_active_renderer(self) -> bool:
        """Whether the Qobuz server currently considers this renderer the
        active playback target (see SrvrRndrSetActive in command_handler.py)."""
        return self._is_active_renderer

    @property
    def current_track(self) -> Optional[QueueTrack]:
        """Get current track."""
        return self._current_track

    @property
    def duration_ms(self) -> int:
        """Get current track duration."""
        return self._current_duration_ms

    def get_state_dict(self) -> dict:
        """Get current state as dictionary for reporting."""
        track = self._current_track
        queue_item_id = track.queue_item_id if track else 0

        return {
            "playingState": int(self._state),
            "bufferState": int(BufferStatus.OK),
            "currentPosition": {
                "timestamp": self._position_timestamp_ms,
                "value": self._position_value_ms,
            },
            "duration": self._current_duration_ms,
            "currentQueueItemId": queue_item_id,
        }

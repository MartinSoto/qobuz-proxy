"""
QobuzProxy Player.

Core playback controller that orchestrates queue, metadata, and audio backend.
"""

import asyncio
import functools
import logging
import time
from typing import Awaitable, Callable, Optional, TYPE_CHECKING

from qobuz_proxy.backends import (
    AudioBackend,
    BackendTrackMetadata,
    PlaybackState,
    BufferStatus,
)
from .queue import QobuzQueue, QueueTrack, RepeatMode
from .metadata import MetadataService

if TYPE_CHECKING:
    from .state_reporter import StateReporter
    from .play_reporter import PlayReporter

logger = logging.getLogger(__name__)

# Threshold for restart vs previous track (milliseconds)
PREVIOUS_TRACK_THRESHOLD_MS = 3000

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
_STALE_SNAPSHOT_THRESHOLD_MS = 5000

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


class _CommandQueueItem:
    """One entry in Player._command_queue — see Player.enqueue().

    Identity-compared (no custom __eq__/__hash__), which is exactly what
    enqueue()'s coalescing sweep and the consumer's "was this coalesced
    away" check need: two items are never "equal" just because they
    happen to wrap the same bound method.
    """

    __slots__ = ("coro_fn", "coalesce", "cancelled")

    def __init__(self, coro_fn: Callable[[], Awaitable[None]], coalesce: bool) -> None:
        self.coro_fn = coro_fn
        self.coalesce = coalesce
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
        # Pending (not yet started) coalesce=True items, in enqueue order —
        # what enqueue() scans/cancels when a new one supersedes them.
        self._pending_coalescable: list[_CommandQueueItem] = []
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

        # Callback for next track info changes (from command handler)
        self._on_next_track_changed_callback: Optional[Callable[[], None]] = None

        self._is_running: bool = False

        # Wire up queue callbacks to metadata service
        self.queue.set_url_callback(self._get_track_url)
        self.queue.set_metadata_callback(self._get_track_metadata)

        # Wire up backend callbacks
        self.backend.on_track_ended(self._on_track_ended)
        self.backend.on_playback_error(self._on_playback_error)
        self.backend.on_position_update(self._on_position_update)
        self.backend.on_next_track_started(self._on_next_track_started)
        self.backend.on_state_change(self._on_state_change)
        self.backend.on_external_takeover(self._on_external_takeover)

        logger.info("QobuzPlayer initialized")

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Start the player and its components."""
        if self._is_running:
            return

        self._is_running = True

        # Start queue preloading
        await self.queue.start()

        # Connect backend
        if not self.backend.is_connected():
            await self.backend.connect()

        # Start the command queue's single consumer
        self._command_task = asyncio.create_task(self._command_consumer_loop())

        logger.info("Player started")

    async def stop(self, send_device_stop: bool = True) -> None:
        """Stop the player and clean up.

        Args:
            send_device_stop: Forwarded to backend.disconnect() — set False
                when the device shouldn't actually be told to stop (see
                AudioBackend.disconnect()).
        """
        self._is_running = False

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

        # Stop queue
        await self.queue.stop()

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

    def set_active_renderer(self, active: bool) -> None:
        """Record whether the Qobuz server currently considers this
        renderer the active playback target (see SrvrRndrSetActive).

        Also relayed to the backend (see AudioBackend.set_active) — in a
        Sonos auto-discovery household every discovered room polls its own
        device continuously regardless of whether Qobuz is driving it, and
        a backend that reacts to what it sees there (hijack detection) has
        no way to know "active" is a Qobuz Connect concept it can't see
        for itself without this."""
        self._is_active_renderer = active
        self.backend.set_active(active)

    async def set_backend_attached(self, attached: bool) -> None:
        """Record whether the backend is currently reachable — called by
        Speaker.detach()/retarget() around a Sonos group_id going pending
        (see SonosDiscoveryManager's pending state). Going False also
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
            # A retarget just succeeded — this is retarget()'s own sole
            # call site (see Speaker.retarget()). The backend clears its
            # own next-track bookkeeping whenever it actually retargets to
            # a different device (DLNABackend.retarget() — the physical
            # device's own queue can't be assumed to carry over what we'd
            # armed on the old one), but nothing told *this* flag its own
            # armed state might now be stale. Left alone, it stayed True
            # forever (observed directly: a gapless transition Sonos
            # itself carried through anyway on the new device went
            # undetected as gapless — the backend had nothing armed to
            # compare against — and read as an external takeover instead),
            # permanently blocking the ordinary per-position-tick re-arm
            # retry (see _on_position_update) from ever running again.
            # Cleared here unconditionally, not just on a detached->
            # attached transition, since Speaker.retarget() calls this on
            # every successful retarget regardless of whether
            # backend_attached ever actually went False — costs at most
            # one harmless re-arm attempt when nothing actually changed
            # (the backend's own set_next_track dedups an identical
            # still-armed URL).
            self._clear_gapless_state()
            return
        self._backend_attached_event.clear()
        if was_attached and self._state == PlaybackState.PLAYING:
            self._state = PlaybackState.LOADING
            await self._send_state_update()

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

        # Apply to backend
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
        """Send volume change notification to app."""
        if not self._volume_report_callback:
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
        """
        try:
            await self.backend.stop()
        except Exception as e:
            logger.warning(f"Failed to stop device on activation: {e}")

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

    def enqueue(self, coro_fn: Callable[[], Awaitable[None]], *, coalesce: bool = False) -> None:
        """Queue an action to run on the single command consumer, in FIFO
        order, one at a time. Synchronous — callable from a sync context,
        e.g. a WsManager handler lambda (see speaker.py) or a backend
        callback that can't itself be async (see _on_track_ended).

        coalesce=True marks this as a playback-directing command: any
        earlier coalesce=True item still sitting in the queue (not yet
        started running) is dropped first, so a burst of these — e.g. an
        aggressive seek-bar scrub sending a SET_STATE per pixel, or a
        natural track end racing a user's explicit skip — still only ever
        runs its last one, matching what the old generation-based
        supersede mechanism did. An item already running can't be
        coalesced away (nothing interleaves with it either way, so there's
        nothing to gain by trying). Volume/queue commands (coalesce=False,
        the default) were never covered by that and stay plain FIFO.
        """
        if coalesce:
            for pending in self._pending_coalescable:
                pending.cancelled = True
            self._pending_coalescable.clear()
        item = _CommandQueueItem(coro_fn, coalesce)
        if coalesce:
            self._pending_coalescable.append(item)
        self._command_queue.put_nowait(item)

    async def _command_consumer_loop(self) -> None:
        """The only thing that ever runs a queued command — one at a time,
        strictly in FIFO order. Nothing else pulls from _command_queue, so
        this is what gives commands their mutual exclusion (see the
        _command_queue field comment).

        Calls task_done() for every item once handled (run, skipped as
        coalesced-away, or failed) — this is what makes
        ``await self._command_queue.join()`` a valid "wait for everything
        currently queued to finish" barrier (used in tests).
        """
        while self._is_running:
            try:
                item = await self._command_queue.get()
            except asyncio.CancelledError:
                break
            try:
                if item.coalesce:
                    # No longer pending: about to run, or already dropped
                    # by a later coalescing command (already removed from
                    # this list in that case — remove() would raise).
                    try:
                        self._pending_coalescable.remove(item)
                    except ValueError:
                        pass
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
        """
        # Detect a stale session-restore snapshot (server replays an old
        # PAUSED position after a reconnect while we're still playing). Done
        # before any mutation so a replayed snapshot can't overwrite live
        # state (position or context) with its outdated values.
        stale = self._is_stale_pause_snapshot_locked(track_id, position_ms, playing_state)

        # Load if a track is specified and differs from the loaded one.
        if track_id is not None:
            cur = self._current_track
            if cur is None or cur.track_id != track_id:
                logger.info(f"Loading new track: {track_id}")
                if not await self._load_track_locked(queue_item_id or 0, track_id, context_uuid):
                    return
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
        queued item — see Player.enqueue()). Returns True when ALL of:
          - server says PAUSED
          - renderer is still PLAYING
          - it's the same track the renderer is on
          - server position is more than _STALE_SNAPSHOT_THRESHOLD_MS behind the
            renderer's actual position
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
        if gap_ms <= _STALE_SNAPSHOT_THRESHOLD_MS:
            return False

        logger.info(
            "Ignoring stale SET_STATE: server says PAUSED at %dms, renderer is "
            "PLAYING at %dms (%.1fs ahead) on same track — likely a session-"
            "restore replay after WebSocket reconnect; keeping playback.",
            position_ms,
            actual_pos,
            gap_ms / 1000.0,
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

        # Get track to play (if not already loaded)
        if not self._current_track:
            track = await self.queue.get_current_track()
            if not track:
                track = await self.queue.advance_to_next()
            if not track:
                logger.warning("No track to play - queue empty")
                return False
            self._current_track = track

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
            # Not actively playing — just clear cached URL so next play uses new quality
            self._current_track.streaming_url = None
            return True

        was_playing = self._state == PlaybackState.PLAYING

        # Save current position
        saved_position = self.current_position_ms
        logger.info(
            f"Reloading track {self._current_track.track_id} at position {saved_position}ms"
        )

        # Stop current playback
        await self.backend.stop()

        # Clear cached streaming URL so it's re-fetched at new quality
        self._current_track.streaming_url = None

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

        await self.backend.pause()
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
    ) -> bool:
        logger.info(f"Loading track: track_id={track_id}, queue_item_id={queue_item_id}")

        # Stop current playback if playing
        if self._state in (PlaybackState.PLAYING, PlaybackState.PAUSED):
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

        # Pre-fetch URL and metadata via the queue's own cache — see
        # QobuzQueue.get_track_url/get_track_metadata.
        try:
            url = await self.queue.get_track_url(self._current_track)
            if not url:
                logger.error(f"Failed to get URL for track {track_id}")
                return False

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
        await self.queue.set_repeat_mode(repeat_mode)

    async def set_shuffle_mode(self, enabled: bool) -> None:
        """
        Set shuffle mode.

        Args:
            enabled: True to enable shuffle
        """
        logger.debug(f"Set shuffle mode: {enabled}")
        await self.queue.set_shuffle(enabled)

    async def set_autoplay_mode(self, enabled: bool) -> None:
        """
        Set autoplay mode.

        Args:
            enabled: True to enable autoplay (similar content when queue ends)
        """
        logger.debug(f"Set autoplay mode: {enabled}")
        # Autoplay is handled at queue level - just log for now
        # Full implementation would require fetching similar tracks

    async def next_track(self) -> bool:
        """
        Skip to next track.

        Returns:
            True if advanced to next track, False if at end
        """
        return await self._next_track_locked()

    async def _next_track_locked(self) -> bool:
        # Clear gapless state — explicit skip
        self._clear_gapless_state()

        logger.debug("Next track command")

        # Stop current playback
        if self._state in (PlaybackState.PLAYING, PlaybackState.PAUSED):
            await self.backend.stop()

        # Get next track from queue
        track = await self.queue.advance_to_next()

        if not track:
            # End of queue
            self._state = PlaybackState.STOPPED
            self._current_track = None
            self._position_value_ms = 0
            await self._send_state_update()
            # Report the finished play so the last track lands in listening
            # history / is scrobbled, and the lingering session is closed (an
            # open session would inflate the next play's reported duration).
            await self._report_stopped()
            logger.info("End of queue - playback stopped")
            return False

        # Start playing next track
        self._current_track = track
        await self._start_playback()
        return True

    async def previous_track(self) -> bool:
        """
        Go to previous track or restart current track.

        - If position > 3 seconds: Restart current track
        - If position <= 3 seconds: Go to previous track

        Returns:
            True if action taken successfully
        """
        return await self._previous_track_locked()

    async def _previous_track_locked(self) -> bool:
        # Clear gapless state — explicit navigation
        self._clear_gapless_state()

        logger.debug("Previous track command")

        current_pos = self.current_position_ms

        # Restart if past threshold
        if current_pos > PREVIOUS_TRACK_THRESHOLD_MS:
            logger.debug(
                f"Restarting track (position {current_pos}ms > {PREVIOUS_TRACK_THRESHOLD_MS}ms)"
            )
            await self.backend.seek(0)
            self._position_value_ms = 0
            self._position_timestamp_ms = int(time.time() * 1000)
            await self._send_state_update()
            if self._state == PlaybackState.PAUSED:
                # Restarting a paused track ends the prior listen so the next
                # resume reports the replay as a fresh play instead of merging
                # into the open (paused) session.
                await self._report_stopped()
            return True

        # Stop current playback
        if self._state in (PlaybackState.PLAYING, PlaybackState.PAUSED):
            await self.backend.stop()

        # Get previous track from queue
        track = await self.queue.go_to_previous()

        if not track:
            logger.warning("No previous track")
            return False

        # Start playing previous track
        self._current_track = track
        await self._start_playback()
        return True

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
        if not self._current_track:
            return False

        track = self._current_track
        logger.info(f"Starting playback: track {track.track_id}")

        # Set loading state
        self._state = PlaybackState.LOADING
        self._backend_engaged = False
        await self._send_state_update()

        try:
            # Get streaming URL and metadata via the queue's own cache —
            # a cached URL past its TTL is treated as absent (a track
            # loaded PAUSED and played later than the URL lifetime must
            # not start from an expired URL); see QobuzQueue.get_track_url.
            url = await self.queue.get_track_url(track)
            if not url:
                logger.error(f"Failed to get URL for track {track.track_id}")
                self._state = PlaybackState.ERROR
                await self._send_state_update()
                return False

            meta = await self.queue.get_track_metadata(track)

            # Get actual quality and format info from cache (set during URL fetch)
            actual_quality, sample_rate, bit_depth = self.metadata.get_track_format(track.track_id)

            # Build backend metadata
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
                sample_rate=sample_rate,
                bit_depth=bit_depth,
            )

            # Log now playing with actual quality (0 = cache miss, fall back to max quality)
            self.metadata.log_now_playing_info(backend_meta, actual_quality or None)

            # Report file quality if callback is set
            if self._file_quality_report_callback:
                logger.debug(f"Track {track.track_id} actual_quality={actual_quality}")
                if actual_quality:
                    await self._file_quality_report_callback(actual_quality)
                else:
                    logger.debug(
                        f"No actual_quality for track {track.track_id}, skipping file quality report"
                    )

            # Start playback on backend
            await self.backend.play(url, backend_meta)
            self._backend_engaged = True

            # Update state. Report the start position, not 0 — the caller
            # seeks the backend right after, and reporting 0 first makes the
            # app's progress bar snap to 0:00 until the next heartbeat.
            self._state = PlaybackState.PLAYING
            self._current_duration_ms = track.duration_ms
            self._position_value_ms = start_position_ms
            self._position_timestamp_ms = int(time.time() * 1000)

            await self._send_state_update()
            await self._report_playing(start_position_ms)
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
        format_id = self.metadata.get_track_actual_quality(track.track_id) or 0
        blob = self.metadata.get_track_blob(track.track_id) or ""
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

    async def _get_track_url(self, track_id: str) -> Optional[str]:
        """Callback for queue to get streaming URL."""
        return await self.metadata.get_streaming_url(track_id)

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

        # No next track available - stop playback
        logger.info("No next track available - playback stopped")
        self._state = PlaybackState.STOPPED
        self._current_track = None
        self._position_value_ms = 0
        await self._send_state_update()

    async def _restart_current_track_locked(self) -> None:
        """Restart the current track from the beginning (repeat-one).

        On natural end the backend has already transitioned to STOPPED, so a
        bare seek(0) leaves it silent — we must re-issue play. The cached URL
        is cleared so a fresh, non-expired streaming link is fetched for the
        repeat. ``_start_playback`` reports the new play; the completed one was
        already reported by the caller.
        """
        if not self._current_track:
            return
        self._current_track.streaming_url = None
        self._set_position(0)
        await self._start_playback()

    def _on_playback_error(self, message: str) -> None:
        """Callback when backend reports playback error."""
        logger.error(f"Playback error: {message}")
        self._state = PlaybackState.ERROR
        asyncio.create_task(self._send_state_update())

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
        self._set_position(position_ms)
        if not self._gapless_armed:
            self.enqueue(self._prepare_next_track_for_gapless)

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

        next_track_info = self._get_next_track_callback()
        if not next_track_info:
            return

        track_id = next_track_info["trackId"]
        queue_item_id = next_track_info["queueItemId"]
        my_generation = self._transition_generation

        try:
            # Fetch URL and metadata
            url = await self._get_track_url(track_id)
            if not url:
                logger.debug(f"Gapless: failed to get URL for next track {track_id}")
                return

            meta = await self._get_track_metadata(track_id)

            _, sample_rate, bit_depth = self.metadata.get_track_format(track_id)
            backend_meta = BackendTrackMetadata(
                track_id=track_id,
                title=meta.get("title", f"Track {track_id}") if meta else f"Track {track_id}",
                artist=meta.get("artist", "") if meta else "",
                album=meta.get("album", "") if meta else "",
                duration_ms=meta.get("duration_ms", 0) if meta else 0,
                artwork_url=meta.get("artwork_url", "") if meta else "",
                sample_rate=sample_rate,
                bit_depth=bit_depth,
            )

            success = await self.backend.set_next_track(url, backend_meta, queue_item_id)

            # State changed while arming (skip, stop, queue edit) — the arm
            # is stale; undo it on the backend instead of marking it armed
            if my_generation != self._transition_generation:
                logger.debug(f"Gapless: discarding stale arm for track {track_id}")
                if success:
                    await self.backend.clear_next_track()
                return

            if success:
                self._pending_next_track = {
                    "trackId": track_id,
                    "queueItemId": queue_item_id,
                    "contextUuid": next_track_info.get("contextUuid"),
                    "url": url,
                    "metadata": meta,
                    "backend_meta": backend_meta,
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
            streaming_url=next_info.get("url"),
            metadata=meta or {},
            duration_ms=meta.get("duration_ms", 0) if meta else 0,
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
        actual_quality = self.metadata.get_track_actual_quality(track_id)
        backend_meta = next_info.get("backend_meta")
        if backend_meta:
            self.metadata.log_now_playing_info(backend_meta, actual_quality)
        if self._file_quality_report_callback and actual_quality:
            await self._file_quality_report_callback(actual_quality)

        # Report the play swap: ends the previous track, starts this one.
        await self._report_playing()

        # Send state update
        await self._send_state_update()

        # Try to arm the next next track
        await self._prepare_next_track_for_gapless()

    async def on_next_track_info_changed(self) -> None:
        """Called when command handler reports the next track info has changed."""
        new_info = self._get_next_track_callback() if self._get_next_track_callback else None

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
        running that item). This handles only the unprompted one.
        """
        if self._processing_command:
            return
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
        asyncio.create_task(self._handle_unprompted_state_change(state))

    async def _handle_unprompted_state_change(self, state: PlaybackState) -> None:
        """Side effects for a state change the backend noticed on its own —
        see _on_state_change."""
        if state == PlaybackState.PAUSED:
            await self._send_state_update()
            # Stop the played-time clock so this pause is excluded from the
            # reported duration, like an app-driven pause.
            self._report_paused()
        elif state == PlaybackState.STOPPED:
            # Zero the position like _stop_playback_locked: a stale
            # pause-point position makes "previous" try a restart-seek on a
            # stopped renderer (a no-op) instead of navigating.
            self._position_value_ms = 0
            self._position_timestamp_ms = int(time.time() * 1000)
            await self._send_state_update()
            await self._report_stopped()

    def _on_external_takeover(self) -> None:
        """Callback when the backend detects another source now driving
        this renderer instead of us (see
        AudioBackend.is_playing_our_content())."""
        logger.debug("External takeover callback")
        asyncio.create_task(self._handle_external_takeover())

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

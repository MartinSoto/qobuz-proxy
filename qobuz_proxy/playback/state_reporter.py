"""
State reporter for Qobuz Connect protocol.

Handles periodic and event-driven state updates to the Qobuz app.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional, TYPE_CHECKING

from qobuz_proxy.backends import PlaybackState, BufferStatus

if TYPE_CHECKING:
    from .player import QobuzPlayer
    from .queue import QobuzQueue

logger = logging.getLogger(__name__)

# Heartbeat watchdog timeout: a report is sent (while PLAYING) only if
# nothing else — a command-driven or backend-event-driven send, both of
# which already funnel through _send_state_update() — has gone out for
# this long. Not a fixed send cadence any more; see _heartbeat_loop.
STATE_HEARTBEAT_TIMEOUT_SECONDS = 5.0


@dataclass
class PlaybackStateReport:
    """
    Complete playback state for reporting to Qobuz app.

    All fields required by the QueueRendererState protobuf message.
    """

    # Playback state
    playing_state: PlaybackState
    buffer_state: BufferStatus

    # Position tracking
    position_timestamp_ms: int  # When position was recorded
    position_value_ms: int  # Position value at timestamp
    duration_ms: int

    # Queue info
    current_queue_item_id: int
    queue_version_major: int
    queue_version_minor: int

    def to_proto_dict(self) -> dict:
        """Convert to dictionary matching protobuf structure."""
        # Protocol only supports: 1=STOPPED, 2=PLAYING, 3=PAUSED
        # Map internal LOADING (4) and ERROR (5) to valid protocol values
        playing_state = self.playing_state
        if playing_state == PlaybackState.LOADING:
            playing_state = PlaybackState.STOPPED  # Loading shown as stopped
        elif playing_state == PlaybackState.ERROR:
            playing_state = PlaybackState.STOPPED  # Error shown as stopped

        return {
            "playingState": int(playing_state),
            "bufferState": int(self.buffer_state),
            "currentPosition": {
                "timestamp": self.position_timestamp_ms,
                "value": self.position_value_ms,
            },
            "duration": self.duration_ms,
            "currentQueueItemId": self.current_queue_item_id,
            "queueVersion": {
                "major": self.queue_version_major,
                "minor": self.queue_version_minor,
            },
        }


# Type alias for send callback
SendCallback = Callable[["PlaybackStateReport"], "asyncio.Future[None]"]


class StateReporter:
    """
    Manages state reporting to Qobuz app.

    Sends:
    - Immediate updates on state changes — command-driven (play/pause/
      stop/seek/error, via report_now()) and backend-event-driven (an
      external pause, a confirmed external stop, a hijack — Player calls
      report_now() for these too, right when its own callback handlers
      react to them).
    - A heartbeat, but only as a watchdog: if nothing above has actually
      fired for STATE_HEARTBEAT_TIMEOUT_SECONDS while PLAYING, this sends
      one itself so position doesn't go stale for long even without a
      discrete event.

    Does NOT handle volume (separate RndrSrvrVolumeChanged message).
    """

    def __init__(
        self,
        player: "QobuzPlayer",
        queue: "QobuzQueue",
        send_callback: SendCallback,
    ):
        """
        Initialize state reporter.

        Args:
            player: Player instance for state access
            queue: Queue instance for queue state
            send_callback: Async callback to send state update
        """
        self._player = player
        self._queue = queue
        self._send_callback = send_callback

        self._is_running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

        # When the last report actually went out (monotonic clock) — the
        # single thing both event-triggered and heartbeat-triggered sends
        # update, and what the watchdog measures its idle window against.
        self._last_report_at: float = 0.0
        # Set by _send_state_update() on every successful send, so the
        # watchdog loop can wake early and recompute its deadline instead
        # of also firing redundantly right after an event-triggered send.
        self._report_sent_event: asyncio.Event = asyncio.Event()

    async def start(self) -> None:
        """Start the state reporter heartbeat."""
        if self._is_running:
            return

        self._is_running = True
        # Start the idle window from now, not from the zero value set at
        # construction — otherwise the watchdog's very first check would
        # see itself already past deadline and fire immediately.
        self._last_report_at = time.monotonic()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("StateReporter started")

    async def stop(self) -> None:
        """Stop the state reporter."""
        self._is_running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        logger.info("StateReporter stopped")

    async def report_now(self) -> None:
        """
        Send immediate state update.

        Call this when state changes:
        - Play/pause/stop
        - Track change
        - Seek complete
        - Shuffle/repeat mode change
        - Error occurred
        """
        await self._send_state_update()

    async def _heartbeat_loop(self) -> None:
        """Heartbeat watchdog.

        Not the primary relay mechanism any more — report_now() (called by
        Player on every command-driven and backend-event-driven change) is.
        This only fires a report itself when nothing has been sent —
        by either path — for STATE_HEARTBEAT_TIMEOUT_SECONDS: it sleeps
        until _last_report_at's deadline, woken early (and its own wait
        restarted from the fresh deadline) whenever _send_state_update()
        signals a send happened elsewhere.
        """
        while self._is_running:
            try:
                wait_time = max(
                    0.0,
                    self._last_report_at + STATE_HEARTBEAT_TIMEOUT_SECONDS - time.monotonic(),
                )
                try:
                    await asyncio.wait_for(self._report_sent_event.wait(), timeout=wait_time)
                    self._report_sent_event.clear()
                    continue  # a send happened elsewhere; recompute the deadline
                except asyncio.TimeoutError:
                    pass

                # Idle for the full window. Only PLAYING needs a position
                # heartbeat — a paused/stopped session has nothing new to
                # say, but the baseline still advances so this doesn't spin
                # rechecking a deadline that's already passed.
                if self._player.state == PlaybackState.PLAYING:
                    await self._send_state_update()
                else:
                    self._last_report_at = time.monotonic()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}", exc_info=True)
                await asyncio.sleep(1.0)  # Brief pause before retry

    async def _send_state_update(self) -> None:
        """Build and send state update.

        Skipped outright while we're not the active renderer. This is
        deliberately independent of Player's own device-stop debounce
        (`_DEACTIVATION_GRACE_SECONDS` in deactivate_renderer) — that grace
        period exists to protect the *physical device* from a spurious
        WS-reconnect race, and keeps `state` reporting PLAYING for the
        whole window on purpose. But nothing says a report sent during
        that window is welcome: the server has already told us we're not
        active, and every one of those reports comes back rejected
        ("Renderer state updated message received from non active
        renderer"). Two separate concerns — don't fold this check into the
        debounce, and don't let a future change to one quietly affect the
        other.
        """
        if not self._player.is_active_renderer:
            logger.debug("Not the active renderer, skipping state update")
            # Still reset the watchdog clock — otherwise the heartbeat loop
            # (which gates on `state`, not on active-renderer status, and
            # can see PLAYING throughout the whole deactivation grace
            # window) would recompute a zero wait_time forever and spin.
            self._last_report_at = time.monotonic()
            return
        try:
            report = await self._build_state_report()
            await self._send_callback(report)
            # Only a successful send resets the watchdog clock — a string
            # of failures must not silently suppress heartbeats too.
            self._last_report_at = time.monotonic()
            self._report_sent_event.set()
            logger.debug(
                f"State update sent: {report.playing_state.name}, "
                f"pos={report.position_value_ms}ms, ts={report.position_timestamp_ms}"
            )
        except Exception as e:
            logger.error(f"Failed to send state update: {e}", exc_info=True)

    async def _build_state_report(self) -> PlaybackStateReport:
        """Build current state report.

        Every field read directly off Player is snapshotted up front, in
        one uninterrupted stretch, before either await below —
        queue.get_state() takes a real lock and backend.get_buffer_status()
        is free to do real I/O for a given backend, so each is a genuine
        point where a command-queue item could run and change Player's
        state in between. Reading Player first and awaiting after (rather
        than interleaved, as this used to) means there's no gap for a
        report to end up a torn mix of before/after values.
        """
        # Snapshot Player state synchronously — nothing here awaits.
        state = self._player.state
        current_track = self._player.current_track
        duration_ms = self._player.duration_ms
        now_ms = int(time.time() * 1000)
        if state == PlaybackState.PLAYING:
            # Timestamp-based position while playing.
            position_timestamp = self._player._position_timestamp_ms
            position_value = self._player._position_value_ms
            logger.debug(
                f"Building report (PLAYING): player._position_value_ms={position_value}, "
                f"player._position_timestamp_ms={position_timestamp}"
            )
        else:
            # Paused/stopped: freeze position at its last known value.
            position_timestamp = now_ms
            position_value = self._player.current_position_ms
            logger.debug(
                f"Building report ({state.name}): player.current_position_ms={position_value}"
            )

        queue_item_id = current_track.queue_item_id if current_track else 0
        # Defense in depth: the outgoing field is a signed int32. A bad
        # upstream value here (e.g. a not-yet-recognized server sentinel)
        # must degrade to "no item" rather than crash the report loop with
        # a protobuf range error every cycle.
        if not (0 <= queue_item_id <= 0x7FFFFFFF):
            logger.warning(f"Ignoring out-of-range queue_item_id={queue_item_id}; reporting 0")
            queue_item_id = 0

        # Everything below this point can genuinely yield to the event
        # loop — nothing above reads Player again after this.
        queue_state = await self._queue.get_state()
        buffer_status = await self._player.backend.get_buffer_status()

        return PlaybackStateReport(
            playing_state=state,
            buffer_state=buffer_status,
            position_timestamp_ms=position_timestamp,
            position_value_ms=position_value,
            duration_ms=duration_ms,
            current_queue_item_id=queue_item_id,
            queue_version_major=queue_state.version.major,
            queue_version_minor=queue_state.version.minor,
        )

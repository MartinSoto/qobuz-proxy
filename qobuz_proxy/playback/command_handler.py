"""
Playback command handler for WebSocket integration.

Processes playback commands from the Qobuz app via WsManager.
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

from qobuz_proxy.backends import PlaybackState

from .queue import QueueVersion

if TYPE_CHECKING:
    from .player import QobuzPlayer
    from .queue import QobuzQueue

logger = logging.getLogger(__name__)

# Quality change callback type
QualityChangeCallback = Callable[[int], Awaitable[None]]

# Asks the server for a full queue snapshot (WsManager.request_queue_state).
RequestQueueState = Callable[[], Awaitable[Any]]

# How long to let a burst of queue edits settle before asking for the snapshot
# that reflects all of them (edits can arrive 1-2 s apart, never faster).
SNAPSHOT_REQUEST_DEBOUNCE_SECONDS = 0.15

# QConnect message types for Server -> Renderer commands
MSG_TYPE_SET_STATE = 41  # SrvrRndrSetState: play/pause/stop, position, queue item
MSG_TYPE_SET_ACTIVE = 43  # SrvrRndrSetActive: renderer activation state
MSG_TYPE_SET_MAX_AUDIO_QUALITY = 44  # SrvrRndrSetMaxAudioQuality
MSG_TYPE_SET_LOOP_MODE = 45  # SrvrRndrSetLoopMode
MSG_TYPE_SET_SHUFFLE_MODE = 46  # SrvrRndrSetShuffleMode
MSG_TYPE_SET_AUTOPLAY_MODE = 47  # SrvrRndrSetAutoplayMode

# SrvrCtrlQueueLoadTracks — nominally a Controller-role broadcast (no
# rendererId field at all — it's session/queue-scoped, not targeted), but
# this is how the app actually signals "play this queue item", full stop.
# Confirmed live (2026-09-13, docs/connect-activation-model-hypotheses.md):
# tapping a track in an album/playlist view never produces a
# SrvrRndrSetState with a currentQueueItem — only this message, with
# queuePosition/queueVersion advancing. A Renderer session only receives
# it at all because of the dual Renderer+Controller join (see
# ws_manager.py's _send_ctrl_join_session) — previously believed
# (wrongly, see queue_diagnostics.py) to never reach a renderer socket.
MSG_TYPE_QUEUE_TRACKS_LOADED = 91  # SrvrCtrlQueueLoadTracks

# The rest of the Controller-role queue-editing traffic — reachable for
# the same dual-join reason as QUEUE_TRACKS_LOADED above. Confirmed live
# (2026-09-15, container capture) that QUEUE_STATE/QUEUE_TRACKS_LOADED are
# full snapshots but Added/Inserted/Removed/Reordered are genuine
# incremental deltas: e.g. QueueTracksAdded carries only the one newly
# added track, not the whole queue — see queue.py's QueueMembership,
# which these all feed.
MSG_TYPE_QUEUE_CLEARED = 89  # SrvrCtrlQueueCleared
MSG_TYPE_QUEUE_STATE = 90  # SrvrCtrlQueueState — full snapshot (also the reply to AskForQueueState)
MSG_TYPE_QUEUE_TRACKS_INSERTED = 92  # SrvrCtrlQueueTracksInserted
MSG_TYPE_QUEUE_TRACKS_ADDED = 93  # SrvrCtrlQueueTracksAdded
MSG_TYPE_QUEUE_TRACKS_REMOVED = 94  # SrvrCtrlQueueTracksRemoved
MSG_TYPE_QUEUE_TRACKS_REORDERED = 95  # SrvrCtrlQueueTracksReordered

# Session-level settings announced to Controllers (docs/queue-and-shuffle-wire-
# observations.md): shuffle (a minor-version bump, no queue resend, O7), repeat
# (no version bump, and never a renderer-targeted message after connect, O10),
# autoplay on/off and the autoplay suggestion list (O12, O21).
MSG_TYPE_SHUFFLE_MODE_SET = 96  # SrvrCtrlShuffleModeSet
MSG_TYPE_LOOP_MODE_SET = 97  # SrvrCtrlLoopModeSet
MSG_TYPE_AUTOPLAY_MODE_SET = 102  # SrvrCtrlAutoplayModeSet
MSG_TYPE_AUTOPLAY_TRACKS_LOADED = 103  # SrvrCtrlAutoplayTracksLoaded

# The server signals "no current/next track" with an all-bits-set
# QueueTrackRef rather than omitting the field — HasField() is still True,
# so these sentinel values must be checked explicitly. Widths match the
# proto: trackId is fixed32, queueItemId is uint64.
_SENTINEL_TRACK_ID = 0xFFFFFFFF
_SENTINEL_QUEUE_ITEM_ID = 0xFFFFFFFFFFFFFFFF

# PlayingState.PLAYING_STATE_PLAYING (qconnect_common.proto) — used directly
# as an int, the same way _handle_set_state passes state.playingState
# straight through to apply_remote_state() without a named enum.
_PLAYING_STATE_PLAYING = 2


# Messages that show somebody is using the room (see counts_as_activity). Queue
# loads and edits, and the volume command; SET_STATE is judged by its content.
_ACTIVITY_MESSAGE_TYPES = frozenset({42, 89, 91, 92, 93, 94, 95, 96})


def counts_as_activity(msg_type: int, message: Any) -> bool:
    """Whether an inbound message is the app doing something with this room (a
    seek, play/pause, a volume change, a queue load or edit) rather than the
    server's own traffic: restore and heartbeat echoes of our reports, which
    arrive as SET_STATE carrying only the queue version and next pointer, and
    broadcasts, must not keep an unused session alive. The restore that follows
    our own (re)join does carry a state; Player.note_ws_joined() masks it.
    Restarts the idle clock (Player.note_activity)."""
    if msg_type == MSG_TYPE_SET_STATE:
        if not message.HasField("srvrRndrSetState"):
            return False
        state = message.srvrRndrSetState
        return bool(
            state.HasField("playingState")
            or state.HasField("currentPosition")
            or state.HasField("currentQueueItem")
        )
    return msg_type in _ACTIVITY_MESSAGE_TYPES


def _is_no_track_sentinel(ref: Any) -> bool:
    """Whether a QueueTrackRef is the server's "no track" marker."""
    return bool(ref.trackId == _SENTINEL_TRACK_ID or ref.queueItemId == _SENTINEL_QUEUE_ITEM_ID)


class PlaybackCommandHandler:
    """
    Handles playback commands from WebSocket.

    Translates protobuf messages to player operations.
    """

    def __init__(
        self,
        player: "QobuzPlayer",
        queue: Optional["QobuzQueue"] = None,
        on_quality_change: Optional[QualityChangeCallback] = None,
        request_queue_state: Optional[RequestQueueState] = None,
        snapshot_debounce_s: float = SNAPSHOT_REQUEST_DEBOUNCE_SECONDS,
    ):
        """
        Initialize command handler.

        Args:
            player: QobuzPlayer instance
            queue: Optional QobuzQueue (defaults to player.queue)
            on_quality_change: Optional callback for quality change events
            request_queue_state: Asks the server for a full queue snapshot.
                Called (debounced) after every queue edit and shuffle toggle,
                because those messages do not say what the play order is now
                (O19). None disables it (queue order then goes stale on
                the first edit and stays so until a snapshot happens to
                arrive).
            snapshot_debounce_s: Settling time before such a request is sent.
        """
        self.player = player
        self.queue = queue or player.queue
        self._on_quality_change = on_quality_change
        self._request_queue_state = request_queue_state
        self._snapshot_debounce_s = snapshot_debounce_s
        self._snapshot_dirty = False
        self._snapshot_reasons: list[str] = []
        # The last next-pointer item reported as being outside the queue, so
        # the server following its autoplay list is logged once per track,
        # not on every state echo.
        self._outside_queue_logged: Optional[int] = None
        self._snapshot_task: Optional["asyncio.Task[None]"] = None

        # Store next track info for auto-advance (from SET_STATE nextQueueItem)
        self._next_track_info: Optional[dict] = None

        # Callback when next track info changes (for gapless re-arming)
        self._on_next_track_changed: Optional[Callable[[], Awaitable[None]]] = None

        # Callback when a snapshot changed the queue (a backend that holds the
        # whole queue on the device brings its copy up to date)
        self._on_queue_changed: Optional[Callable[[], Awaitable[None]]] = None

    def get_message_types(self) -> list[int]:
        """Get list of message types this handler processes."""
        return [
            MSG_TYPE_SET_STATE,
            MSG_TYPE_SET_ACTIVE,
            MSG_TYPE_SET_MAX_AUDIO_QUALITY,
            MSG_TYPE_SET_LOOP_MODE,
            MSG_TYPE_SET_SHUFFLE_MODE,
            MSG_TYPE_SET_AUTOPLAY_MODE,
            MSG_TYPE_QUEUE_CLEARED,
            MSG_TYPE_QUEUE_STATE,
            MSG_TYPE_QUEUE_TRACKS_LOADED,
            MSG_TYPE_QUEUE_TRACKS_INSERTED,
            MSG_TYPE_QUEUE_TRACKS_ADDED,
            MSG_TYPE_QUEUE_TRACKS_REMOVED,
            MSG_TYPE_QUEUE_TRACKS_REORDERED,
            MSG_TYPE_SHUFFLE_MODE_SET,
            MSG_TYPE_LOOP_MODE_SET,
            MSG_TYPE_AUTOPLAY_MODE_SET,
            MSG_TYPE_AUTOPLAY_TRACKS_LOADED,
        ]

    async def handle_message(self, msg_type: int, message: Any) -> None:
        """Handle a playback command message."""
        # TEMP: dump the raw message as-is, no interpretation — remove once
        # the next/previous investigation is done.
        logger.debug(f"Raw message (type {msg_type}): {message}")
        try:
            if msg_type == MSG_TYPE_SET_STATE:
                await self._handle_set_state(message)
            elif msg_type == MSG_TYPE_SET_ACTIVE:
                await self._handle_set_active(message)
            elif msg_type == MSG_TYPE_SET_MAX_AUDIO_QUALITY:
                await self._handle_set_max_audio_quality(message)
            elif msg_type == MSG_TYPE_SET_LOOP_MODE:
                await self._handle_set_loop_mode(message)
            elif msg_type == MSG_TYPE_SET_SHUFFLE_MODE:
                await self._handle_set_shuffle_mode(message)
            elif msg_type == MSG_TYPE_SET_AUTOPLAY_MODE:
                await self._handle_set_autoplay_mode(message)
            elif msg_type == MSG_TYPE_QUEUE_TRACKS_LOADED:
                await self._handle_queue_tracks_loaded(message)
            elif msg_type == MSG_TYPE_QUEUE_STATE:
                await self._handle_queue_state(message)
            elif msg_type == MSG_TYPE_QUEUE_TRACKS_ADDED:
                await self._handle_queue_tracks_added(message)
            elif msg_type == MSG_TYPE_QUEUE_TRACKS_INSERTED:
                await self._handle_queue_tracks_inserted(message)
            elif msg_type == MSG_TYPE_QUEUE_TRACKS_REMOVED:
                await self._handle_queue_tracks_removed(message)
            elif msg_type == MSG_TYPE_QUEUE_TRACKS_REORDERED:
                await self._handle_queue_tracks_reordered(message)
            elif msg_type == MSG_TYPE_QUEUE_CLEARED:
                await self._handle_queue_cleared(message)
            elif msg_type == MSG_TYPE_SHUFFLE_MODE_SET:
                await self._handle_shuffle_mode_set(message)
            elif msg_type == MSG_TYPE_LOOP_MODE_SET:
                await self._handle_loop_mode_set(message)
            elif msg_type == MSG_TYPE_AUTOPLAY_MODE_SET:
                await self._handle_autoplay_mode_set(message)
            elif msg_type == MSG_TYPE_AUTOPLAY_TRACKS_LOADED:
                await self._handle_autoplay_tracks_loaded(message)
            else:
                logger.warning(f"Unhandled playback message type: {msg_type}")
        except Exception as e:
            logger.error(f"Error handling playback command {msg_type}: {e}", exc_info=True)

    async def _handle_set_state(self, message: Any) -> None:
        """
        Handle SET_STATE message (type 41).

        This is the main playback control message from the server.
        Contains: playingState, currentPosition, queueVersion, currentQueueItem, nextQueueItem

        Important: Track info must be loaded BEFORE applying playingState, because
        the app may send track info with PAUSED state first, then PLAYING later.

        Note: For renderers, the server sends track info via SET_STATE rather than
        queue state messages (types 90/91). We store the next track info for
        auto-advance when the current track ends.
        """
        if not message.HasField("srvrRndrSetState"):
            logger.warning("SET_STATE message missing srvrRndrSetState field")
            return

        state = message.srvrRndrSetState
        logger.debug(f"SET_STATE received: {state}")

        # Extract current queue item info
        current_item = None
        current_queue_item_id = None
        current_track_id = None
        current_context_uuid = None
        if state.HasField("currentQueueItem") and not _is_no_track_sentinel(state.currentQueueItem):
            current_item = state.currentQueueItem
            current_queue_item_id = current_item.queueItemId
            current_track_id = current_item.trackId
            current_context_uuid = current_item.contextUuid if current_item.contextUuid else None
            logger.debug(
                f"Current queue item: queueItemId={current_queue_item_id}, trackId={current_track_id}"
            )
        elif state.HasField("currentQueueItem"):
            logger.debug("SET_STATE: currentQueueItem is the server's no-track sentinel")

        # Extract and store next queue item for auto-advance. The server
        # signals "no next track" with the explicit all-bits-set sentinel
        # (_is_no_track_sentinel) — exactly like it does for currentQueueItem
        # above — not by omitting the field. Previously, simply omitting
        # nextQueueItem was *also* treated as "clear it", but a SET_STATE
        # that's only a position/state update (a seek, a restart-current-
        # track tap) can legitimately leave it out without meaning "there's
        # no next track any more" — that reading tore down a still-valid
        # gapless arm on every such message, forcing a needless remove-then-
        # re-add of the armed track on the device each time (observed
        # directly: "Gapless: armed next track" logged again for the exact
        # same track a few seconds later, with nothing between but an
        # intervening seek). Sonos's queue is fragile to this kind of churn
        # (see sonos-retarget-gapless-desync) — a real "no next track" still
        # clears via the sentinel branch below; a message that simply didn't
        # mention it leaves whatever's already stored alone.
        next_track_changed = False
        if state.HasField("nextQueueItem"):
            if _is_no_track_sentinel(state.nextQueueItem):
                if self._next_track_info is not None:
                    self._next_track_info = None
                    next_track_changed = True
                    logger.debug("Next track cleared (server sent no-track sentinel)")
            else:
                next_item = state.nextQueueItem
                old_queue_item_id = (
                    self._next_track_info.get("queueItemId") if self._next_track_info else None
                )
                old_context_uuid = (
                    self._next_track_info.get("contextUuid") if self._next_track_info else None
                )
                new_context_uuid = next_item.contextUuid if next_item.contextUuid else None
                # Preserve a previously-known context if the server resends the same
                # next item without the optional contextUuid, so we don't lose it for
                # the next play (mirrors the current-track behaviour).
                if new_context_uuid is None and next_item.queueItemId == old_queue_item_id:
                    new_context_uuid = old_context_uuid
                new_next_info = {
                    "queueItemId": next_item.queueItemId,
                    "trackId": str(next_item.trackId),
                    "contextUuid": new_context_uuid,
                }
                # Detect change by queueItemId (handles same track at different
                # positions) or by a changed/late-arriving context UUID, which the
                # gapless arm must pick up so the next play reports the right context.
                if (
                    new_next_info["queueItemId"] != old_queue_item_id
                    or new_next_info["contextUuid"] != old_context_uuid
                ):
                    next_track_changed = True
                self._next_track_info = new_next_info
                logger.debug(
                    f"Next track stored: queueItemId={next_item.queueItemId}, "
                    f"trackId={next_item.trackId}"
                )
                await self._note_if_outside_queue(next_item.queueItemId, next_item.trackId)
        # else: nextQueueItem simply absent from this message — no signal
        # either way (see above); leave whatever's already stored as-is.

        # Store the server's queue version so our own state reports echo it
        # back — this was never done for a renderer session (queue.py's
        # version only otherwise updated via SRVR_CTRL_QUEUE_STATE/
        # QUEUE_TRACKS_LOADED, which never actually arrive for a renderer
        # session — see queue.py's module docstring), so every outbound
        # report always claimed queue version 0.0 regardless of what the
        # server had just told us
        # (observed directly, test1.log 2026-08-28: every message from the
        # app carried queueVersion, e.g. 4.1, while every one of our own
        # reports echoed 0.0 back). Worth fixing on its own — the version
        # exists specifically "for synchronization" (see QueueVersion) — and
        # is a plausible reason a client-side sync check on the app's end
        # would hold back further navigation commands, matching what's been
        # observed (bare playingState pings instead of a real track change).
        if state.HasField("queueVersion"):
            await self.queue.set_version(
                QueueVersion(major=state.queueVersion.major, minor=state.queueVersion.minor)
            )

        # Apply the desired remote state as a single atomic unit. A SET_STATE is
        # a multi-step intent (load this track, seek here, then play/pause/stop)
        # and each SET_STATE message runs in its own task, so applying the steps
        # via separate locked player calls could interleave and leave playback on
        # a stale track. apply_remote_state() applies the whole sequence under one
        # lock acquisition + generation check (newest SET_STATE wins as a unit),
        # and also handles the stale session-restore snapshot detection.
        track_id_arg = str(current_track_id) if current_item else None
        item_id_arg = current_queue_item_id
        playing_state_arg = state.playingState if state.HasField("playingState") else None
        position_arg = state.currentPosition if state.HasField("currentPosition") else None

        # A session that starts with a queue the app only *announced* (a snapshot,
        # no load) is told to seek, pause and play without ever being told which
        # track is current: nothing is loaded here and the message names none.
        inferred = await self._infer_current_item(state, current_item is not None)
        if inferred is not None:
            track_id_arg, item_id_arg = inferred
            if playing_state_arg is None:
                playing_state_arg = 3  # a position alone: hold the track at it, not playing

        await self.player.apply_remote_state(
            track_id=track_id_arg,
            queue_item_id=item_id_arg,
            position_ms=position_arg,
            playing_state=playing_state_arg,
            context_uuid=current_context_uuid,
        )

        # Notify gapless system about next track change (after state handling)
        if next_track_changed and self._on_next_track_changed:
            await self._on_next_track_changed()

    async def _infer_current_item(
        self, state: Any, has_current_item: bool
    ) -> Optional[tuple[str, int]]:
        """(track id, queue item id) of the track a SET_STATE means when it names
        none and nothing is loaded, or None when it does not apply.

        Seen on a fresh Connect session (2026-10-12, the "first join" case): the
        app announced its queue with a snapshot (no load), then sent a seek, a
        pause and a bare play, and the player answered 'nothing loaded' to each
        until the user tapped another track. The server's next pointer says where
        the queue stands (the current one is the entry before it); with none, a
        fresh session starts at the first entry of the play order."""
        if has_current_item or state.HasField("currentQueueItem"):
            return None  # it names one (or says explicitly there is none)
        if self.player.current_track is not None:
            return None
        asks_for_something = state.HasField("currentPosition") or (
            state.HasField("playingState") and state.playingState in (2, 3)
        )
        if not asks_for_something:
            return None
        order = await self.queue.play_order()
        if not order:
            return None
        entry = order[0]
        if state.HasField("nextQueueItem") and not _is_no_track_sentinel(state.nextQueueItem):
            ids = [e.queue_item_id for e in order]
            if state.nextQueueItem.queueItemId in ids:
                at = ids.index(state.nextQueueItem.queueItemId)
                if at > 0:
                    entry = order[at - 1]
        logger.info(
            f"SET_STATE names no current track and nothing is loaded: taking track "
            f"{entry.track_id} (queue item {entry.queue_item_id}) of the announced queue"
        )
        return str(entry.track_id), entry.queue_item_id

    async def _handle_queue_tracks_loaded(self, message: Any) -> None:
        """
        Handle SrvrCtrlQueueLoadTracks (type 91) — see MSG_TYPE_QUEUE_TRACKS_LOADED's
        own comment for why this, not SrvrRndrSetState, is the real "play
        this" signal for tapping a track in an album/playlist view.

        Carries no rendererId — it's a session-wide Controller broadcast,
        not targeted at us specifically, so it arrives regardless of which
        renderer is currently active. Only acted on while we're the active
        one; otherwise it's not our concern (and, per the same reasoning
        that gates StateReporter — see its own docstring — nothing of ours
        should be driven by it either).
        """
        if not message.HasField("srvrCtrlQueueTracksLoaded"):
            logger.warning("QUEUE_TRACKS_LOADED message missing srvrCtrlQueueTracksLoaded field")
            return

        loaded = message.srvrCtrlQueueTracksLoaded

        # Keep our own queue-version bookkeeping current regardless of
        # activation state — same reasoning as SET_STATE's queueVersion
        # handling above: so a report sent once we *do* become active
        # reflects reality instead of whatever the connect-time restore
        # last said (see docs/connect-activation-model-hypotheses.md,
        # "queueVersion never echoed").
        if loaded.HasField("queueVersion"):
            await self.queue.set_version(
                QueueVersion(major=loaded.queueVersion.major, minor=loaded.queueVersion.minor)
            )

        # Full snapshot — feed queue membership regardless of activation
        # state, same reasoning as the version bookkeeping just above.
        await self.queue.apply_snapshot(
            [(t.queueItemId, str(t.trackId)) for t in loaded.tracks],
            shuffle_mode=loaded.shuffleMode,
        )
        if loaded.shuffleMode:
            # Never seen on the wire (O9): a load does not carry the shuffled
            # order, so ask for it rather than guess.
            self._request_snapshot_soon("load with shuffle on")

        if not self.player.is_active_renderer:
            logger.debug("Not the active renderer, ignoring QUEUE_TRACKS_LOADED")
            return

        await self._start_from_tracks(
            loaded.tracks,
            loaded.queuePosition,  # absent reads as 0, the server's convention
            loaded.contextUuid,
            "QUEUE_TRACKS_LOADED",
        )

    async def _start_from_tracks(
        self, tracks: Any, position: int, message_context_uuid: Any, source: str
    ) -> None:
        """Start playing `tracks[position]` as a new queue generation and arm
        the entry after it for gapless."""
        loaded_tracks = tracks
        if not (0 <= position < len(loaded_tracks)):
            logger.warning(
                f"{source}: queuePosition={position} out of range for {len(loaded_tracks)} track(s)"
            )
            return

        track_ref = loaded_tracks[position]
        # Per-track contextUuid, falling back to the message-level one —
        # mirrors currentQueueItem.contextUuid's own fallback in
        # _handle_set_state above.
        context_uuid = (
            track_ref.contextUuid
            if track_ref.contextUuid
            else (message_context_uuid if message_context_uuid else None)
        )

        logger.info(f"{source} selects position {position}: trackId={track_ref.trackId}")

        # Tapping a track plays it, full stop — no follow-up SrvrRndrSetState
        # should be assumed to ever arrive to say so explicitly (confirmed
        # live: see the doc reference above). Always starts at 0, not
        # wherever a stale _current_track happened to be paused.
        await self.player.apply_remote_state(
            track_id=str(track_ref.trackId),
            queue_item_id=track_ref.queueItemId,
            position_ms=0,
            playing_state=_PLAYING_STATE_PLAYING,
            context_uuid=context_uuid,
            # An explicit load starts a new queue generation: even if it
            # names the track already playing, that track must be republished
            # under the new generation, not just seeked back to 0.
            force_reload=True,
        )

        # Populate next-track info for gapless, the same way a SET_STATE's
        # nextQueueItem does — we already have the whole queue right here.
        next_position = position + 1
        next_track_changed = False
        if next_position < len(loaded_tracks):
            next_ref = loaded_tracks[next_position]
            next_context_uuid = next_ref.contextUuid if next_ref.contextUuid else context_uuid
            new_next_info = {
                "queueItemId": next_ref.queueItemId,
                "trackId": str(next_ref.trackId),
                "contextUuid": next_context_uuid,
            }
            if new_next_info != self._next_track_info:
                next_track_changed = True
            self._next_track_info = new_next_info
        elif self._next_track_info is not None:
            self._next_track_info = None
            next_track_changed = True

        if next_track_changed and self._on_next_track_changed:
            await self._on_next_track_changed()

    def get_next_track_info(self) -> Optional[dict]:
        """Get the stored next track info for auto-advance."""
        return self._next_track_info

    def clear_next_track_info(self) -> None:
        """Clear the stored next track info after it's been used."""
        self._next_track_info = None

    def set_on_queue_changed(self, callback: Optional[Callable[[], Awaitable[None]]]) -> None:
        """Set callback for when a queue snapshot has been applied (see
        _handle_queue_state): the queue's order or contents may have changed."""
        self._on_queue_changed = callback

    def set_on_next_track_changed(self, callback: Optional[Callable[[], Awaitable[None]]]) -> None:
        """Set callback for when next track info changes (for gapless re-arming)."""
        self._on_next_track_changed = callback

    async def _handle_queue_state(self, message: Any) -> None:
        """Handle SrvrCtrlQueueState (type 90) — a full snapshot: the queue
        in its original order, the shuffle flag and, with shuffle on, the
        server's play order as `shuffledTrackIndexes` (O16). It is the reply
        to our own AskForQueueState and is also volunteered when another
        client (re)joins. Never drives playback the way QUEUE_TRACKS_LOADED
        does, so this only feeds the queue model."""
        if not message.HasField("srvrCtrlQueueState"):
            return
        state = message.srvrCtrlQueueState
        await self._note_version(state)
        await self.queue.apply_snapshot(
            [(t.queueItemId, str(t.trackId)) for t in state.tracks],
            shuffle_mode=state.shuffleMode,
            shuffled_indexes=list(state.shuffledTrackIndexes),
        )
        shuffled = await self.queue.shuffle_enabled()
        logger.info(
            f"Queue snapshot applied: {len(state.tracks)} track(s), "
            f"version {state.queueVersion.major}.{state.queueVersion.minor}, "
            f"shuffle {'on' if shuffled else 'off'}"
        )
        if self._on_queue_changed:
            await self._on_queue_changed()
        if self._is_play_request_snapshot(state):
            await self._start_from_tracks(state.tracks, 0, b"", "QUEUE_STATE")

    def _is_play_request_snapshot(self, state: Any) -> bool:
        """Experimental reading of a type-90 snapshot as "play this queue from
        its first entry" (the user's hypothesis, not confirmed by any spec).
        Join-time and reconnect snapshots carry an empty actionUuid in every
        log we have; a snapshot that names an action, arrives while nothing is
        playing and carries tracks is treated as the play request that never
        came as a type 91 (2026-10-14 fresh-session glitch)."""
        return bool(
            state.actionUuid
            and len(state.tracks) > 0
            and self.player.is_active_renderer
            and self.player.state == PlaybackState.STOPPED
        )

    async def _handle_queue_tracks_added(self, message: Any) -> None:
        """Handle SrvrCtrlQueueTracksAdded (type 93) — a genuine
        incremental delta (confirmed live 2026-09-15): only the newly
        added track(s) are carried, not the whole queue. They are appended
        to membership; the order goes stale and a snapshot is requested."""
        if not message.HasField("srvrCtrlQueueTracksAdded"):
            return
        added = message.srvrCtrlQueueTracksAdded
        await self._note_version(added)
        await self.queue.add_queue_tracks([(t.queueItemId, str(t.trackId)) for t in added.tracks])
        self._request_snapshot_soon("tracks added")

    async def _handle_queue_tracks_inserted(self, message: Any) -> None:
        """Handle SrvrCtrlQueueTracksInserted (type 92) — membership-wise
        identical to Added. Where the track ends up in the play order is not
        derivable (with shuffle on a "play next" re-arranges the upcoming
        tracks, O19), so the snapshot that follows is the only source."""
        if not message.HasField("srvrCtrlQueueTracksInserted"):
            return
        inserted = message.srvrCtrlQueueTracksInserted
        await self._note_version(inserted)
        await self.queue.add_queue_tracks(
            [(t.queueItemId, str(t.trackId)) for t in inserted.tracks]
        )
        self._request_snapshot_soon("tracks inserted")

    async def _handle_queue_tracks_removed(self, message: Any) -> None:
        """Handle SrvrCtrlQueueTracksRemoved (type 94) — a delta carrying
        only the removed item(s)' queueItemIds."""
        if not message.HasField("srvrCtrlQueueTracksRemoved"):
            return
        removed = message.srvrCtrlQueueTracksRemoved
        await self._note_version(removed)
        await self.queue.remove_queue_tracks(list(removed.queueItemIds))
        self._request_snapshot_soon("tracks removed")

    async def _handle_queue_tracks_reordered(self, message: Any) -> None:
        """Handle SrvrCtrlQueueTracksReordered (type 95) — a pure
        ordering change (queueItemIds + insertAfter anchor, where the anchor
        refers to the *shuffled* sequence while shuffle is on, O19);
        membership is unaffected, the order goes stale."""
        if not message.HasField("srvrCtrlQueueTracksReordered"):
            return
        reordered = message.srvrCtrlQueueTracksReordered
        await self._note_version(reordered)
        await self.queue.mark_order_stale()
        self._request_snapshot_soon("tracks reordered")

    async def _handle_queue_cleared(self, message: Any) -> None:
        """Handle SrvrCtrlQueueCleared (type 89)."""
        if not message.HasField("srvrCtrlQueueCleared"):
            return
        cleared = message.srvrCtrlQueueCleared
        await self._note_version(cleared)
        await self.queue.clear_queue_tracks()

    async def _handle_shuffle_mode_set(self, message: Any) -> None:
        """Handle SrvrCtrlShuffleModeSet (type 96): shuffle was toggled in
        the app. It carries only the new flag, a seed and the pivot item —
        never the order (O7) — and bumps the minor version without
        interrupting playback. The play order lives in the snapshot, so mark
        ours stale and ask for it."""
        if not message.HasField("srvrCtrlShuffleModeSet"):
            return
        toggled = message.srvrCtrlShuffleModeSet
        await self._note_version(toggled)
        logger.info(f"Shuffle toggled: {'on' if toggled.shuffleOn else 'off'}")
        await self.queue.mark_order_stale()
        self._request_snapshot_soon("shuffle toggled")

    async def _handle_loop_mode_set(self, message: Any) -> None:
        """Handle SrvrCtrlLoopModeSet (type 97): the repeat mode, announced
        to Controllers on every change and on connect. It is the only way
        this session ever learns of a change after connect (the
        renderer-targeted type 45 does not follow, O10), and it arrives
        twice per change (one per role of the dual join) — applying it is
        idempotent. No version bump accompanies it."""
        if not message.HasField("srvrCtrlLoopModeSet"):
            return
        mode = message.srvrCtrlLoopModeSet.mode
        logger.info(f"Loop mode announced: {mode}")
        await self.player.set_loop_mode(mode)

    async def _handle_autoplay_mode_set(self, message: Any) -> None:
        """Handle SrvrCtrlAutoplayModeSet (type 102): the autoplay setting
        changed (on = field present, off = absent, O21)."""
        if not message.HasField("srvrCtrlAutoplayModeSet"):
            return
        mode = message.srvrCtrlAutoplayModeSet
        await self._note_version(mode)
        await self.queue.set_autoplay_mode(mode.autoplayOn)

    async def _handle_autoplay_tracks_loaded(self, message: Any) -> None:
        """Handle SrvrCtrlAutoplayTracksLoaded (type 103): a fresh list of
        suggestions (sent after queue edits, O12). Only the version
        bookkeeping matters — what plays after the last queue item is
        whatever the server's next pointer names, not this list."""
        if not message.HasField("srvrCtrlAutoplayTracksLoaded"):
            return
        loaded = message.srvrCtrlAutoplayTracksLoaded
        await self._note_version(loaded)
        # Info, not debug: the server sends these after queue edits whether or not
        # the app's autoplay setting is on (docs/queue-and-shuffle-wire-
        # observations.md, O12/O20), and what plays after the last queue item
        # follows from them — worth seeing in an ordinary log.
        logger.info(f"Autoplay suggestions loaded: {len(loaded.tracks)} track(s)")

    async def _note_if_outside_queue(self, queue_item_id: int, track_id: int) -> None:
        """Log (once per item) when the server's next pointer names an item that
        is not in the queue: it is following its autoplay list (O20)."""
        if self._outside_queue_logged == queue_item_id:
            return
        if await self.queue.contains_item(queue_item_id) is False:
            self._outside_queue_logged = queue_item_id
            logger.info(
                f"Next track {track_id} (item {queue_item_id}) is not in the queue: "
                "the server is following its autoplay list"
            )

    async def _note_version(self, payload: Any) -> None:
        """Record the queue version a message carries, if it has one."""
        if payload.HasField("queueVersion"):
            await self.queue.set_version(
                QueueVersion(major=payload.queueVersion.major, minor=payload.queueVersion.minor)
            )

    def _request_snapshot_soon(self, reason: str = "queue change") -> None:
        """Ask the server for a fresh queue snapshot once the current burst
        of edits has settled. Every call marks the request wanted again, so
        an edit arriving after the ask went out (and so not covered by its
        reply) always triggers another round; edits arriving during the
        settling time share one request."""
        if self._request_queue_state is None:
            return
        if reason not in self._snapshot_reasons:
            self._snapshot_reasons.append(reason)
        self._snapshot_dirty = True
        if self._snapshot_task is None or self._snapshot_task.done():
            self._snapshot_task = asyncio.create_task(self._snapshot_request_loop())

    async def _snapshot_request_loop(self) -> None:
        assert self._request_queue_state is not None
        while self._snapshot_dirty:
            await asyncio.sleep(self._snapshot_debounce_s)
            self._snapshot_dirty = False  # everything seen so far is covered by this ask
            reasons = ", ".join(self._snapshot_reasons)
            self._snapshot_reasons = []
            logger.info(f"Requesting a fresh queue snapshot after: {reasons}")
            try:
                await self._request_queue_state()
            except Exception as e:
                logger.warning(f"Queue snapshot request failed: {e}")

    async def _handle_set_active(self, message: Any) -> None:
        """
        Handle SET_ACTIVE message (type 43).

        This tells the renderer if it's the currently active playback device.
        """
        if not message.HasField("srvrRndrSetActive"):
            logger.debug("SET_ACTIVE message missing srvrRndrSetActive field")
            return

        active = message.srvrRndrSetActive.active
        logger.info(f"Renderer set active: {active}")
        await self.player.set_active_renderer(active)

        if active:
            # A controller just attached — claim a silent, ready state
            # (Spotify-Connect-style) rather than leaving whatever's
            # already playing on a shared renderer, from a completely
            # different source, running until the app actually picks a
            # track. Then: the Qobuz cloud does not seem to replay our
            # last RndrSrvrVolumeChanged to a freshly-attached controller,
            # so the device picker can show an empty volume bar until we
            # re-emit — push current volume now.
            await self.player.claim_device()
            await self.player.broadcast_current_volume()
        else:
            # We're no longer the active renderer - stop playback (debounced;
            # see Player.deactivate_renderer for why this isn't immediate)
            await self.player.deactivate_renderer()

    async def _handle_set_max_audio_quality(self, message: Any) -> None:
        """
        Handle SET_MAX_AUDIO_QUALITY message (type 44).

        The protocol uses different values: 1=MP3, 2=LOSSLESS, 3=HIRES_L1, 4=HIRES_L3
        We convert to Qobuz quality IDs: 5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k
        """
        if not message.HasField("srvrRndrSetMaxAudioQuality"):
            return

        proto_quality = message.srvrRndrSetMaxAudioQuality.maxAudioQuality
        # Map protocol value to Qobuz quality ID
        protocol_to_quality = {1: 5, 2: 6, 3: 7, 4: 27}
        quality = protocol_to_quality.get(proto_quality, 27)

        logger.info(
            f"Max audio quality change requested: proto={proto_quality} -> qobuz_id={quality}"
        )

        if self._on_quality_change:
            await self._on_quality_change(quality)

    async def _handle_set_loop_mode(self, message: Any) -> None:
        """Handle SET_LOOP_MODE message (type 45)."""
        if not message.HasField("srvrRndrSetLoopMode"):
            return

        # Protocol LoopMode: 0=UNKNOWN, 1=OFF, 2=REPEAT_ONE, 3=REPEAT_ALL
        mode = message.srvrRndrSetLoopMode.mode
        logger.info(f"Loop mode set to: {mode}")
        await self.player.set_loop_mode(mode)

    async def _handle_set_shuffle_mode(self, message: Any) -> None:
        """Handle SET_SHUFFLE_MODE message (type 46)."""
        if not message.HasField("srvrRndrSetShuffleMode"):
            return

        shuffle_on = message.srvrRndrSetShuffleMode.shuffleOn
        logger.info(f"Shuffle mode set to: {shuffle_on}")
        await self.player.set_shuffle_mode(shuffle_on)

    async def _handle_set_autoplay_mode(self, message: Any) -> None:
        """Handle SET_AUTOPLAY_MODE message (type 47)."""
        if not message.HasField("srvrRndrSetAutoplayMode"):
            return

        autoplay_on = message.srvrRndrSetAutoplayMode.autoplayOn
        logger.info(f"Autoplay mode set to: {autoplay_on}")
        await self.player.set_autoplay_mode(autoplay_on)

"""
Queue management for QobuzProxy.

A Qobuz Connect session has two distinct client roles: **Renderer** (what
this app is — RNDR_SRVR_*/SRVR_RNDR_* messages) and **Controller** (the
Qobuz app's own queue UI, and any other controlling client on the account
— CTRL_SRVR_*/SRVR_CTRL_* messages). The full ordered queue — load, insert,
remove, reorder, ask-for-full-state — is nominally a Controller concern; a
Renderer alone would only ever be told, one SET_STATE at a time, "this is
current, this is next" (currentQueueItem/nextQueueItem/queueVersion). This
app now also joins as a Controller (see ws_manager.py's dual
Renderer+Controller join), so it does receive the queue messages a pure
Renderer never would — SRVR_CTRL_QUEUE_STATE/QUEUE_TRACKS_LOADED (full
snapshots) and QUEUE_TRACKS_ADDED/INSERTED/REMOVED/REORDERED/CLEARED
(incremental deltas on top of one) — confirmed live 2026-09-15 (see
command_handler.py's handlers for all of them).

What's here beyond the plain Renderer bookkeeping (repeat mode, the queue
version stamp echoed back in outbound state reports) is the queue as the
Controller-role messages describe it: QueueMembership (which tracks are in the
queue — the consumer is backends/dlna/sonos/gain.py's approximated-album-gain
grouping) and QueueOrder, the ordered view built **only from full snapshots**
(SRVR_CTRL_QUEUE_STATE / QUEUE_TRACKS_LOADED): the tracks in their original
order plus, when shuffle is on, the server's shuffled order as indexes into
that list. The incremental deltas (added/inserted/removed/reordered) keep
membership current but deliberately do *not* patch the order: with shuffle on
their effect on the play order is not derivable from the delta (see
docs/queue-and-shuffle-wire-observations.md, O19), so the order is marked
stale until the next snapshot, which the command handler asks for after every
edit. Track metadata caching (needs no track list at all — Player hands it
the exact QueueTrack to cache onto) and the blob/actual_quality a backend
resolved for a track (see set_track_stream_info) round out what's left —
streaming URL resolution itself lives entirely in the backend now (see
backends.dlna.proxy_server.resolve_track), not here.
"""

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


def session_tag(session_id: str) -> str:
    """A short, stable tag for a Qobuz session id, safe to put in URLs.

    Not secret and not meant to be: it only has to tell one session from
    another so a stream URL from a previous session is recognized as stale.
    Empty for an unknown session.
    """
    if not session_id:
        return ""
    return hashlib.sha256(session_id.encode()).hexdigest()[:8]


class RepeatMode(Enum):
    """Queue repeat modes."""

    OFF = "off"  # Stop after last track
    ONE = "one"  # Repeat current track
    ALL = "all"  # Loop entire queue


@dataclass
class QueueTrack:
    """
    Represents a track in the queue.

    Attributes:
        queue_item_id: Unique ID for this queue entry (from server)
        track_id: Qobuz track ID (for API calls)
        context_uuid: Optional context (album, playlist) UUID
        metadata: Cached track metadata dict
        start_ms: Start position for partial plays
        duration_ms: Track duration in milliseconds
        blob: Opaque token from Qobuz's getFileUrl, needed for
            reportStreamingEnd — set from the backend's PlayResult once
            play()/set_next_track() actually resolves this track (see
            QobuzQueue.set_track_stream_info). Streaming URL resolution
            itself lives entirely in the backend now (see
            backends.dlna.proxy_server.resolve_track /
            playback.stream_resolver.QobuzStreamResolver) — this class
            no longer fetches or caches one.
        actual_quality: Qobuz format_id actually served for this track.
    """

    queue_item_id: int
    track_id: str
    context_uuid: Optional[bytes] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    start_ms: int = 0
    duration_ms: int = 0
    blob: str = ""
    actual_quality: int = 0


@dataclass
class QueueVersion:
    """
    Queue version for synchronization.

    The server tracks queue state with major/minor version numbers.
    Major increments on structural changes (add/remove/reorder).
    Minor increments on metadata updates.
    """

    major: int = 0
    minor: int = 0

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"

    def is_newer_than(self, other: "QueueVersion") -> bool:
        """Check if this version is newer than another."""
        if self.major != other.major:
            return self.major > other.major
        return self.minor > other.minor


@dataclass
class QueueState:
    """
    Snapshot of current queue state for reporting.

    Used when sending state updates to the Qobuz app. Track-list-derived
    fields (count, index, shuffle) don't exist here — see the module
    docstring for why a Renderer session never has an ordered track list
    to describe in the first place.
    """

    version: QueueVersion
    repeat_mode: RepeatMode


@dataclass
class QueueMembership:
    """The current manual queue's track membership — replaced wholesale by
    a full snapshot (SrvrCtrlQueueTracksLoaded/QueueState) and patched by
    the incremental deltas (Added/Inserted/Removed/Cleared) on top of one;
    see command_handler.py's handlers for each.

    Order is deliberately not tracked: nothing that reads this needs it —
    Reordered is a pure membership no-op — and the one consumer today
    (Sonos's approximated-album-gain grouping, backends/dlna/sonos/
    gain.py) was confirmed live not to require completeness or original
    order, only that every track currently queued shares one album.

    `trustworthy` is False until a full snapshot has been seen for the
    current session generation. A Renderer-only session can otherwise
    only ever see deltas layered on a queue whose starting contents it
    never received (e.g. joining mid-session, or the version-went-
    backward case QobuzQueue.set_version handles) — without this,
    membership would be silently wrong rather than merely incomplete.
    """

    by_item: dict[int, str] = field(default_factory=dict)  # queue_item_id -> track_id
    trustworthy: bool = False


@dataclass(frozen=True)
class QueueEntry:
    """One item of the queue: the per-load ``queue_item_id`` (unique only
    within one major version of one load) and the Qobuz track id."""

    queue_item_id: int
    track_id: str


@dataclass
class QueueOrder:
    """The queue's order as the last full snapshot gave it.

    ``entries`` is the original order; with ``shuffle_mode`` on,
    ``shuffled_indexes`` is the server's play order as indexes into
    ``entries`` (confirmed to match the server's next pointers, O16/O19).
    ``fresh`` is False once a delta has arrived since the snapshot, i.e. the
    order may no longer be what the server holds.
    """

    entries: list[QueueEntry] = field(default_factory=list)
    shuffle_mode: bool = False
    shuffled_indexes: list[int] = field(default_factory=list)
    fresh: bool = False

    def play_order(self) -> list[QueueEntry]:
        """Entries in the order they will be played."""
        if self.shuffle_mode and self.shuffled_indexes:
            return [self.entries[i] for i in self.shuffled_indexes]
        return list(self.entries)


# Type alias for callbacks
MetadataCallback = Callable[[str], Coroutine[Any, Any, Optional[dict[str, Any]]]]


class QobuzQueue:
    """
    What a Renderer session needs from "the queue" — see the module
    docstring for why this isn't an ordered track list.
    """

    def __init__(self) -> None:
        # Mode settings
        self._repeat_mode: RepeatMode = RepeatMode.OFF

        # Version tracking — set from SET_STATE's own queueVersion field
        # (see PlaybackCommandHandler._handle_set_state), echoed back in
        # outbound state reports.
        self._version: QueueVersion = QueueVersion()

        # Callback for fetching metadata
        self._get_metadata_callback: Optional[MetadataCallback] = None

        # See QueueMembership.
        self._membership: QueueMembership = QueueMembership()
        # See QueueOrder.
        self._order: QueueOrder = QueueOrder()
        # The app's autoplay setting as last announced by the server (type
        # 102); None until one has been seen.
        self._autoplay_mode: Optional[bool] = None
        # session_tag() of the Qobuz session this queue belongs to; "" until
        # known. Together with the major version it names a queue generation.
        self._session_tag: str = ""

        self._lock = asyncio.Lock()

        logger.debug("QobuzQueue initialized")

    # =========================================================================
    # Callback Registration
    # =========================================================================

    def set_metadata_callback(self, callback: MetadataCallback) -> None:
        """Set callback for fetching track metadata."""
        self._get_metadata_callback = callback

    # =========================================================================
    # Repeat Mode
    # =========================================================================

    async def set_repeat_mode(self, mode: RepeatMode) -> None:
        """Set repeat mode."""
        async with self._lock:
            self._repeat_mode = mode
        logger.info(f"Repeat mode: {mode.value}")

    # =========================================================================
    # Track Caching
    # =========================================================================

    def set_track_stream_info(self, track: QueueTrack, blob: str, actual_quality: int) -> None:
        """Record what the backend actually resolved/served for this track
        (see AudioBackend.play/set_next_track's PlayResult) — the one thing
        the queue still caches about streaming, purely for play-reporting
        (Player._report_playing) to read back later without needing to ask
        the backend again."""
        track.blob = blob
        track.actual_quality = actual_quality

    async def get_track_metadata(self, track: QueueTrack) -> Optional[dict[str, Any]]:
        """The track's metadata — from cache if already fetched, otherwise
        fetched via the registered metadata callback and cached back onto
        the track (including duration_ms, which several callers read
        separately from the metadata dict itself).
        """
        if track.metadata:
            return track.metadata
        if not self._get_metadata_callback:
            return None
        meta = await self._get_metadata_callback(track.track_id)
        if meta:
            track.metadata = meta
            track.duration_ms = meta.get("duration_ms", 0)
            logger.debug(
                f"Fetched metadata for track {track.track_id}: "
                f"{meta.get('artist', '?')} - {meta.get('title', '?')}"
            )
        return meta

    # =========================================================================
    # State Access
    # =========================================================================

    async def get_state(self) -> QueueState:
        """Get current queue state snapshot."""
        async with self._lock:
            return QueueState(version=self._version, repeat_mode=self._repeat_mode)

    def set_session(self, session_id: str) -> None:
        """Record which Qobuz session this queue belongs to (known once the
        WebSocket tokens arrive)."""
        self._session_tag = session_tag(session_id)

    @property
    def generation(self) -> tuple[str, int]:
        """(session tag, major version): what a stream URL must match to
        count as belonging to the current queue. A new load bumps the major
        version, a new session changes the tag; minor bumps (shuffle, edits,
        autoplay) change neither, so entries survive them. Read without the
        lock on purpose: two plain attribute reads, called from request
        handlers that must not wait on queue updates."""
        return self._session_tag, self._version.major

    async def get_version(self) -> QueueVersion:
        """Get current queue version."""
        async with self._lock:
            return self._version

    async def set_version(self, version: QueueVersion) -> None:
        """Update queue version.

        Queue-generation counters only ever restart from 1 when a brand
        new session starts, which itself only happens once every client
        has been disconnected for a while (confirmed live) — so a version
        going *backward* is exactly that signal, and any queue membership
        reconstructed so far belongs to a session that's gone. Discarding
        it here — rather than only ever replacing it wholesale on the
        next full snapshot — means a delta message that arrives for the
        new session before its first full snapshot can't silently patch
        state left over from the old one.
        """
        async with self._lock:
            if version.major < self._version.major:
                logger.info(
                    f"Queue version went backward ({self._version} -> {version}); "
                    "assuming a new session, discarding queue membership"
                )
                self._membership = QueueMembership()
                self._order = QueueOrder()
            self._version = version

    # =========================================================================
    # Queue Membership (see QueueMembership)
    # =========================================================================

    async def apply_snapshot(
        self,
        items: list[tuple[int, str]],
        *,
        shuffle_mode: bool = False,
        shuffled_indexes: Optional[list[int]] = None,
    ) -> None:
        """A full snapshot arrived (SrvrCtrlQueueState/QueueTracksLoaded) —
        replace membership and order outright and mark both trustworthy.

        Args:
            items: (queue_item_id, track_id) pairs in the order the message
                carried them (the original, un-shuffled order).
            shuffle_mode: The snapshot's shuffle flag.
            shuffled_indexes: With shuffle on, the server's play order as
                indexes into ``items``. Anything that is not a permutation
                of ``range(len(items))`` is rejected with a warning and the
                original order is used instead — a wrong order must never
                be acted on.
        """
        indexes = list(shuffled_indexes or [])
        if shuffle_mode and indexes and sorted(indexes) != list(range(len(items))):
            logger.warning(
                f"Ignoring invalid shuffledTrackIndexes (not a permutation of "
                f"{len(items)} tracks): {indexes[:20]}"
            )
            indexes = []
        if shuffle_mode and not indexes and items:
            logger.warning("Shuffle is on but the snapshot carries no usable shuffled order")
        entries = [QueueEntry(queue_item_id, track_id) for queue_item_id, track_id in items]
        async with self._lock:
            self._membership = QueueMembership(by_item=dict(items), trustworthy=True)
            self._order = QueueOrder(
                entries=entries,
                shuffle_mode=shuffle_mode and bool(indexes),
                shuffled_indexes=indexes,
                fresh=True,
            )

    async def replace_queue_tracks(self, items: list[tuple[int, str]]) -> None:
        """A full snapshot arrived without shuffle information — see
        apply_snapshot."""
        await self.apply_snapshot(items)

    async def add_queue_tracks(self, items: list[tuple[int, str]]) -> None:
        """Tracks were added or inserted (SrvrCtrlQueueTracksAdded/
        Inserted). Membership gains them; the order goes stale (see
        QueueOrder)."""
        async with self._lock:
            self._membership.by_item.update(items)
            self._order.fresh = False

    async def remove_queue_tracks(self, queue_item_ids: list[int]) -> None:
        """Tracks were removed (SrvrCtrlQueueTracksRemoved)."""
        async with self._lock:
            for item_id in queue_item_ids:
                self._membership.by_item.pop(item_id, None)
            self._order.fresh = False

    async def mark_order_stale(self) -> None:
        """A delta that changes only the order (reorder, shuffle toggle)
        arrived: membership is unaffected, the order is no longer known."""
        async with self._lock:
            self._order.fresh = False

    async def clear_queue_tracks(self) -> None:
        """The queue was emptied (SrvrCtrlQueueCleared) — a fully known
        (trustworthy) empty state, not "unknown"."""
        async with self._lock:
            self._membership = QueueMembership(trustworthy=True)
            self._order = QueueOrder(fresh=True)

    async def play_order(self) -> Optional[list[QueueEntry]]:
        """The queue in play order, or None if the last snapshot has been
        outdated by a delta (or none has arrived yet) — callers must wait
        for the next snapshot rather than act on a stale order."""
        async with self._lock:
            if not self._order.fresh:
                return None
            return self._order.play_order()

    async def shuffle_enabled(self) -> bool:
        """Whether the last snapshot said shuffle is on (and carried a
        usable shuffled order)."""
        async with self._lock:
            return self._order.shuffle_mode

    # =========================================================================
    # Autoplay setting (as announced by the server)
    # =========================================================================

    async def set_autoplay_mode(self, enabled: bool) -> None:
        """The server announced the autoplay setting (SrvrCtrlAutoplayModeSet)."""
        async with self._lock:
            self._autoplay_mode = enabled
        logger.info(f"Autoplay mode: {'on' if enabled else 'off'}")

    async def autoplay_mode(self) -> Optional[bool]:
        """Last announced autoplay setting, or None if never announced."""
        async with self._lock:
            return self._autoplay_mode

    async def contains_item(self, queue_item_id: int) -> Optional[bool]:
        """Whether the queue holds this item id, or None while the queue's
        membership is not trustworthy yet (see QueueMembership). An id the
        queue does not hold, named by the server as the next track, is an
        autoplay track."""
        async with self._lock:
            if not self._membership.trustworthy:
                return None
            return queue_item_id in self._membership.by_item

    async def current_track_ids(self) -> Optional[frozenset[str]]:
        """The current queue's track ids, or None if membership isn't
        trustworthy yet (see QueueMembership) — the one consumer today is
        backends/dlna/sonos/gain.py's approximated-album-gain grouping."""
        async with self._lock:
            if not self._membership.trustworthy:
                return None
            return frozenset(self._membership.by_item.values())

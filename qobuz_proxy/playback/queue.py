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
version stamp echoed back in outbound state reports) is QueueMembership:
just enough of that Controller-role queue traffic to answer "which tracks
are in the queue right now", for backends/dlna/sonos/gain.py's
approximated-album-gain grouping — deliberately not a full ordered
track list (nothing needs one yet; see git history for the fuller
"does Player need a Queue component at all?" reasoning this superseded).
Track metadata caching (needs no track list at all — Player hands it the
exact QueueTrack to cache onto) and the blob/actual_quality a backend
resolved for a track (see set_track_stream_info) round out what's left —
streaming URL resolution itself lives entirely in the backend now (see
backends.dlna.proxy_server.resolve_track), not here.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


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
            self._version = version

    # =========================================================================
    # Queue Membership (see QueueMembership)
    # =========================================================================

    async def replace_queue_tracks(self, items: list[tuple[int, str]]) -> None:
        """A full snapshot arrived (SrvrCtrlQueueTracksLoaded/QueueState) —
        replace known membership outright and mark it trustworthy.

        Args:
            items: (queue_item_id, track_id) pairs, in whatever order the
                message carried them (order isn't kept — see
                QueueMembership).
        """
        async with self._lock:
            self._membership = QueueMembership(by_item=dict(items), trustworthy=True)

    async def add_queue_tracks(self, items: list[tuple[int, str]]) -> None:
        """Tracks were added or inserted (SrvrCtrlQueueTracksAdded/
        Inserted) — membership-wise these are identical: where they land
        doesn't matter here, only that they're now present."""
        async with self._lock:
            self._membership.by_item.update(items)

    async def remove_queue_tracks(self, queue_item_ids: list[int]) -> None:
        """Tracks were removed (SrvrCtrlQueueTracksRemoved)."""
        async with self._lock:
            for item_id in queue_item_ids:
                self._membership.by_item.pop(item_id, None)

    async def clear_queue_tracks(self) -> None:
        """The queue was emptied (SrvrCtrlQueueCleared) — a fully known
        (trustworthy) empty state, not "unknown"."""
        async with self._lock:
            self._membership = QueueMembership(trustworthy=True)

    async def current_track_ids(self) -> Optional[frozenset[str]]:
        """The current queue's track ids, or None if membership isn't
        trustworthy yet (see QueueMembership) — the one consumer today is
        backends/dlna/sonos/gain.py's approximated-album-gain grouping."""
        async with self._lock:
            if not self._membership.trustworthy:
                return None
            return frozenset(self._membership.by_item.values())

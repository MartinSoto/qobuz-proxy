"""
Queue management for QobuzProxy.

A Qobuz Connect session has two distinct client roles: **Renderer** (what
this app is — RNDR_SRVR_*/SRVR_RNDR_* messages) and **Controller** (the
Qobuz app's own queue UI, and any other controlling client on the account
— CTRL_SRVR_*/SRVR_CTRL_* messages). The full ordered queue — load, insert,
remove, reorder, ask-for-full-state — is exclusively a Controller concern;
a Renderer is simply told, one SET_STATE at a time, "this is current, this
is next" (currentQueueItem/nextQueueItem/queueVersion). This app never
plays a Controller role, so it never receives the messages
(SRVR_CTRL_QUEUE_STATE/QUEUE_TRACKS_LOADED) that would populate an ordered
track list — confirmed both by the protocol's own role separation and by
their handlers never firing across real captured sessions. See
docs/playback-concurrency.md, "Does Player need a Queue component at all?"

What's left here is what a Renderer session actually needs, all of it fed
directly by SET_STATE rather than derived from a track list this app never
has: the repeat mode (read by Player on natural track end) and the queue
version stamp (echoed back in outbound state reports, purely for the
server's own synchronization bookkeeping — nothing here reconciles against
it). Plus track metadata caching (needs no track list at all — Player
hands it the exact QueueTrack to cache onto) and the blob/actual_quality
a backend resolved for a track (see set_track_stream_info) — streaming URL
resolution itself lives entirely in the backend now (see
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
        """Update queue version."""
        async with self._lock:
            self._version = version

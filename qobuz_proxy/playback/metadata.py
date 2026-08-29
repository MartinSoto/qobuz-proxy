"""
Track display metadata retrieval and caching.

Title/artist/album/artwork/duration only — everything about streaming
URLs, Qobuz quality tiers, and play-format resolution lives in
QobuzStreamResolver (playback/stream_resolver.py) and, for DLNA/Sonos,
AudioProxyServer.resolve_track (backends/dlna/proxy_server.py) instead.
This split exists because the two are genuinely independent concerns:
display metadata is fetched once and never changes; streaming format is
decided per-backend, per-device-capability, and can be re-resolved many
times over a track's life.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from qobuz_proxy.auth.api_client import QobuzAPIClient
    from qobuz_proxy.backends import BackendTrackMetadata

logger = logging.getLogger(__name__)


class AudioQuality:
    """Qobuz audio quality format IDs."""

    MP3_320 = 5  # MP3 320 kbps
    FLAC_CD = 6  # FLAC 16-bit/44.1kHz
    FLAC_HIRES_96 = 7  # FLAC 24-bit/96kHz
    FLAC_HIRES_192 = 27  # FLAC 24-bit/192kHz

    NAMES: dict[int, str] = {
        5: "MP3 320kbps",
        6: "FLAC CD (16-bit/44.1kHz)",
        7: "FLAC Hi-Res (24-bit/96kHz)",
        27: "FLAC Hi-Res (24-bit/192kHz)",
    }

    @classmethod
    def get_name(cls, quality_id: int) -> str:
        """Get human-readable name for quality ID."""
        return cls.NAMES.get(quality_id, f"Unknown ({quality_id})")


@dataclass
class TrackMetadata:
    """Track display metadata."""

    track_id: str = ""
    title: str = ""
    artist: str = ""
    album: str = ""
    duration_ms: int = 0
    artwork_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "track_id": self.track_id,
            "title": self.title,
            "artist": self.artist,
            "album": self.album,
            "duration_ms": self.duration_ms,
            "artwork_url": self.artwork_url,
        }

    @property
    def duration_s(self) -> float:
        """Duration in seconds."""
        return self.duration_ms / 1000.0


@dataclass
class MetadataCache:
    """In-memory cache for track display metadata."""

    _cache: dict[str, TrackMetadata] = field(default_factory=dict)
    _max_size: int = 100

    def get(self, track_id: str) -> Optional[TrackMetadata]:
        """Get cached metadata for track."""
        return self._cache.get(track_id)

    def set(self, track_id: str, metadata: TrackMetadata) -> None:
        """Cache metadata for track."""
        # Simple LRU: remove oldest if at capacity
        if len(self._cache) >= self._max_size and track_id not in self._cache:
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        self._cache[track_id] = metadata

    def clear(self) -> None:
        """Clear all cached metadata."""
        self._cache.clear()


class MetadataService:
    """
    Service for retrieving and caching track display metadata.

    Uses QobuzAPIClient for API calls and maintains an in-memory cache.
    """

    def __init__(self, api_client: "QobuzAPIClient"):
        """
        Initialize metadata service.

        Args:
            api_client: Authenticated Qobuz API client
        """
        self._api = api_client
        self._cache = MetadataCache()

    async def get_metadata(self, track_id: str) -> Optional[TrackMetadata]:
        """
        Get track metadata, using cache when available.

        Args:
            track_id: Qobuz track ID

        Returns:
            TrackMetadata or None if not found
        """
        cached = self._cache.get(track_id)
        if cached:
            return cached

        fetched = await self._fetch_metadata(track_id)
        if not fetched:
            return None

        self._cache.set(track_id, fetched)
        return fetched

    async def _fetch_metadata(self, track_id: str) -> Optional[TrackMetadata]:
        """Fetch metadata from Qobuz API."""
        try:
            data = await self._api.get_track_metadata(track_id)
            if not data:
                logger.warning(f"No metadata found for track {track_id}")
                return None

            metadata = TrackMetadata(
                track_id=track_id,
                title=data.get("title", "Unknown"),
                artist=data.get("artist", "Unknown"),
                album=data.get("album", "Unknown"),
                duration_ms=data.get("duration_ms", 0),
                artwork_url=data.get("album_art_url", ""),
            )

            logger.debug(f"Fetched metadata: {metadata.artist} - {metadata.title}")
            return metadata

        except Exception as e:
            logger.error(f"Failed to fetch metadata for {track_id}: {e}")
            return None

    def log_now_playing_info(
        self, metadata: "BackendTrackMetadata", actual_quality: Optional[int] = None
    ) -> None:
        """
        Log currently playing track at INFO level using backend metadata.

        Args:
            metadata: Backend track metadata to log
            actual_quality: Actual quality ID the backend resolved/served —
                see PlayResult.format_id. None if unavailable.
        """
        quality_name = (
            AudioQuality.get_name(actual_quality) if actual_quality is not None else "unknown"
        )
        logger.info(
            f"Now playing: {metadata.artist} - {metadata.title} [{metadata.album}] ({quality_name})"
        )

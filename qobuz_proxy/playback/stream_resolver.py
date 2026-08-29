"""
Qobuz CDN stream resolution — the one place that calls track/getFileUrl.

Owns everything about turning (track_id, format_id) into a playable,
signed CDN URL: the authenticated, MD5-signed API call itself, and a
per-(track_id, format_id) cache with Qobuz's ~5 minute URL TTL. Every
consumer that needs Qobuz audio bytes — the DLNA/Sonos proxy's format
resolution (see backends/dlna/proxy_server.py's resolve_track) and the
local PortAudio backend — goes through this rather than calling the API
client directly, so there's exactly one implementation of "is this URL
still good, and how do I get a fresh one."

Single shared instance across the whole app (not one per speaker): two
rooms playing the same track at the same quality share one getFileUrl
call and one cached URL instead of each fetching their own — the app is
the only thing that ever reads from Qobuz here, speakers just consume
what this hands back.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from qobuz_proxy.auth.api_client import QobuzAPIClient

logger = logging.getLogger(__name__)

# Qobuz signed URLs live for ~5 minutes; anything older must be re-fetched.
URL_TTL_SECONDS = 5 * 60


@dataclass
class ResolvedStream:
    """What Qobuz actually handed back for one (track_id, format_id) request.

    ``format_id``/``sample_rate``/``bit_depth`` describe the *actual*
    format served — which can differ from the format_id requested (Qobuz
    falls back to whatever it really has for that track, it doesn't error)
    — this is the mechanism a caller uses to discover a track's true
    native format.
    """

    url: str
    blob: str
    format_id: int
    sample_rate: int  # Hz
    bit_depth: int
    fetched_at: float

    def is_expired(self, ttl: float = URL_TTL_SECONDS) -> bool:
        return (time.time() - self.fetched_at) >= ttl


class QobuzStreamResolver:
    """Resolves and caches Qobuz CDN URLs, keyed by (track_id, format_id).

    Keyed by format_id (not just track_id) because a single track can
    legitimately be resolved at more than one tier in one session — e.g.
    the DLNA proxy asking for a hi-res ceiling first and, only if that
    doesn't fit the device and downsampling is disabled, falling back to
    a CD-tier request for the same track.
    """

    def __init__(self, api_client: "QobuzAPIClient"):
        self._api = api_client
        self._cache: dict[tuple[str, int], ResolvedStream] = {}

    async def resolve(
        self, track_id: str, format_id: int, force: bool = False
    ) -> Optional[ResolvedStream]:
        """Return a fresh (or still-valid cached) stream for this track at
        this format tier. None if Qobuz has nothing to offer at all."""
        key = (track_id, format_id)
        cached = self._cache.get(key)
        if cached and not force and not cached.is_expired():
            return cached

        result = await self._api.get_track_url(track_id, format_id)
        if not result:
            logger.warning(f"No streaming URL available for track {track_id} @ {format_id}")
            return None

        sr = result.get("sampling_rate", 0)
        stream = ResolvedStream(
            url=result["url"],
            blob=result.get("blob", "") or "",
            format_id=result.get("format_id", format_id),
            # sampling_rate from Qobuz API is in kHz (e.g. 44.1, 96.0, 192.0)
            sample_rate=int(float(sr) * 1000) if sr else 0,
            bit_depth=int(result.get("bit_depth", 0)),
            fetched_at=time.time(),
        )
        self._cache[key] = stream
        return stream

    def invalidate(self, track_id: str) -> None:
        """Drop every cached tier for a track — e.g. after Qobuz rejects a
        cached URL as expired/invalid, so the next resolve() re-fetches."""
        for key in [k for k in self._cache if k[0] == track_id]:
            del self._cache[key]


__all__ = ["QobuzStreamResolver", "ResolvedStream", "URL_TTL_SECONDS"]

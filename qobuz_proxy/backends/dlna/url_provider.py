"""
Streaming URL provider protocol.

Abstraction for fetching fresh Qobuz streaming URLs.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class StreamingURLProvider(Protocol):
    """
    Protocol for providing streaming URLs.

    Implementations fetch fresh streaming URLs from Qobuz API.
    Used by AudioProxyServer to refresh expired URLs.
    """

    async def get_streaming_url(self, track_id: str, force: bool = False) -> str:
        """
        Get a fresh streaming URL for a track.

        Args:
            track_id: Qobuz track ID
            force: Bypass any URL cache and fetch a brand-new URL. Use when
                   the current URL is known to be dead (e.g. the CDN rejected
                   it) even if a cache still considers it valid.

        Returns:
            Fresh streaming URL from Qobuz CDN

        Raises:
            Exception: If URL cannot be fetched
        """
        ...

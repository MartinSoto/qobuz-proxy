"""
Metadata Service URL Provider.

Implementation of StreamingURLProvider that uses MetadataService.
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qobuz_proxy.playback import MetadataService

logger = logging.getLogger(__name__)


class MetadataServiceURLProvider:
    """
    URL provider that delegates to MetadataService.

    This adapter connects the AudioProxyServer to the MetadataService
    for fetching fresh streaming URLs.
    """

    def __init__(self, metadata_service: "MetadataService"):
        """
        Initialize provider.

        Args:
            metadata_service: MetadataService instance for URL fetching
        """
        self._metadata_service = metadata_service

    async def get_streaming_url(self, track_id: str, force: bool = False) -> str:
        """
        Get a fresh streaming URL for a track.

        Args:
            track_id: Qobuz track ID
            force: Invalidate the metadata cache first so a genuinely new URL
                   is fetched even when the cached one hasn't hit its TTL yet.

        Returns:
            Fresh streaming URL

        Raises:
            RuntimeError: If URL cannot be fetched
        """
        if force:
            url = await self._metadata_service.refresh_streaming_url(track_id)
        else:
            url = await self._metadata_service.get_streaming_url(track_id)
        if not url:
            raise RuntimeError(f"Failed to get streaming URL for track {track_id}")
        return url

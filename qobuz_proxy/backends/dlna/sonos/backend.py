"""
Sonos queue-based playback layered on top of the generic DLNA backend.

Sonos plays via its own AVTransport queue (SonosClient) rather than a
plain SetAVTransportURI — this is what gets Qobuz track metadata to show
up correctly in the Sonos app, and gapless relies on the same queue
(arming the next track means appending to it). Everything here only ever
overrides the seams DLNABackend exposes for exactly this
(_client_class/_start_transport/_arm_next_track/_get_current_transport_uri)
— no Sonos knowledge leaks back into the generic class.
"""

import logging
from typing import Optional

from qobuz_proxy.backends.types import BackendTrackMetadata

from ..backend import DLNABackend
from .client import SonosClient

logger = logging.getLogger(__name__)


class SonosBackend(DLNABackend):
    """DLNABackend with Sonos's AVTransport-queue playback."""

    _client_class = SonosClient

    @property
    def _sonos_client(self) -> SonosClient:
        assert isinstance(self._client, SonosClient)
        return self._client

    async def connect(self) -> bool:
        ok = await super().connect()
        if ok:
            logger.info("Sonos device detected — using queue-based playback")
        return ok

    async def _start_transport(self, url: str, didl: str) -> bool:
        """Start playback using the Sonos queue (shows metadata in the
        Sonos app), falling back to plain SetAVTransportURI if the queue
        itself can't be prepared."""
        client = self._sonos_client
        if not await client.clear_queue():
            logger.warning("Failed to clear queue, falling back to transport URI")
            return await self._play_via_transport(url, didl)

        if not await client.add_uri_to_queue(url, didl):
            logger.warning("Failed to add to queue, falling back to transport URI")
            return await self._play_via_transport(url, didl)

        if await client.play_from_queue(0):
            return True

        self._notify_playback_error("Failed to play from queue")
        return False

    async def _arm_next_track(
        self, actual_url: str, didl: str, metadata: BackendTrackMetadata
    ) -> bool:
        """Arm the next track by appending it to the Sonos queue."""
        # Already armed with this URL — appending again would queue a
        # duplicate entry and make the song play twice.
        if self._next_track_proxy_url == actual_url and self._next_track_queue_nr is not None:
            logger.debug("Gapless: next track already armed, skipping duplicate")
            return True

        queue_nr = await self._sonos_client.add_uri_to_queue(actual_url, didl)
        if queue_nr is not None:
            self._next_track_proxy_url = actual_url
            self._next_track_metadata = metadata
            self._next_track_queue_nr = queue_nr
            logger.info(f"Gapless: armed next track: {metadata.artist} - {metadata.title}")
            return True
        logger.warning("Gapless: failed to add next track to queue")
        return False

    async def clear_next_track(self) -> None:
        """Clear prepared next track.

        The armed track was appended to the device queue, so it must be
        removed there too — otherwise it still plays after the current one.
        """
        if self._client and self._next_track_queue_nr is not None:
            await self._sonos_client.remove_track_from_queue(self._next_track_queue_nr)
        self._next_track_queue_nr = None
        await super().clear_next_track()

    async def _get_current_transport_uri(self) -> Optional[str]:
        """Sonos queue playback's GetMediaInfo.CurrentURI returns the
        *queue* URI, not the track URL — GetPositionInfo.TrackURI is used
        instead."""
        return await self._sonos_client.get_track_uri()


__all__ = ["SonosBackend"]

"""
Sonos-only UPnP actions layered on top of the generic DLNA SOAP client.

Sonos plays via its own AVTransport *queue* rather than a plain
SetAVTransportURI — this is what gets Qobuz track metadata to show up
correctly in the Sonos app, and it's what gapless relies on (arming the
next track means appending to this same queue). None of this is
standard DLNA; a generic renderer (Denon HEOS, etc.) has no queue concept
and must never see these actions.
"""

from typing import Optional

from ..client import UPNP_AV_TRANSPORT, DLNAClient


class SonosClient(DLNAClient):
    """DLNAClient plus Sonos's AVTransport-queue actions."""

    async def clear_queue(self) -> bool:
        """Clear the device playback queue."""
        if not self.device_info:
            return False

        return (
            await self._soap_action(
                self.device_info.av_transport_url,
                UPNP_AV_TRANSPORT,
                "RemoveAllTracksFromQueue",
                {"InstanceID": "0"},
            )
            is not None
        )

    async def add_uri_to_queue(self, url: str, metadata: str = "") -> Optional[int]:
        """
        Add a URI to the device playback queue.

        Args:
            url: Audio URL to enqueue
            metadata: DIDL-Lite metadata XML

        Returns:
            1-based queue position of the enqueued track, or None on failure
        """
        if not self.device_info:
            return None

        response = await self._soap_action(
            self.device_info.av_transport_url,
            UPNP_AV_TRANSPORT,
            "AddURIToQueue",
            {
                "InstanceID": "0",
                "EnqueuedURI": url,
                "EnqueuedURIMetaData": metadata,
                "DesiredFirstTrackNumberEnqueued": "0",
                "EnqueueAsNext": "1",
            },
        )
        if response is None:
            return None

        track_nr = self._parse_xml_value(response, "FirstTrackNumberEnqueued")
        try:
            return int(track_nr) if track_nr else None
        except ValueError:
            return None

    async def remove_track_from_queue(self, track_nr: int) -> bool:
        """
        Remove a track from the device playback queue.

        Args:
            track_nr: 1-based queue position to remove

        Returns:
            True if successful
        """
        if not self.device_info:
            return False

        return (
            await self._soap_action(
                self.device_info.av_transport_url,
                UPNP_AV_TRANSPORT,
                "RemoveTrackFromQueue",
                {
                    "InstanceID": "0",
                    "ObjectID": f"Q:0/{track_nr}",
                    "UpdateID": "0",
                },
            )
            is not None
        )

    async def play_from_queue(self, index: int = 0) -> bool:
        """
        Start playback from a position in the queue.

        Args:
            index: 0-based queue position

        Returns:
            True if successful
        """
        if not self.device_info:
            return False

        # Set the queue as the active transport source (required by Sonos)
        # UDN may be "uuid:RINCON_xxx_MR" (MediaRenderer sub-device) — strip
        # the prefix and suffix to get the root device ID for the queue URI.
        uid = self.device_info.udn.removeprefix("uuid:")
        # Remove sub-device suffixes like _MR, _MS
        for suffix in ("_MR", "_MS"):
            if uid.endswith(suffix):
                uid = uid[: -len(suffix)]
                break
        queue_uri = f"x-rincon-queue:{uid}#0"
        if not await self.set_av_transport_uri(queue_uri, ""):
            return False

        # Seek to the queue position (1-based)
        track_nr = str(index + 1)
        seek_result = await self._soap_action(
            self.device_info.av_transport_url,
            UPNP_AV_TRANSPORT,
            "Seek",
            {"InstanceID": "0", "Unit": "TRACK_NR", "Target": track_nr},
        )
        if seek_result is None:
            return False

        return await self.play()


__all__ = ["SonosClient"]

"""
Sonos-only UPnP actions layered on top of the generic DLNA SOAP client.

Sonos plays via its own AVTransport *queue* rather than a plain
SetAVTransportURI — this is what gets Qobuz track metadata to show up
correctly in the Sonos app, and it's what gapless relies on (arming the
next track means appending to this same queue). None of this is
standard DLNA; a generic renderer (Denon HEOS, etc.) has no queue concept
and must never see these actions.

Also overrides volume control to act on the whole dynamic group
(GroupRenderingControl) rather than just the one physical speaker this
client happens to be connected to — see get_volume()/_do_set_volume().
"""

import time
from typing import Optional

from ..client import UPNP_AV_TRANSPORT, DLNAClient

# Controls the volume of an entire dynamic Sonos group (all its members
# scaled together) via the coordinator, rather than just the coordinator's
# own speaker. See DLNADeviceInfo.group_rendering_control_url.
UPNP_GROUP_RENDERING_CONTROL = "urn:schemas-upnp-org:service:GroupRenderingControl:1"


class SonosClient(DLNAClient):
    """DLNAClient plus Sonos's AVTransport-queue actions and group volume."""

    async def get_volume(self) -> Optional[int]:
        """
        Get current volume.

        Group volume (GroupRenderingControl) when the device exposes it,
        so it reflects what the Sonos app itself shows for the group, not
        just this one speaker. Falls back to the generic per-speaker
        RenderingControl volume otherwise.

        Returns:
            Volume 0-100
        """
        if not self.device_info or not self.device_info.group_rendering_control_url:
            return await super().get_volume()

        response = await self._soap_action(
            self.device_info.group_rendering_control_url,
            UPNP_GROUP_RENDERING_CONTROL,
            "GetGroupVolume",
            {"InstanceID": "0"},
        )
        if response:
            vol_str = self._parse_xml_value(response, "CurrentVolume")
            if vol_str:
                return int(vol_str)
        return None

    async def _do_set_volume(self, volume: int) -> bool:
        """Actually send the volume command — group volume when the
        device exposes it (SetGroupVolume), which Sonos itself scales
        proportionally across every member — matching what the Sonos
        app's own volume slider does for a group, instead of moving only
        the one physical speaker we happen to be connected to. Falls back
        to the generic per-speaker SetVolume otherwise."""
        if not self.device_info or not self.device_info.group_rendering_control_url:
            return await super()._do_set_volume(volume)

        # SetGroupVolume scales each member proportionally to the ratio
        # captured by the *last* SnapshotGroupVolume — not to their
        # current actual volumes. Without a fresh snapshot right before
        # each change, it scales against a stale (or undefined) ratio,
        # which is why one member (typically the coordinator, since it's
        # the one device we used to set individually pre-group-volume)
        # can end up moving far more than the others. See
        # https://sonos.svrooij.io/services/group-rendering-control
        await self._soap_action(
            self.device_info.group_rendering_control_url,
            UPNP_GROUP_RENDERING_CONTROL,
            "SnapshotGroupVolume",
            {"InstanceID": "0"},
            max_retries=1,
        )

        self._last_volume_time_ms = time.time() * 1000
        result = await self._soap_action(
            self.device_info.group_rendering_control_url,
            UPNP_GROUP_RENDERING_CONTROL,
            "SetGroupVolume",
            {"InstanceID": "0", "DesiredVolume": str(volume)},
            max_retries=1,
        )
        return result is not None

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

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
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional, Sequence

from ..client import UPNP_AV_TRANSPORT, DLNAClient, SoapResult

UPNP_CONTENT_DIRECTORY = "urn:schemas-upnp-org:service:ContentDirectory:1"

# The device refuses more than this many entries in one AddMultipleURIsToQueue
# call with UPnP error 402 (Phase 0, docs/sonos-queue-handover-design.md 9.2).
MAX_URIS_PER_ADD = 16

# UpdateID we pass to say "don't check": the device accepts it for every queue
# action (Phase 0). A real value is checked, and a stale one is refused.
ANY_UPDATE_ID = 0

# UPnP errors the queue actions answer with (Phase 0).
ERROR_INVALID_ARGS = 402  # e.g. a range that does not exist, 17 entries in one add
ERROR_NOT_COORDINATOR = 800  # a queue write sent to a group member, or an index past the end
ERROR_STALE_UPDATE_ID = 1028  # the UpdateID given is not the queue's current one


@dataclass(frozen=True)
class QueueOpResult:
    """Outcome of one queue-changing action."""

    ok: bool
    error_code: Optional[int] = None
    update_id: Optional[int] = None  # the queue's UpdateID after the change
    first_position: Optional[int] = None  # adds: 1-based position of the first new entry
    added: int = 0
    queue_length: Optional[int] = None  # adds: the queue's length afterwards

    @property
    def stale_update_id(self) -> bool:
        return self.error_code == ERROR_STALE_UPDATE_ID


@dataclass(frozen=True)
class DeviceQueueEntry:
    """One entry as the device reports its queue back."""

    position: int  # 1-based, as in the Q:0/<n> object id
    title: str
    url: str


@dataclass(frozen=True)
class QueuePage:
    """A slice of the device queue, with the queue's size and UpdateID."""

    entries: list[DeviceQueueEntry]
    total: int
    update_id: int


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

    # -------------------------------------------------------------------
    # Whole-queue management (queue handover). Every action here is sent
    # once and any UPnP answer is final: these change the queue, so a blind
    # retry could apply them twice (a re-sent add duplicates entries, a
    # re-sent removal removes the wrong ones). The caller re-reads the queue
    # (browse_queue) whenever it is unsure what happened.
    # -------------------------------------------------------------------

    async def _queue_action(self, action: str, args: dict[str, str]) -> SoapResult:
        assert self.device_info is not None
        return await self._soap_action_detailed(
            self.device_info.av_transport_url,
            UPNP_AV_TRANSPORT,
            action,
            args,
            max_retries=1,
            retry_upnp_errors=False,
        )

    def _op_result(self, result: SoapResult, **extra: Optional[int]) -> QueueOpResult:
        if not result.success:
            return QueueOpResult(ok=False, error_code=result.error_code)
        body = result.body or ""

        def number(tag: str) -> Optional[int]:
            text = self._parse_xml_value_exact(body, tag)
            return int(text) if text and text.lstrip("-").isdigit() else None

        return QueueOpResult(
            ok=True,
            update_id=number("NewUpdateID"),
            first_position=number("FirstTrackNumberEnqueued"),
            added=number("NumTracksAdded") or 0,
            queue_length=number("NewQueueLength"),
            **extra,
        )

    async def add_uris_to_queue(
        self,
        uris: Sequence[str],
        metadata: Sequence[str],
        position: int = 0,
        update_id: int = ANY_UPDATE_ID,
    ) -> QueueOpResult:
        """Add up to MAX_URIS_PER_ADD entries in one call.

        Args:
            uris: Stream URLs, in queue order. They must not contain spaces
                (the device takes them as one space-separated string).
            metadata: One DIDL-Lite document per URL, same order.
            position: 1-based position the first new entry takes (existing
                entries from there on shift down); 0, or anything past the
                end, appends.
            update_id: The queue UpdateID the caller believes is current, or
                ANY_UPDATE_ID.
        """
        if not self.device_info:
            return QueueOpResult(ok=False)
        if not 0 < len(uris) <= MAX_URIS_PER_ADD or len(uris) != len(metadata):
            raise ValueError(
                f"add_uris_to_queue takes 1-{MAX_URIS_PER_ADD} entries with matching metadata, "
                f"got {len(uris)} uris and {len(metadata)} metadata"
            )
        result = await self._queue_action(
            "AddMultipleURIsToQueue",
            {
                "InstanceID": "0",
                "UpdateID": str(update_id),
                "NumberOfURIs": str(len(uris)),
                "EnqueuedURIs": " ".join(uris),
                "EnqueuedURIsMetaData": " ".join(metadata),
                "ContainerURI": "",
                "ContainerMetaData": "",
                "DesiredFirstTrackNumberEnqueued": str(position),
                "EnqueueAsNext": "0",
            },
        )
        return self._op_result(result)

    async def remove_queue_range(
        self, start: int, count: int, update_id: int = ANY_UPDATE_ID
    ) -> QueueOpResult:
        """Remove `count` entries from 1-based position `start`. The range
        must lie inside the queue exactly (the device answers 402 otherwise,
        it does not clamp)."""
        if not self.device_info:
            return QueueOpResult(ok=False)
        result = await self._queue_action(
            "RemoveTrackRangeFromQueue",
            {
                "InstanceID": "0",
                "UpdateID": str(update_id),
                "StartingIndex": str(start),
                "NumberOfTracks": str(count),
            },
        )
        return self._op_result(result)

    async def move_queue_range(
        self, start: int, count: int, insert_before: int, update_id: int = ANY_UPDATE_ID
    ) -> QueueOpResult:
        """Move `count` entries from 1-based `start` so that they end up
        directly before the entry that is *currently* at 1-based position
        `insert_before` (len + 1 moves them to the end; anything past that
        is refused). A target inside or adjacent to the block itself changes
        nothing. The other entries keep their relative order."""
        if not self.device_info:
            return QueueOpResult(ok=False)
        result = await self._queue_action(
            "ReorderTracksInQueue",
            {
                "InstanceID": "0",
                "StartingIndex": str(start),
                "NumberOfTracks": str(count),
                "InsertBefore": str(insert_before),
                "UpdateID": str(update_id),
            },
        )
        # ReorderTracksInQueue answers with no output arguments, so the new
        # UpdateID is not in the reply; callers that care re-read it.
        return self._op_result(result)

    async def set_play_mode(self, mode: str) -> bool:
        """NORMAL, REPEAT_ALL or REPEAT_ONE (the Qobuz shuffle is never
        mapped to the device's own SHUFFLE modes, see the design doc 5.4)."""
        if not self.device_info:
            return False
        result = await self._queue_action("SetPlayMode", {"InstanceID": "0", "NewPlayMode": mode})
        return result.success

    async def seek_to_queue_position(self, position: int) -> bool:
        """Jump to the entry at 1-based queue `position` (keeps playing if it
        was playing)."""
        if not self.device_info:
            return False
        result = await self._queue_action(
            "Seek", {"InstanceID": "0", "Unit": "TRACK_NR", "Target": str(position)}
        )
        return result.success

    async def browse_queue(self, start: int = 0, count: int = 1000) -> Optional[QueuePage]:
        """Read the device queue back: 0-based `start` and up to `count`
        entries (the device returns at most 1000 per request). None if the
        read failed."""
        if not self.device_info or not self.device_info.content_directory_url:
            return None
        result = await self._soap_action_detailed(
            self.device_info.content_directory_url,
            UPNP_CONTENT_DIRECTORY,
            "Browse",
            {
                "ObjectID": "Q:0",
                "BrowseFlag": "BrowseDirectChildren",
                "Filter": "*",
                "StartingIndex": str(start),
                "RequestedCount": str(count),
                "SortCriteria": "",
            },
            max_retries=2,
        )
        if not result.success or not result.body:
            return None
        return self._parse_queue_page(result.body)

    @staticmethod
    def _parse_queue_page(body: str) -> Optional[QueuePage]:
        """Parse a ContentDirectory.Browse response for Q:0."""
        try:
            envelope = ET.fromstring(body)
            fields = {el.tag.split("}")[-1]: (el.text or "") for el in envelope.iter()}
            didl = ET.fromstring(fields["Result"])
            entries = []
            for item in didl:
                if item.tag.split("}")[-1] != "item":
                    continue
                position = int(item.get("id", "Q:0/0").rsplit("/", 1)[-1])
                title = ""
                url = ""
                for child in item:
                    tag = child.tag.split("}")[-1]
                    if tag == "title":
                        title = child.text or ""
                    elif tag == "res" and not url:
                        url = child.text or ""
                entries.append(DeviceQueueEntry(position=position, title=title, url=url))
            return QueuePage(
                entries=entries,
                total=int(fields.get("TotalMatches", len(entries))),
                update_id=int(fields.get("UpdateID", 0)),
            )
        except (ET.ParseError, KeyError, ValueError):
            return None


__all__ = [
    "ANY_UPDATE_ID",
    "DeviceQueueEntry",
    "MAX_URIS_PER_ADD",
    "QueueOpResult",
    "QueuePage",
    "SonosClient",
]

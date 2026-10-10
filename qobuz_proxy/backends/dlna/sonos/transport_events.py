"""Reading Sonos AVTransport `LastChange` events.

A NOTIFY body is a UPnP property set whose one `LastChange` property is itself
an XML document (escaped inside the first), carrying `<Field val="..."/>`
elements under an `InstanceID`. Only what changed is guaranteed to be there:
the first event after subscribing carries everything, later ones may carry a
subset, so every field of a TransportEvent is optional and None means "not
mentioned", never "empty".
"""

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

AVTRANSPORT_EVENT_PATH = "/MediaRenderer/AVTransport/Event"
GROUP_RENDERING_EVENT_PATH = "/MediaRenderer/GroupRenderingControl/Event"


@dataclass(frozen=True)
class TransportEvent:
    """What one AVTransport event says (None: the event does not mention it)."""

    transport_state: Optional[str] = None  # STOPPED, PLAYING, PAUSED_PLAYBACK, TRANSITIONING
    play_mode: Optional[str] = None  # NORMAL, REPEAT_ONE, REPEAT_ALL, ...
    current_track: Optional[int] = None  # 1-based position in the device queue
    number_of_tracks: Optional[int] = None
    current_track_uri: Optional[str] = None
    current_track_duration_ms: Optional[int] = None
    next_track_uri: Optional[str] = None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


_DURATION = re.compile(r"^(\d+):(\d{2}):(\d{2})(?:\.\d+)?$")


def _duration_ms(text: Optional[str]) -> Optional[int]:
    match = _DURATION.match(text.strip()) if text else None
    if match is None:
        return None
    hours, minutes, seconds = (int(g) for g in match.groups())
    return ((hours * 60 + minutes) * 60 + seconds) * 1000


def _int(text: Optional[str]) -> Optional[int]:
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_transport_event(body: str) -> Optional[TransportEvent]:
    """Read a NOTIFY body; None if it is not an AVTransport LastChange (or is
    not parseable), which callers treat as "nothing to learn from this"."""
    try:
        outer = ET.fromstring(body)
    except ET.ParseError:
        logger.debug("AVTransport NOTIFY body is not XML")
        return None
    last_change = next((e.text for e in outer.iter() if _local(e.tag) == "LastChange"), None)
    if not last_change:
        return None
    try:
        inner = ET.fromstring(last_change)
    except ET.ParseError:
        logger.debug("LastChange is not XML")
        return None

    values: dict[str, str] = {}
    for instance in inner:
        if _local(instance.tag) != "InstanceID":
            continue
        for field in instance:
            if "val" in field.attrib:
                values[_local(field.tag)] = field.attrib["val"]
    if not values:
        return None

    duration = values.get("CurrentTrackDuration")
    parsed = TransportEvent(
        transport_state=values.get("TransportState"),
        play_mode=values.get("CurrentPlayMode"),
        current_track=_int(values.get("CurrentTrack")),
        number_of_tracks=_int(values.get("NumberOfTracks")),
        current_track_uri=values.get("CurrentTrackURI"),
        current_track_duration_ms=_duration_ms(duration),
        next_track_uri=values.get("NextTrackURI"),
    )
    # A LastChange of another service (rendering control) carries none of these.
    return parsed if parsed != TransportEvent() else None


@dataclass(frozen=True)
class GroupVolumeEvent:
    """What a GroupRenderingControl event says (None: not mentioned)."""

    volume: Optional[int] = None
    muted: Optional[bool] = None


def parse_group_volume_event(body: str) -> Optional[GroupVolumeEvent]:
    """Read a GroupRenderingControl NOTIFY body (plain properties, no LastChange);
    None if it carries neither the volume nor the mute."""
    try:
        outer = ET.fromstring(body)
    except ET.ParseError:
        return None
    values = {_local(e.tag): (e.text or "") for e in outer.iter() if len(e) == 0}
    volume = _int(values.get("GroupVolume"))
    mute = _int(values.get("GroupMute"))
    if volume is None and mute is None:
        return None
    return GroupVolumeEvent(volume=volume, muted=None if mute is None else bool(mute))


__all__ = [
    "AVTRANSPORT_EVENT_PATH",
    "GROUP_RENDERING_EVENT_PATH",
    "GroupVolumeEvent",
    "TransportEvent",
    "parse_group_volume_event",
    "parse_transport_event",
]

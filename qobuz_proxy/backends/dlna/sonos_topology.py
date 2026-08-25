"""
Sonos zone group topology enrichment for DLNA discovery.

In a Sonos bonded stereo pair (or home theater set), only the group
coordinator accepts AVTransport/queue commands; the other members still
answer SSDP and expose all UPnP services, so plain discovery lists them
as seemingly valid renderers. This module queries the Sonos
`ZoneGroupTopology` service (any household member returns the topology of
the whole household) and uses it to:

- drop `Invisible="1"` members (bonded pair members, HT satellites) from
  discovery results, and
- replace Sonos's unhelpful friendlyName ("10.0.1.30 - Sonos One") with
  the room's ZoneName, tagging bonded stereo pairs.

Non-Sonos devices are never touched: the query only runs when a Sonos
device was discovered, and enrichment is a join on the device UDN against
the household's member UUIDs. On any failure the original discovery
results are returned unchanged.

The same `GetZoneGroupState` response also describes the household's
current *dynamic* grouping (`SonosGroup`/`fetch_sonos_groups`/
`parse_zone_groups`) — which players are currently combined to play
together, and which member of each group is the coordinator. Unlike a
bonded stereo pair, dynamic group members are not `Invisible`; they're
independently valid renderers, but AVTransport/queue commands must target
the group's coordinator to control playback for the whole group.
"""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import aiohttp

if TYPE_CHECKING:
    from .discovery import DiscoveredDevice

logger = logging.getLogger(__name__)

ZONE_GROUP_TOPOLOGY_PATH = "/ZoneGroupTopology/Control"
ZONE_GROUP_TOPOLOGY_SERVICE = "urn:schemas-upnp-org:service:ZoneGroupTopology:1"

_GET_ZONE_GROUP_STATE_BODY = (
    '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
    's:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">'
    "<s:Body>"
    f'<u:GetZoneGroupState xmlns:u="{ZONE_GROUP_TOPOLOGY_SERVICE}"/>'
    "</s:Body></s:Envelope>"
)


@dataclass
class SonosZoneMember:
    """One Sonos player as described by the household topology."""

    uuid: str
    zone_name: str
    invisible: bool
    is_stereo_pair: bool
    ip: str = ""
    port: int = 1400


@dataclass
class SonosGroup:
    """One dynamic Sonos group, as described by the household topology.

    Every player is always in some group — a room playing solo is simply
    the sole member of its own single-member group, with itself as
    coordinator. ``member_uuids`` lists direct ``ZoneGroupMember`` children
    only (in topology order, coordinator first); nested ``Satellite``
    elements (home theater surrounds/sub) are not separate group members.
    """

    coordinator_uuid: str
    member_uuids: list[str]


def _is_sonos(manufacturer: str) -> bool:
    return "sonos" in manufacturer.lower()


async def _fetch_zone_group_state_xml(
    devices: list["DiscoveredDevice"],
    timeout: float,
) -> str | None:
    """Query GetZoneGroupState on the first discovered Sonos device that answers.

    Returns the raw inner ``<ZoneGroupState>`` XML text (already unwrapped
    from the SOAP envelope), or None if no Sonos device was discovered or
    none could be queried.
    """
    sonos_devices = [d for d in devices if _is_sonos(d.manufacturer)]
    if not sonos_devices:
        return None

    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=client_timeout) as session:
        for device in sonos_devices:
            url = f"http://{device.ip}:{device.port}{ZONE_GROUP_TOPOLOGY_PATH}"
            try:
                async with session.post(
                    url,
                    data=_GET_ZONE_GROUP_STATE_BODY,
                    headers={
                        "SOAPACTION": f'"{ZONE_GROUP_TOPOLOGY_SERVICE}#GetZoneGroupState"',
                        "Content-Type": 'text/xml; charset="utf-8"',
                    },
                ) as response:
                    if response.status != 200:
                        logger.debug(f"GetZoneGroupState on {device.ip} returned {response.status}")
                        continue
                    return await response.text()
            except Exception as e:
                logger.debug(f"GetZoneGroupState on {device.ip} failed: {e}")

    logger.debug("No Sonos device answered GetZoneGroupState")
    return None


async def fetch_sonos_topology(
    devices: list["DiscoveredDevice"],
    timeout: float = 5.0,
) -> dict[str, SonosZoneMember] | None:
    """
    Fetch the Sonos household topology, keyed by member UUID.

    Tries each discovered Sonos device until one answers. Returns None if
    no Sonos device was discovered, none could be queried, or the response
    couldn't be parsed.
    """
    soap_response = await _fetch_zone_group_state_xml(devices, timeout)
    if soap_response is None:
        return None
    return parse_zone_group_state(soap_response)


async def fetch_sonos_groups(
    devices: list["DiscoveredDevice"],
    timeout: float = 5.0,
) -> list[SonosGroup] | None:
    """
    Fetch the Sonos household's dynamic group structure.

    Tries each discovered Sonos device until one answers. Returns None if
    no Sonos device was discovered, none could be queried, or the response
    couldn't be parsed.
    """
    soap_response = await _fetch_zone_group_state_xml(devices, timeout)
    if soap_response is None:
        return None
    return parse_zone_groups(soap_response)


def _extract_zone_group_state_root(soap_response: str) -> ET.Element | None:
    """Unwrap a GetZoneGroupState SOAP response to the inner topology XML root.

    Returns None if the envelope or the inner XML can't be parsed.
    """
    try:
        envelope = ET.fromstring(soap_response)
        state_elem = next((e for e in envelope.iter() if e.tag.endswith("ZoneGroupState")), None)
        if state_elem is None or not state_elem.text:
            return None

        # ZoneGroupState carries the topology as escaped XML text
        return ET.fromstring(state_elem.text)

    except ET.ParseError as e:
        logger.debug(f"Error parsing ZoneGroupState XML: {e}")
        return None


def parse_zone_group_state(soap_response: str) -> dict[str, SonosZoneMember] | None:
    """
    Parse a GetZoneGroupState SOAP response into members keyed by UUID.

    Returns None if the response can't be parsed.
    """
    root = _extract_zone_group_state_root(soap_response)
    if root is None:
        return None

    members: dict[str, SonosZoneMember] = {}
    # Satellite elements (home theater surrounds/sub) carry the same
    # attributes as ZoneGroupMember and are Invisible too
    for elem in list(root.iter("ZoneGroupMember")) + list(root.iter("Satellite")):
        uuid = elem.get("UUID", "")
        if not uuid:
            continue
        location = urlparse(elem.get("Location", ""))
        members[uuid] = SonosZoneMember(
            uuid=uuid,
            zone_name=elem.get("ZoneName", ""),
            invisible=elem.get("Invisible") == "1",
            is_stereo_pair=bool(elem.get("ChannelMapSet")),
            ip=location.hostname or "",
            port=location.port or 1400,
        )
    return members or None


def parse_zone_groups(soap_response: str) -> list[SonosGroup] | None:
    """
    Parse a GetZoneGroupState SOAP response into its dynamic group structure.

    Each ``<ZoneGroup>`` becomes one ``SonosGroup``. Only direct
    ``ZoneGroupMember`` children are collected as members — nested
    ``Satellite`` elements (home theater surrounds/sub) are part of their
    parent member, not independent group members.

    Returns None if the response can't be parsed or no groups are present.
    """
    root = _extract_zone_group_state_root(soap_response)
    if root is None:
        return None

    groups: list[SonosGroup] = []
    for group_elem in root.iter("ZoneGroup"):
        coordinator_uuid = group_elem.get("Coordinator", "")
        member_uuids = [
            uuid
            for member in group_elem.findall("ZoneGroupMember")
            if (uuid := member.get("UUID", ""))
        ]
        if coordinator_uuid and member_uuids:
            groups.append(SonosGroup(coordinator_uuid=coordinator_uuid, member_uuids=member_uuids))

    return groups or None


def enrich_discovered_devices(
    devices: list["DiscoveredDevice"],
    topology: dict[str, SonosZoneMember],
) -> list["DiscoveredDevice"]:
    """
    Apply Sonos topology to discovery results.

    Drops invisible members (bonded stereo pair non-coordinators, home
    theater satellites) and replaces friendly names with the room's
    ZoneName. Devices not present in the topology pass through unchanged.
    """
    result: list[DiscoveredDevice] = []
    for device in devices:
        member = topology.get(device.udn.removeprefix("uuid:"))
        if member is None:
            result.append(device)
            continue
        if member.invisible:
            logger.debug(
                f"Filtering out invisible Sonos member {device.ip} (part of '{member.zone_name}')"
            )
            continue
        if member.zone_name:
            device.friendly_name = member.zone_name + (
                " (stereo pair)" if member.is_stereo_pair else ""
            )
        result.append(device)
    return result


__all__ = [
    "SonosZoneMember",
    "SonosGroup",
    "fetch_sonos_topology",
    "fetch_sonos_groups",
    "parse_zone_group_state",
    "parse_zone_groups",
    "enrich_discovered_devices",
]

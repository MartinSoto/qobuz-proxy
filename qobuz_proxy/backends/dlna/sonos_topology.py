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
"""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import TYPE_CHECKING

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


def _is_sonos(manufacturer: str) -> bool:
    return "sonos" in manufacturer.lower()


async def fetch_sonos_topology(
    devices: list["DiscoveredDevice"],
    timeout: float = 5.0,
) -> dict[str, SonosZoneMember] | None:
    """
    Fetch the Sonos household topology, keyed by member UUID.

    Tries each discovered Sonos device until one answers. Returns None if
    no Sonos device was discovered or none could be queried.
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
                    topology = parse_zone_group_state(await response.text())
                    if topology:
                        return topology
            except Exception as e:
                logger.debug(f"GetZoneGroupState on {device.ip} failed: {e}")

    logger.debug("No Sonos device answered GetZoneGroupState")
    return None


def parse_zone_group_state(soap_response: str) -> dict[str, SonosZoneMember] | None:
    """
    Parse a GetZoneGroupState SOAP response into members keyed by UUID.

    Returns None if the response can't be parsed.
    """
    try:
        envelope = ET.fromstring(soap_response)
        state_elem = next((e for e in envelope.iter() if e.tag.endswith("ZoneGroupState")), None)
        if state_elem is None or not state_elem.text:
            return None

        # ZoneGroupState carries the topology as escaped XML text
        root = ET.fromstring(state_elem.text)

        members: dict[str, SonosZoneMember] = {}
        # Satellite elements (home theater surrounds/sub) carry the same
        # attributes as ZoneGroupMember and are Invisible too
        for elem in list(root.iter("ZoneGroupMember")) + list(root.iter("Satellite")):
            uuid = elem.get("UUID", "")
            if not uuid:
                continue
            members[uuid] = SonosZoneMember(
                uuid=uuid,
                zone_name=elem.get("ZoneName", ""),
                invisible=elem.get("Invisible") == "1",
                is_stereo_pair=bool(elem.get("ChannelMapSet")),
            )
        return members or None

    except ET.ParseError as e:
        logger.debug(f"Error parsing ZoneGroupState XML: {e}")
        return None


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
    "fetch_sonos_topology",
    "parse_zone_group_state",
    "enrich_discovered_devices",
]

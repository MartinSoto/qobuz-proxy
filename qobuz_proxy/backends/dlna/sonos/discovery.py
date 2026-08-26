"""
Sonos-aware wrapper around the generic DLNA SSDP scan.

discover_dlna_devices() itself stays manufacturer-blind — this module is
where a caller that specifically wants the nicer Sonos-aware result (hidden
bonded-pair members/HT satellites, room names instead of raw Sonos
friendlyNames) opts into it explicitly, instead of every discovery caller
paying for topology enrichment whether it wants it or not.
"""

from ..discovery import DiscoveredDevice, discover_dlna_devices
from .topology import enrich_discovered_devices, fetch_sonos_topology


async def discover_and_enrich(timeout: float = 5.0) -> list[DiscoveredDevice]:
    """
    Run the generic DLNA SSDP scan, then apply Sonos household-topology
    enrichment if any Sonos device was found.

    A no-op (and no extra network traffic) when no Sonos device is present
    in the scan results — see fetch_sonos_topology.
    """
    devices = await discover_dlna_devices(timeout=timeout)
    topology = await fetch_sonos_topology(devices)
    if topology:
        devices = enrich_discovered_devices(devices, topology)
    return devices


__all__ = ["discover_and_enrich"]

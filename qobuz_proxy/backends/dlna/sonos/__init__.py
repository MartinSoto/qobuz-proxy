"""
Sonos-specific extensions layered on top of the generic DLNA backend.

Nothing outside this package (client.py, backend.py, capabilities.py,
discovery.py at the dlna/ level) should need to know Sonos exists — see
this repo's architecture notes for the split.
"""

from .topology import (
    SonosZoneMember,
    enrich_discovered_devices,
    fetch_sonos_topology,
    parse_zone_group_state,
)

__all__ = [
    "SonosZoneMember",
    "enrich_discovered_devices",
    "fetch_sonos_topology",
    "parse_zone_group_state",
]

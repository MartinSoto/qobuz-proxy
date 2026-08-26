"""
Sonos-specific extensions layered on top of the generic DLNA backend.

Nothing outside this package (client.py, backend.py, capabilities.py,
discovery.py at the dlna/ level) should need to know Sonos exists — see
this repo's architecture notes for the split.
"""

from . import capabilities as _capabilities  # noqa: F401 — registers the Sonos override
from .backend import SonosBackend
from .client import SonosClient
from .discovery import discover_and_enrich
from .events import GenaSubscription, SonosEventSubscriber
from .topology import (
    SonosZoneMember,
    enrich_discovered_devices,
    fetch_sonos_topology,
    parse_zone_group_state,
)

__all__ = [
    "GenaSubscription",
    "SonosBackend",
    "SonosClient",
    "SonosEventSubscriber",
    "SonosZoneMember",
    "discover_and_enrich",
    "enrich_discovered_devices",
    "fetch_sonos_topology",
    "parse_zone_group_state",
]

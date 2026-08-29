"""
DLNA audio backend package.
"""

from .backend import DLNABackend
from .capabilities import (
    DLNACapabilities,
    CapabilityCache,
    DlnaProtocolInfoEntry,
    parse_protocol_info_sink,
    apply_device_overrides,
    build_protocol_info,
    QOBUZ_QUALITY_MP3,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_192K,
)
from .client import DLNAClient, DLNAClientError, DLNADeviceInfo, SoapResult
from .proxy_server import AudioProxyServer, RegisteredTrack, ResolvedTrack
from .discovery import DLNADiscovery, DiscoveredDevice, discover_dlna_devices
from .sonos.topology import (
    SonosZoneMember,
    enrich_discovered_devices,
    fetch_sonos_topology,
    parse_zone_group_state,
)

__all__ = [
    "DLNABackend",
    "DLNACapabilities",
    "CapabilityCache",
    "DlnaProtocolInfoEntry",
    "parse_protocol_info_sink",
    "apply_device_overrides",
    "build_protocol_info",
    "QOBUZ_QUALITY_MP3",
    "QOBUZ_QUALITY_CD",
    "QOBUZ_QUALITY_96K",
    "QOBUZ_QUALITY_192K",
    "DLNAClient",
    "DLNAClientError",
    "DLNADeviceInfo",
    "SoapResult",
    "AudioProxyServer",
    "RegisteredTrack",
    "ResolvedTrack",
    "DLNADiscovery",
    "DiscoveredDevice",
    "discover_dlna_devices",
    "SonosZoneMember",
    "enrich_discovered_devices",
    "fetch_sonos_topology",
    "parse_zone_group_state",
]

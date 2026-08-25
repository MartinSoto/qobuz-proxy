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
from .proxy_server import AudioProxyServer, RegisteredTrack
from .url_provider import StreamingURLProvider
from .metadata_url_provider import MetadataServiceURLProvider
from .discovery import DLNADiscovery, DiscoveredDevice, discover_dlna_devices
from .sonos_topology import (
    SonosZoneMember,
    SonosGroup,
    enrich_discovered_devices,
    fetch_sonos_topology,
    fetch_sonos_groups,
    parse_zone_group_state,
    parse_zone_groups,
)
from .sonos_discovery_manager import SonosRoom, SonosDiscoveryManager

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
    "StreamingURLProvider",
    "MetadataServiceURLProvider",
    "DLNADiscovery",
    "DiscoveredDevice",
    "discover_dlna_devices",
    "SonosZoneMember",
    "SonosGroup",
    "enrich_discovered_devices",
    "fetch_sonos_topology",
    "fetch_sonos_groups",
    "parse_zone_group_state",
    "parse_zone_groups",
    "SonosRoom",
    "SonosDiscoveryManager",
]

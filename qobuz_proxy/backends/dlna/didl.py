"""DIDL-Lite metadata for a queue entry or a single-URI transport.

Shared by DLNABackend (one track at a time) and the Sonos queue publisher
(a whole queue), which describe tracks to the device the same way.
"""

import logging
from typing import Optional

from qobuz_proxy.backends.types import BackendTrackMetadata

from .capabilities import DLNACapabilities, build_protocol_info

logger = logging.getLogger(__name__)


def build_didl(
    url: str,
    metadata: BackendTrackMetadata,
    content_type: str,
    sample_rate: int,
    bit_depth: int,
    capabilities: Optional[DLNACapabilities],
) -> str:
    """Build DIDL-Lite metadata XML.

    Args:
        capabilities: What the device can play, used for the protocolInfo
            (None if never discovered: a plain `http-get:*:<mime>:*`).
        sample_rate, bit_depth: What ``url`` actually serves — straight
            from resolve_track's ResolvedTrack, never re-derived here.
            The <res> element's sampleFrequency/bitsPerSample must
            describe what's really on the wire, not Qobuz's native
            format: a renderer that trusts these declared attributes
            (rather than re-parsing the WAV header) to compute a
            REL_TIME seek's byte offset would otherwise land
            mid-sample-frame and decode noise — audible only on seek,
            since playback from byte 0 never needs that math.
    """

    def escape(s: str) -> str:
        return (
            s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
        )

    # Build protocol info string based on capabilities
    if capabilities:
        protocol_info = build_protocol_info(capabilities, content_type, sample_rate, bit_depth)
    else:
        protocol_info = f"http-get:*:{content_type}:*"

    # Format duration as H:MM:SS for the res element
    duration_attr = ""
    if metadata.duration_ms > 0:
        total_s = metadata.duration_ms // 1000
        h = total_s // 3600
        m = (total_s % 3600) // 60
        s = total_s % 60
        duration_attr = f' duration="{h}:{m:02d}:{s:02d}"'

    didl = f"""<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/"
               xmlns:dc="http://purl.org/dc/elements/1.1/"
               xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">
<item id="1" parentID="0" restricted="1">
    <dc:title>{escape(metadata.title)}</dc:title>
    <dc:creator>{escape(metadata.artist)}</dc:creator>
    <upnp:artist>{escape(metadata.artist)}</upnp:artist>
    <upnp:album>{escape(metadata.album)}</upnp:album>
    <upnp:class>object.item.audioItem.musicTrack</upnp:class>"""

    if metadata.artwork_url:
        didl += f"\n        <upnp:albumArtURI>{escape(metadata.artwork_url)}</upnp:albumArtURI>"

    audio_attrs = ""
    if sample_rate:
        audio_attrs += f' sampleFrequency="{sample_rate}"'
    if bit_depth:
        audio_attrs += f' bitsPerSample="{bit_depth}"'

    didl += f"""
    <res protocolInfo="{escape(protocol_info)}"{duration_attr}{audio_attrs}>{escape(url)}</res>
</item>
</DIDL-Lite>"""

    return didl

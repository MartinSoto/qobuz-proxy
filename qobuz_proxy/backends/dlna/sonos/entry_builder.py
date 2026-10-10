"""Building publishable queue entries: stream URL plus DIDL-Lite per track.

The EntryBuilder SonosQueueSync asks for, backed by the real pieces: one
`track/getList` batch per call for display metadata and each track's best
format (cached, so a queue edit never refetches what it already knows), the
proxy's publish-time container choice for the URL, and the shared DIDL
builder. No per-track Qobuz stream request happens here: that waits until
the device asks for the entry (docs/sonos-queue-handover-design.md, 5.2).
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence

from qobuz_proxy.backends.types import BackendTrackMetadata

from ..didl import build_didl
from .audio_url import EntryKey
from .queue_sync import BuiltEntry

if TYPE_CHECKING:
    from qobuz_proxy.auth.api_client import QobuzAPIClient

    from ..capabilities import DLNACapabilities
    from .proxy_server import SonosAudioProxyServer

logger = logging.getLogger(__name__)


class ProxyEntryBuilder:
    def __init__(
        self,
        proxy: "SonosAudioProxyServer",
        api_client: "QobuzAPIClient",
        get_capabilities: Callable[[], Optional["DLNACapabilities"]],
        get_quality_override: Callable[[], Optional[int]] = lambda: None,
    ) -> None:
        """
        Args:
            proxy: Builds the stream URLs (publish_track) and owns the
                queue generation they are tagged with.
            api_client: Source of the batch track metadata.
            get_capabilities: What the device can play, read at build time
                (it may only be known after the backend connected).
            get_quality_override: A manual quality cap, if any.
        """
        self._proxy = proxy
        self._api = api_client
        self._get_capabilities = get_capabilities
        self._get_quality_override = get_quality_override
        # Unbounded like QueueGainResolver's cache, for the same reason: a few
        # strings and numbers per track a session actually touches.
        self._metadata: dict[str, dict[str, Any]] = {}

    async def _ensure_metadata(self, track_ids: Sequence[str]) -> None:
        missing = [t for t in dict.fromkeys(track_ids) if t not in self._metadata]
        if missing:
            self._metadata.update(await self._api.get_tracks_metadata(missing))

    async def build(self, keys: Sequence[EntryKey]) -> list[BuiltEntry]:
        await self._ensure_metadata([k.track_id for k in keys])
        capabilities = self._get_capabilities()
        override = self._get_quality_override()
        entries: list[BuiltEntry] = []
        for key in keys:
            known = self._metadata.get(key.track_id)
            if known is None:
                # Qobuz returned nothing for this id: publish it anyway (the
                # stream itself may still resolve) with a conservative
                # container choice and a placeholder title.
                logger.warning(f"No metadata for track {key.track_id}; publishing a bare entry")
            data = known or {}
            resolved = self._proxy.publish_track(
                key.track_id,
                capabilities,
                max_sample_rate_hz=int(data.get("max_sample_rate_hz", 0)),
                max_bit_depth=int(data.get("max_bit_depth", 0)),
                proxy_key=f"{key.track_id}_{key.queue_item_id}",
                forced_format_id=override,
                queue_item_id=key.queue_item_id,
                gain_db=await self._proxy.gain_for(key.track_id),
            )
            metadata = BackendTrackMetadata(
                track_id=key.track_id,
                title=data.get("title") or f"Track {key.track_id}",
                artist=data.get("artist", ""),
                album=data.get("album", ""),
                duration_ms=int(data.get("duration_ms", 0)),
                artwork_url=data.get("album_art_url", ""),
                queue_item_id=key.queue_item_id,
            )
            didl = build_didl(
                resolved.proxy_url,
                metadata,
                resolved.content_type,
                resolved.sample_rate,
                resolved.bit_depth,
                capabilities,
            )
            entries.append(BuiltEntry(url=resolved.proxy_url, didl=didl))
        return entries

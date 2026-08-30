"""
Sonos's on-the-fly Hi-Res downsampling, layered on the generic
AudioProxyServer via its two overridable seams
(_resolve_unfitting_stream/_serve) rather than a branch in the generic
class — see proxy_server.py's module docstring for why.

Sonos's S2 platform is capped at 48kHz regardless of model (see
capabilities.py's SONOS_MAX_SAMPLE_RATE) — a real, hardware-wide ceiling,
unlike a device that simply doesn't support FLAC or 24-bit at all. So a
track natively above that cap doesn't have to fall all the way back to CD
quality the way a genuinely incapable device would: it can be downsampled
in real time instead, keeping full 24-bit resolution at the device's own
rate. TranscodeStreamHandler (see transcode_stream.py) owns everything
about actually doing that — this class only decides *when* to reach for
it and dispatches matching requests to it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from aiohttp import web

from ..proxy_server import AudioProxyServer, RegisteredTrack, ResolvedTrack
from .transcode_stream import TranscodeStreamHandler
from .transcoding_reader import BYTES_PER_SAMPLE_24BIT

if TYPE_CHECKING:
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver, ResolvedStream
    from ..capabilities import DLNACapabilities

logger = logging.getLogger(__name__)


@dataclass
class SonosRegisteredTrack(RegisteredTrack):
    """A RegisteredTrack additionally marked for on-the-fly downsampling —
    served via TranscodeStreamHandler as downsampled PCM/WAV instead of a
    plain pass-through of the source FLAC. None (the default) is a normal
    passthrough/CD-fallback track, identical to any other DLNA device's."""

    transcode_to_sample_rate: Optional[int] = None


class SonosAudioProxyServer(AudioProxyServer):
    """AudioProxyServer with on-the-fly downsampling for tracks that
    exceed Sonos's 48kHz cap but would otherwise (bit-depth-wise) fit."""

    def __init__(
        self,
        resolver: "QobuzStreamResolver",
        hires_downsampling: bool = False,
        host: str = "0.0.0.0",
        port: int = 7120,
    ) -> None:
        """
        Args:
            resolver: Shared QobuzStreamResolver for fetching/caching CDN URLs
            hires_downsampling: Experimental, opt-in. When True, a track
                exceeding Sonos's 48kHz cap is downsampled on the fly
                instead of falling back to CD quality. False (the
                default) keeps the old, conservative behavior: nothing is
                ever transcoded.
            host: Host to bind to
            port: Port to listen on
        """
        super().__init__(resolver, host, port)
        self._hires_downsampling = hires_downsampling
        # Backs the on-the-fly downsampling path — one instance per proxy
        # server, shared by every transcoded track it serves. See
        # transcode_stream.py.
        self._transcode = TranscodeStreamHandler(resolver)

    async def stop(self) -> None:
        await self._transcode.close()
        await super().stop()

    async def _resolve_unfitting_stream(
        self,
        track_id: str,
        stream: "ResolvedStream",
        capabilities: Optional["DLNACapabilities"],
        proxy_key: Optional[str],
    ) -> Optional[ResolvedTrack]:
        """Try downsampling before falling back to CD quality — see module
        docstring. Downsampling only ever applies to a track that's
        exceeding the device's *sample-rate* cap while still fitting its
        bit depth (capabilities.max_bit_depth >= 24, already true of
        every Sonos device — see sonos/capabilities.py); anything that
        also fails on bit depth (or supports_flac at all) genuinely can't
        be served better than CD, same as any other DLNA device."""
        if (
            self._hires_downsampling
            and capabilities is not None
            and capabilities.supports_flac
            and capabilities.max_bit_depth >= 24
        ):
            return self._register_transcode(
                track_id, stream, capabilities.max_sample_rate, proxy_key
            )
        return await super()._resolve_unfitting_stream(track_id, stream, capabilities, proxy_key)

    def _register_transcode(
        self,
        track_id: str,
        stream: "ResolvedStream",
        target_sample_rate: int,
        proxy_key: Optional[str],
    ) -> ResolvedTrack:
        key = proxy_key or track_id
        self._tracks[key] = SonosRegisteredTrack(
            track_id=track_id,
            format_id=stream.format_id,
            content_type="audio/wav",
            transcode_to_sample_rate=target_sample_rate,
        )
        proxy_url = f"{self.base_url}/audio/{key}.wav"
        logger.debug(
            f"Registered track {track_id} (key={key}) -> {proxy_url} "
            f"[transcode {stream.sample_rate}Hz -> {target_sample_rate}Hz]"
        )
        return ResolvedTrack(
            proxy_url=proxy_url,
            content_type="audio/wav",
            sample_rate=target_sample_rate,
            bit_depth=BYTES_PER_SAMPLE_24BIT * 8,  # TranscodingFlacReader's fixed output depth
            format_id=stream.format_id,
            blob=stream.blob,
        )

    async def _serve(self, request: web.Request, track: RegisteredTrack) -> web.StreamResponse:
        if isinstance(track, SonosRegisteredTrack) and track.transcode_to_sample_rate is not None:
            if request.method == "HEAD":
                return await self._transcode.handle_head_probe(track)
            return await self._transcode.stream(request, track)
        return await super()._serve(request, track)


__all__ = ["SonosAudioProxyServer", "SonosRegisteredTrack"]

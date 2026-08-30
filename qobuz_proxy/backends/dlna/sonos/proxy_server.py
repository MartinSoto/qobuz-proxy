"""
Sonos's on-the-fly Hi-Res downsampling, layered on the generic
AudioProxyServer — see proxy_server.py's module docstring for why the
base class knows nothing about it. This class overrides resolve_track()
and _handle_audio() outright rather than the base class's finer-grained
seams (_register_passthrough/_resolve_unfitting_stream/_serve), and
duplicates _ceiling_tier_for/_fits_device rather than inheriting them
(identical logic today, but no longer shared code — see those methods'
own docstrings): it's moving toward standing on its own rather than being
layered on top of AudioProxyServer's internals. What's left actually
shared is the base HTTP-streaming engine (_proxy_stream/
_handle_head_probe/_log_stream_end), the aiohttp app/route setup in
start(), and the RegisteredTrack/ResolvedTrack dataclasses.

Sonos's S2 platform is capped at 48kHz regardless of model (see
capabilities.py's SONOS_MAX_SAMPLE_RATE) — a real, hardware-wide ceiling,
unlike a device that simply doesn't support FLAC or 24-bit at all. So a
track natively above that cap doesn't have to fall all the way back to CD
quality the way a genuinely incapable device would: it can be downsampled
in real time instead, keeping full 24-bit resolution at the device's own
rate. TranscodeStreamHandler (see transcode_stream.py) owns everything
about actually doing that — this class only decides *when* to reach for
it and dispatches matching requests to it.

Unlike the base class, this one is deliberately stateless: a proxy URL
here is fully self-describing —
"/audio/{track_id}?format={flac,mp3,wav}&depth={16,24}&rate={hz}&item=..."
— so there's no self._tracks registration table to consult when a request
comes in. `format`+`depth`+`rate` are decoded straight back into either a
plain CDN passthrough or a transcode request (`format=wav` *is* the
transcode signal — there's no separate flag to branch on); `item` is
never read, it exists purely so two different queue slots for the same
track+quality get distinguishable URLs (see backend.py's gapless-
transition detection, which keys off proxy-URL identity). Only a handful
of (format, depth, rate) combinations are actually servable — see
_PASSTHROUGH_QUALITIES — anything else is rejected outright rather than
guessed at.

That means duplicating resolve_track's decision tree and _handle_audio's
request parsing here rather than reusing the base class's own (dict-
registering) versions — an accepted cost of keeping the base class itself
generic and, elsewhere, genuinely stateful (gapless dedup there isn't
needed the same way, since a device that isn't Sonos never hits this
class at all). Passthrough and transcode share one builder
(_resolved_track) for that URL/ResolvedTrack — they differ only in which
(format, depth, rate) triple goes into it, not in how it's assembled.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from aiohttp import web

from ..capabilities import (
    QOBUZ_QUALITY_MP3,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_192K,
)
from ..proxy_server import AudioProxyServer, RegisteredTrack, ResolvedTrack
from .transcode_stream import TranscodeStreamHandler
from .transcoding_reader import BYTES_PER_SAMPLE_24BIT

if TYPE_CHECKING:
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver, ResolvedStream
    from ..capabilities import DLNACapabilities

logger = logging.getLogger(__name__)

# Every (format, depth, rate) triple this proxy can serve as a plain CDN
# passthrough, and the Qobuz quality tier each one requests. Deliberately
# closed — see module docstring — so an unrecognized combination in an
# incoming request is rejected rather than guessed at.
_PASSTHROUGH_QUALITIES = (
    ("mp3", 16, 44100, QOBUZ_QUALITY_MP3),
    ("flac", 16, 44100, QOBUZ_QUALITY_CD),
    ("flac", 24, 96000, QOBUZ_QUALITY_96K),
    ("flac", 24, 192000, QOBUZ_QUALITY_192K),
)
_QUERY_TO_FORMAT_ID = {(fmt, depth, rate): fid for fmt, depth, rate, fid in _PASSTHROUGH_QUALITIES}
_FORMAT_ID_TO_QUERY = {fid: (fmt, depth, rate) for fmt, depth, rate, fid in _PASSTHROUGH_QUALITIES}

# On-the-fly downsampling always transcodes from this tier — it's what
# _ceiling_tier_for() (proxy_server.py) requests for any device with
# max_bit_depth >= 24, which every Sonos device is (see capabilities.py) —
# so a transcode URL never needs to carry its own source tier.
_TRANSCODE_SOURCE_FORMAT_ID = QOBUZ_QUALITY_192K


@dataclass
class SonosRegisteredTrack(RegisteredTrack):
    """Same fields RegisteredTrack always carried, but never actually
    registered anywhere any more: _handle_audio builds one of these
    on the fly, straight from a request's own query string, purely as the
    parameter object TranscodeStreamHandler expects. None (the default) is
    unreachable in practice — this class is only ever constructed for a
    request already known to be format=wav."""

    transcode_to_sample_rate: Optional[int] = None


class SonosAudioProxyServer(AudioProxyServer):
    """AudioProxyServer with on-the-fly downsampling for tracks that
    exceed Sonos's 48kHz cap but would otherwise (bit-depth-wise) fit.

    See module docstring: unlike the base class, every URL this hands out
    is self-describing, so resolve_track() builds one directly via
    _resolved_track (never registering anything), and _handle_audio
    decodes a URL back into a request without ever consulting
    self._tracks."""

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

    @staticmethod
    def _ceiling_tier_for(capabilities: Optional["DLNACapabilities"]) -> int:
        """The initial Qobuz format tier to request for a track —
        deliberately the highest tier the device's *format* (FLAC
        support, bit depth) could ever use, ignoring its sample-rate cap,
        so resolve_track()'s fits/fallback decision is based on the
        track's true native format rather than a capability-clamped
        guess. Duplicated from AudioProxyServer's own version (see this
        module's docstring) rather than inherited — identical behavior
        for every Sonos device today, but no shared code any more."""
        if capabilities is None or not capabilities.supports_flac:
            return QOBUZ_QUALITY_MP3
        if capabilities.max_bit_depth >= 24:
            return QOBUZ_QUALITY_192K
        return QOBUZ_QUALITY_CD

    @staticmethod
    def _fits_device(stream: "ResolvedStream", capabilities: Optional["DLNACapabilities"]) -> bool:
        """Whether what Qobuz actually handed back for this request can be
        played by the device as-is, with no transcoding. Duplicated from
        AudioProxyServer's own version — see _ceiling_tier_for above."""
        if capabilities is None or not capabilities.supports_flac:
            return True  # nothing higher than MP3 was ever requested
        if stream.format_id == QOBUZ_QUALITY_MP3:
            return True
        return (
            stream.sample_rate <= capabilities.max_sample_rate
            and stream.bit_depth <= capabilities.max_bit_depth
        )

    async def resolve_track(
        self,
        track_id: str,
        capabilities: Optional["DLNACapabilities"],
        *,
        proxy_key: Optional[str] = None,
        forced_format_id: Optional[int] = None,
    ) -> Optional[ResolvedTrack]:
        """Sonos override of AudioProxyServer's own resolve_track — decides
        passthrough vs. on-the-fly downsampling vs. CD-tier fallback
        itself and calls _resolved_track directly for all three, rather
        than going through the base class's _register_passthrough/
        _resolve_unfitting_stream seams (this class is moving toward
        standing on its own rather than layering on AudioProxyServer's
        internals — see module docstring). _ceiling_tier_for/_fits_device
        (above) are this class's own duplicates, not the inherited ones.

        Downsampling only ever applies to a track that's exceeding the
        device's *sample-rate* cap while still fitting its bit depth
        (capabilities.max_bit_depth >= 24, already true of every Sonos
        device — see sonos/capabilities.py); anything that also fails on
        bit depth (or supports_flac at all) genuinely can't be served
        better than CD, same as any other DLNA device."""
        ceiling = (
            forced_format_id
            if forced_format_id is not None
            else self._ceiling_tier_for(capabilities)
        )
        stream = await self._resolver.resolve(track_id, ceiling)
        if stream is None:
            return None

        if self._fits_device(stream, capabilities):
            return self._resolved_track(track_id, stream, proxy_key)

        if (
            self._hires_downsampling
            and capabilities is not None
            and capabilities.supports_flac
            and capabilities.max_bit_depth >= 24
        ):
            return self._resolved_track(
                track_id, stream, proxy_key, transcode_to_sample_rate=capabilities.max_sample_rate
            )

        cd_stream = await self._resolver.resolve(track_id, QOBUZ_QUALITY_CD)
        if cd_stream is None:
            return None
        return self._resolved_track(track_id, cd_stream, proxy_key)

    def _resolved_track(
        self,
        track_id: str,
        stream: "ResolvedStream",
        proxy_key: Optional[str],
        transcode_to_sample_rate: Optional[int] = None,
    ) -> ResolvedTrack:
        """Build the self-describing URL (see module docstring) and
        ResolvedTrack for one stream — a plain passthrough of `stream` as
        given, or (transcode_to_sample_rate set) an on-the-fly downsample
        to it.

        On the transcode branch, the URL always fetches from
        _TRANSCODE_SOURCE_FORMAT_ID (the top 24-bit tier) at serve time,
        regardless of stream.format_id here — normally the same thing (see
        that constant's own comment), but forced_format_id (a manual
        quality override) can hand this a lower 24-bit tier's stream (e.g.
        96k) when it still doesn't fit the device; the URL can't carry an
        arbitrary source tier, so it deliberately always re-fetches the
        best available source instead of the specific one checked here —
        strictly equal-or-better input for the same downsample, at the
        cost of not honoring the override for this one edge case.
        stream.format_id is still kept as ResolvedTrack.format_id purely
        for app-facing quality reporting, unrelated to what gets fetched.
        """
        if transcode_to_sample_rate is not None:
            fmt, depth, rate = "wav", 24, transcode_to_sample_rate
            content_type = "audio/wav"
            sample_rate = transcode_to_sample_rate
            bit_depth = BYTES_PER_SAMPLE_24BIT * 8  # TranscodingFlacReader's fixed output depth
        else:
            fmt, depth, rate = _FORMAT_ID_TO_QUERY[stream.format_id]
            content_type = "audio/mpeg" if stream.format_id == QOBUZ_QUALITY_MP3 else "audio/flac"
            sample_rate = stream.sample_rate
            bit_depth = stream.bit_depth

        item = proxy_key or track_id
        proxy_url = (
            f"{self.base_url}/audio/{track_id}?format={fmt}&depth={depth}&rate={rate}&item={item}"
        )
        kind = (
            f"transcode {stream.sample_rate}Hz -> {rate}Hz"
            if transcode_to_sample_rate
            else "passthrough"
        )
        logger.debug(f"Resolved track {track_id} (item={item}) -> {proxy_url} [{kind}]")
        return ResolvedTrack(
            proxy_url=proxy_url,
            content_type=content_type,
            sample_rate=sample_rate,
            bit_depth=bit_depth,
            format_id=stream.format_id,
            blob=stream.blob,
        )

    async def _handle_audio(self, request: web.Request) -> web.StreamResponse:
        """Sonos override of AudioProxyServer's own _handle_audio:
        everything needed to answer the request is decoded straight from
        its URL (see module docstring) rather than looked up in
        self._tracks. `format=wav` *is* the transcode signal — no
        isinstance check, no stored flag, just a three-way switch on a
        query param. `item` is read only to be ignored: see module
        docstring for what it's actually for."""
        track_id = request.match_info["track_id"]
        query = request.query
        try:
            fmt = query["format"]
            depth = int(query["depth"])
            rate = int(query["rate"])
        except (KeyError, ValueError):
            logger.warning(
                f"Malformed audio request for track {track_id}: {request.query_string!r}"
            )
            return web.Response(status=400, text="Missing or invalid format/depth/rate")

        if fmt == "wav":
            if depth != 24:
                logger.warning(f"Unsupported wav depth for track {track_id}: {depth}")
                return web.Response(status=400, text=f"Unsupported wav depth: {depth}")
            sonos_track = SonosRegisteredTrack(
                track_id=track_id,
                format_id=_TRANSCODE_SOURCE_FORMAT_ID,
                content_type="audio/wav",
                transcode_to_sample_rate=rate,
            )
            if request.method == "HEAD":
                return await self._transcode.handle_head_probe(sonos_track)
            return await self._transcode.stream(request, sonos_track)

        format_id = _QUERY_TO_FORMAT_ID.get((fmt, depth, rate))
        if format_id is None:
            logger.warning(
                f"Unsupported format/depth/rate for track {track_id}: {fmt}/{depth}/{rate}"
            )
            return web.Response(status=404, text="Unsupported format/depth/rate combination")
        content_type = "audio/mpeg" if format_id == QOBUZ_QUALITY_MP3 else "audio/flac"
        track = RegisteredTrack(track_id=track_id, format_id=format_id, content_type=content_type)
        if request.method == "HEAD":
            return await self._handle_head_probe(track)
        return await self._proxy_stream(request, track)


__all__ = ["SonosAudioProxyServer", "SonosRegisteredTrack"]

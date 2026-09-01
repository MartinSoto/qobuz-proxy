"""
Sonos's proxy server — a standalone implementation, not a subclass of the
generic AudioProxyServer (see proxy_server.py's module docstring for why
that class knows nothing about Sonos). It shares no base class or runtime
code with AudioProxyServer at all; AudioProxyServerProtocol (see
proxy_server.py) is what lets DLNABackend/Speaker hold either one without
caring which. The two have simply diverged too far for a shared base to
help: a self-describing URL scheme with no server-side registration
table, its own request routing, and every track (transcoded or not) read
through a shared block cache rather than AudioProxyServer's per-request
upstream connection.

Sonos's S2 platform is capped at 48kHz regardless of model (see
capabilities.py's SONOS_MAX_SAMPLE_RATE) — a real, hardware-wide ceiling,
unlike a device that simply doesn't support FLAC or 24-bit at all. So a
track natively above that cap doesn't have to fall all the way back to CD
quality the way a genuinely incapable device would: it can be downsampled
in real time instead, keeping full 24-bit resolution at the device's own
rate. TranscodeStreamHandler (see transcode_stream.py) owns everything
about actually doing that; PassthroughStreamHandler (see
passthrough_stream.py) serves everything else — a track that already fits
the device, or the CD-tier fallback for one that doesn't and can't be
downsampled. Both read the CDN through one shared CDNBlockCache (see
cdn_block_cache.py), constructed here and handed to each — a passthrough
request gets the same single-flight coalescing and lingering-connection
reuse a transcode one already relied on, instead of each request opening
its own independent upstream connection.

Every proxy URL is fully self-describing —
"/audio/{track_id}?format={flac,mp3,wav}&depth={16,24}&rate={hz}&item=..."
— so there's no server-side registration table to consult when a request
comes in: resolve_track() only ever has to build one of these strings,
never register anything, and _handle_audio decodes `format`+`depth`+`rate`
straight back into either a plain CDN passthrough or a transcode request
(`format=wav` *is* the transcode signal — there's no separate flag to
branch on). `item` is never read back — it exists purely so two different
queue slots for the same track+quality get distinguishable URLs (see
backend.py's gapless-transition detection, which keys off proxy-URL
identity). Only a handful of (format, depth, rate) combinations are
actually servable — see _PASSTHROUGH_QUALITIES — anything else is
rejected outright rather than guessed at.

_ceiling_tier_for/_fits_device duplicate AudioProxyServer's own versions
verbatim (see those methods' own docstrings) — identical logic today
(nothing proxy/URL-specific about them), but no shared code.
"""

from __future__ import annotations

import logging
import socket
from typing import Optional, TYPE_CHECKING

from aiohttp import web

from ..capabilities import (
    QOBUZ_QUALITY_MP3,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_192K,
)
from ..proxy_server import ResolvedTrack
from .cdn_block_cache import CDNBlockCache
from .passthrough_stream import PassthroughStreamHandler
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
# _ceiling_tier_for() requests for any device with max_bit_depth >= 24,
# which every Sonos device is (see capabilities.py) — so a transcode URL
# never needs to carry its own source tier.
_TRANSCODE_SOURCE_FORMAT_ID = QOBUZ_QUALITY_192K


class SonosAudioProxyServer:
    """Standalone proxy server for Sonos: on-the-fly downsampling for
    tracks that exceed its 48kHz cap but would otherwise (bit-depth-wise)
    fit, plain passthrough for everything else — see module docstring for
    why this doesn't subclass AudioProxyServer."""

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
        self._resolver = resolver
        self._hires_downsampling = hires_downsampling
        self._host = host
        self._port = port

        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

        # One CDNBlockCache backs both request-handling paths below — see
        # module docstring.
        self._cache = CDNBlockCache(resolver=resolver)
        self._transcode = TranscodeStreamHandler(self._cache)
        self._passthrough = PassthroughStreamHandler(self._cache)

    @property
    def base_url(self) -> str:
        """Get the base URL for this proxy server."""
        host = self._host
        # Use actual IP if bound to 0.0.0.0
        if host == "0.0.0.0":
            host = self._get_local_ip()
        return f"http://{host}:{self._port}"

    @property
    def is_running(self) -> bool:
        """Check if the server is running."""
        return self._site is not None

    async def start(self) -> None:
        """Start the proxy server."""
        self._app = web.Application()
        self._app.router.add_get("/audio/{track_id}", self._handle_audio)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()

        logger.info(f"Sonos audio proxy server started on {self._host}:{self._port}")

    async def stop(self) -> None:
        """Stop the proxy server."""
        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

        await self._cache.close()
        logger.info("Sonos audio proxy server stopped")

    @staticmethod
    def _ceiling_tier_for(capabilities: Optional["DLNACapabilities"]) -> int:
        """The initial Qobuz format tier to request for a track —
        deliberately the highest tier the device's *format* (FLAC
        support, bit depth) could ever use, ignoring its sample-rate cap,
        so resolve_track()'s fits/fallback decision is based on the
        track's true native format rather than a capability-clamped
        guess. Duplicated from AudioProxyServer's own version (see this
        module's docstring) — identical behavior for every Sonos device
        today, but no shared code."""
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
        """Decide passthrough vs. on-the-fly downsampling vs. CD-tier
        fallback, and build the resulting self-describing URL via
        _resolved_track for all three (see module docstring).

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
        """Everything needed to answer the request is decoded straight
        from its URL (see module docstring). `format=wav` *is* the
        transcode signal — no stored flag, just a three-way switch on a
        query param, dispatching to whichever of TranscodeStreamHandler/
        PassthroughStreamHandler applies. `item` is read only to be
        ignored: see module docstring for what it's actually for."""
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
            if request.method == "HEAD":
                return await self._transcode.handle_head_probe(
                    track_id, _TRANSCODE_SOURCE_FORMAT_ID, rate
                )
            return await self._transcode.stream(
                request, track_id, _TRANSCODE_SOURCE_FORMAT_ID, rate
            )

        format_id = _QUERY_TO_FORMAT_ID.get((fmt, depth, rate))
        if format_id is None:
            logger.warning(
                f"Unsupported format/depth/rate for track {track_id}: {fmt}/{depth}/{rate}"
            )
            return web.Response(status=404, text="Unsupported format/depth/rate combination")
        content_type = "audio/mpeg" if format_id == QOBUZ_QUALITY_MP3 else "audio/flac"
        if request.method == "HEAD":
            return await self._passthrough.handle_head_probe(track_id, format_id, content_type)
        return await self._passthrough.stream(request, track_id, format_id, content_type)

    def _get_local_ip(self) -> str:
        """Get local IP address for proxy URL. Duplicated from
        AudioProxyServer's own version — see module docstring."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = str(s.getsockname()[0])
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"


__all__ = ["SonosAudioProxyServer"]

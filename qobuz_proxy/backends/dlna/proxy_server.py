"""
Audio Proxy Server.

HTTP server that proxies audio streams from Qobuz CDN to DLNA devices. Owns
the format/quality decision transparently: given a track and a device's
capabilities, resolve_track() always asks for the device's true format
ceiling first (never a capability-clamped guess), and either passes the
result straight through if it fits, or falls back to a safe CD-tier
request if it doesn't — handing back one proxy URL either way. The
renderer never knows or cares which; it just GETs a URL.

All actual Qobuz CDN URL fetching/caching lives one layer down, in
QobuzStreamResolver (see playback/stream_resolver.py) — this class never
talks to the Qobuz API directly, only to that shared resolver.

Deliberately generic — no manufacturer knowledge lives here. On-the-fly
Hi-Res downsampling (for devices with a real sample-rate cap below what
they could otherwise take, e.g. Sonos) is Sonos-specific today and lives
entirely in dlna/sonos/proxy_server.py's SonosAudioProxyServer, layered on
top of this class via two overridable seams rather than a hardcoded
branch here: _resolve_unfitting_stream() (what to do when the native
stream doesn't fit — this class always falls back to CD; the subclass
tries downsampling first) and _serve() (how to answer a request for an
already-registered track — the subclass recognizes its own transcode-
marked tracks and routes them elsewhere, deferring everything else here).
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
import traceback
from dataclasses import dataclass
from typing import Dict, Optional, TYPE_CHECKING

from aiohttp import web, ClientError, ClientSession, ClientTimeout

from .capabilities import (
    DLNACapabilities,
    QOBUZ_QUALITY_MP3,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_192K,
)

if TYPE_CHECKING:
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver, ResolvedStream

logger = logging.getLogger(__name__)

STREAM_CHUNK_SIZE = 64 * 1024  # 64KB chunks
REQUEST_TIMEOUT_SECONDS = 30
# Per-read timeout: fail fast when the CDN stalls mid-stream instead of hanging
# forever (the overall stream has no total timeout).
READ_TIMEOUT_SECONDS = 30
# Mid-stream reconnect settings: how many times to reconnect (with a Range resume)
# before giving up, and how long to back off between attempts.
MAX_UPSTREAM_RETRIES = 3
UPSTREAM_RETRY_DELAY_SECONDS = 0.5


def _parse_range_start(range_header: str) -> int:
    """Extract the start byte from a Range header ("bytes=X-...")."""
    try:
        return int(range_header.split("=", 1)[1].split("-", 1)[0] or 0)
    except (IndexError, ValueError):
        return 0


@dataclass
class RegisteredTrack:
    """A track registered with the proxy server — just enough to serve
    requests for it. The actual CDN URL is never stored here: it's re-
    resolved (via QobuzStreamResolver, which does its own caching/TTL) on
    every request that needs one, so this dataclass never goes stale.

    Subclassed by SonosAudioProxyServer's SonosRegisteredTrack to carry
    its own on-the-fly-downsampling bookkeeping — nothing generic here
    needs to know that exists."""

    track_id: str
    format_id: int
    content_type: str


@dataclass
class ResolvedTrack:
    """What resolve_track() decided to serve for one track — everything a
    caller (DLNABackend) needs to point a device at it and describe it
    correctly in DIDL-Lite metadata."""

    proxy_url: str
    content_type: str
    sample_rate: int
    bit_depth: int
    format_id: int
    blob: str


class AudioProxyServer:
    """
    Local HTTP proxy server for DLNA audio streaming.

    Provides stable local URLs to DLNA devices while handling:
    - Format/quality decisions (resolve_track)
    - Qobuz URL expiration (via the shared QobuzStreamResolver's own cache)
    - HTTP range requests for seeking
    - Streaming without full buffering

    Usage:
        proxy = AudioProxyServer(resolver=stream_resolver, host="0.0.0.0", port=7120)
        await proxy.start()

        resolved = await proxy.resolve_track(track_id, capabilities)
        # resolved.proxy_url = "http://192.168.1.100:7120/audio/12345.flac"

        # Pass resolved.proxy_url to the DLNA device
    """

    def __init__(
        self,
        resolver: "QobuzStreamResolver",
        host: str = "0.0.0.0",
        port: int = 7120,
    ):
        """
        Initialize audio proxy server.

        Args:
            resolver: Shared QobuzStreamResolver for fetching/caching CDN URLs
            host: Host to bind to
            port: Port to listen on
        """
        self._resolver = resolver
        self._host = host
        self._port = port

        self._tracks: Dict[str, RegisteredTrack] = {}
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

        # Will be set after start() to actual bound address
        self._actual_host: Optional[str] = None

    @property
    def base_url(self) -> str:
        """Get the base URL for this proxy server."""
        host = self._actual_host or self._host
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
        self._app.router.add_get("/audio/{track_id}.flac", self._handle_audio)
        self._app.router.add_get("/audio/{track_id}.mp3", self._handle_audio)
        self._app.router.add_get("/audio/{track_id}.wav", self._handle_audio)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()

        logger.info(f"Audio proxy server started on {self._host}:{self._port}")

    async def stop(self) -> None:
        """Stop the proxy server."""
        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

        self._tracks.clear()
        logger.info("Audio proxy server stopped")

    # =========================================================================
    # Format/quality resolution
    # =========================================================================

    @staticmethod
    def _ceiling_tier_for(capabilities: Optional[DLNACapabilities]) -> int:
        """The initial Qobuz format tier to request for a track.

        Deliberately the highest tier the device's *format* (FLAC support,
        bit depth) could ever make use of, ignoring its sample-rate cap —
        so the fits/fallback decision in resolve_track() is based on the
        track's true native format (what Qobuz actually hands back), not a
        capability-clamped guess. A device with real 24-bit support always
        gets asked for the 192k ceiling even if its own max_sample_rate is
        much lower: many hi-res tracks are natively mastered at or below
        that cap anyway, so this alone still gets them served as true
        24-bit rather than downgraded to CD quality for the whole track.
        On the ones that genuinely exceed the device's cap,
        _resolve_unfitting_stream() decides what happens next — this
        class falls all the way back to 16-bit; a subclass may do
        something smarter first (see its own docstring).
        """
        if capabilities is None or not capabilities.supports_flac:
            return QOBUZ_QUALITY_MP3
        if capabilities.max_bit_depth >= 24:
            return QOBUZ_QUALITY_192K
        return QOBUZ_QUALITY_CD

    @staticmethod
    def _fits_device(stream: "ResolvedStream", capabilities: Optional[DLNACapabilities]) -> bool:
        """Whether what Qobuz actually handed back for this request can be
        played by the device as-is, with no transcoding."""
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
        capabilities: Optional[DLNACapabilities],
        *,
        proxy_key: Optional[str] = None,
        forced_format_id: Optional[int] = None,
    ) -> Optional[ResolvedTrack]:
        """
        Decide what to actually serve for this track, and register it.

        Algorithm: always request the device's true format ceiling first
        (see _ceiling_tier_for) — never a capability-clamped tier — so the
        decision below is made against what the track *actually* is, not a
        guess. If that native format already fits the device, pass it
        through unmodified (this is the common case: most "Hi-Res" tracks
        aren't actually mastered above a device's real cap). If it doesn't
        fit, _resolve_unfitting_stream() decides what happens next — this
        class always falls back to a plain CD-quality (16/44) request,
        always within reach of anything that speaks FLAC at all; a
        subclass can override that seam to try something else first (e.g.
        SonosAudioProxyServer's on-the-fly downsampling).

        Args:
            track_id: Qobuz track ID
            capabilities: Device capabilities (None if never discovered)
            proxy_key: Optional key for the proxy URL path (defaults to
                track_id). Use a unique key like "{track_id}_{queue_item_id}"
                to produce distinct proxy URLs for duplicate tracks in a
                queue (gapless preload).
            forced_format_id: When set, used as the initial request instead
                of the capability-derived ceiling (a manual/app-driven
                quality override) — the fits/fallback decision still
                applies on top of it.

        Returns:
            ResolvedTrack, or None if Qobuz has nothing for this track at all.
        """
        ceiling = (
            forced_format_id
            if forced_format_id is not None
            else self._ceiling_tier_for(capabilities)
        )
        stream = await self._resolver.resolve(track_id, ceiling)
        if stream is None:
            return None

        if self._fits_device(stream, capabilities):
            return self._register_passthrough(track_id, stream, proxy_key)

        return await self._resolve_unfitting_stream(track_id, stream, capabilities, proxy_key)

    async def _resolve_unfitting_stream(
        self,
        track_id: str,
        stream: "ResolvedStream",
        capabilities: Optional[DLNACapabilities],
        proxy_key: Optional[str],
    ) -> Optional[ResolvedTrack]:
        """What to do when the native stream resolve_track() got back
        doesn't fit the device. This class always falls back to a safe
        CD-tier request; a subclass can try something else first (e.g. on-
        the-fly downsampling) and call super() for the same fallback."""
        cd_stream = await self._resolver.resolve(track_id, QOBUZ_QUALITY_CD)
        if cd_stream is None:
            return None
        return self._register_passthrough(track_id, cd_stream, proxy_key)

    def _register_passthrough(
        self, track_id: str, stream: "ResolvedStream", proxy_key: Optional[str]
    ) -> ResolvedTrack:
        content_type = "audio/mpeg" if stream.format_id == QOBUZ_QUALITY_MP3 else "audio/flac"
        ext = "mp3" if stream.format_id == QOBUZ_QUALITY_MP3 else "flac"
        key = proxy_key or track_id
        self._tracks[key] = RegisteredTrack(
            track_id=track_id, format_id=stream.format_id, content_type=content_type
        )
        proxy_url = f"{self.base_url}/audio/{key}.{ext}"
        logger.debug(f"Registered track {track_id} (key={key}) -> {proxy_url} [passthrough]")
        return ResolvedTrack(
            proxy_url=proxy_url,
            content_type=content_type,
            sample_rate=stream.sample_rate,
            bit_depth=stream.bit_depth,
            format_id=stream.format_id,
            blob=stream.blob,
        )

    async def _resolve_stream(
        self, track: RegisteredTrack, force: bool = False
    ) -> Optional["ResolvedStream"]:
        """Fetch (or re-fetch) the CDN stream for an already-registered
        track, via the shared resolver. force=True bypasses the resolver's
        own cache — required whenever the current URL is known or
        suspected dead."""
        try:
            return await self._resolver.resolve(track.track_id, track.format_id, force=force)
        except Exception as e:
            logger.warning(f"Could not resolve URL for track {track.track_id}: {e}")
            return None

    # =========================================================================
    # Request handling
    # =========================================================================

    async def _handle_audio(self, request: web.Request) -> web.StreamResponse:
        """Handle audio stream requests from DLNA devices."""
        # Extract proxy key (remove extension if present). Note: this is the
        # registration key, not necessarily the Qobuz track ID — gapless
        # registers tracks with composite keys like "{track_id}_{queue_item_id}"
        # so duplicates in a queue get distinct proxy URLs.
        proxy_key = request.match_info["track_id"]
        proxy_key = proxy_key.rsplit(".", 1)[0]  # Remove .flac/.mp3/.wav

        track = self._tracks.get(proxy_key)
        if not track:
            logger.warning(f"Unknown track requested: {proxy_key}")
            return web.Response(status=404, text="Track not found")

        logger.debug(
            f"Audio request: key={proxy_key} track={track.track_id} method={request.method} "
            f"range={request.headers.get('Range')!r}"
        )
        # TEMP DEBUG LOGGING
        logger.info(
            f"AudioProxyServer: incoming {request.method} url={request.url} "
            f"Range={request.headers.get('Range')!r}"
        )

        return await self._serve(request, track)

    async def _serve(self, request: web.Request, track: RegisteredTrack) -> web.StreamResponse:
        """Answer a request for an already-registered track. This class
        only ever does a plain CDN pass-through; a subclass overrides this
        to recognize its own specially-registered tracks (e.g.
        SonosAudioProxyServer's transcode-marked ones) and route them
        elsewhere, deferring to super() for everything else."""
        # HEAD probes (Denon/HEOS send one before every GET) route here too via
        # add_get's implicit HEAD support. Answer them headers-only — streaming
        # the body just to have aiohttp discard it downloads the whole track
        # from the CDN.
        if request.method == "HEAD":
            return await self._handle_head_probe(track)

        # Forward request to Qobuz CDN
        return await self._proxy_stream(request, track)

    async def _handle_head_probe(self, track: RegisteredTrack) -> web.Response:
        """Answer a HEAD probe from upstream headers, without a body transfer."""
        # web.Response computes Content-Length from its body automatically
        # and refuses `.content_length = ...` afterwards (RuntimeError) — it
        # has to go in the headers dict at construction time instead.
        headers = {"Content-Type": track.content_type, "Accept-Ranges": "bytes"}
        stream = await self._resolve_stream(track)
        if stream is None:
            return web.Response(status=200, headers=headers)
        timeout = ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        try:
            # TEMP DEBUG LOGGING
            fetch_start = time.monotonic()
            logger.info(f"AudioProxyServer: outbound HEAD start url={stream.url}")
            async with ClientSession(timeout=timeout) as session:
                async with session.head(stream.url, allow_redirects=True) as upstream:
                    cl = upstream.headers.get("Content-Length")
                    if upstream.status in (200, 206) and cl and cl.isdigit():
                        headers["Content-Length"] = cl
                    # TEMP DEBUG LOGGING
                    logger.info(
                        f"AudioProxyServer: outbound HEAD end url={stream.url} "
                        f"status={upstream.status} elapsed={time.monotonic() - fetch_start:.3f}s "
                        f"Content-Length={cl}"
                    )
        except Exception as e:
            # A probe answer without Content-Length is still useful; renderers
            # mostly check availability and type here.
            logger.debug(f"Upstream HEAD failed for track {track.track_id}: {e}")
        return web.Response(status=200, headers=headers)

    async def _proxy_stream(
        self,
        request: web.Request,
        track: RegisteredTrack,
    ) -> web.StreamResponse:
        """Proxy the audio stream from Qobuz CDN.

        If the upstream connection dies mid-stream (the CDN occasionally aborts
        long-running transfers with a short read), reconnect with a Range
        request from the last byte delivered to the client instead of dropping
        the renderer's stream mid-track.
        """
        stream = await self._resolve_stream(track)
        if stream is None:
            return web.Response(status=502, text="Failed to resolve streaming URL")

        # Build headers for upstream request
        headers: Dict[str, str] = {}

        # Forward Range header for seeking support
        range_header = request.headers.get("Range")
        request_start = 0
        if range_header:
            headers["Range"] = range_header
            request_start = _parse_range_start(range_header)
            logger.debug(f"Proxying with Range: {range_header}")

        # No total timeout for streaming, but a per-read timeout so a stalled
        # CDN connection fails fast instead of hanging forever.
        timeout = ClientTimeout(total=None, connect=30, sock_read=READ_TIMEOUT_SECONDS)

        response: Optional[web.StreamResponse] = None
        expected_bytes: Optional[int] = None
        is_range = range_header is not None
        stream_start = time.monotonic()
        bytes_sent = 0
        retries = 0
        expired_url_retry_done = False

        while True:
            upstream_headers = dict(headers)
            if bytes_sent:
                upstream_headers["Range"] = f"bytes={request_start + bytes_sent}-"

            try:
                logger.debug(
                    f"Connecting to upstream URL for track {track.track_id}: {stream.url[:100]}..."
                )
                # TEMP DEBUG LOGGING
                fetch_start = time.monotonic()
                logger.info(
                    f"AudioProxyServer: outbound GET start url={stream.url} "
                    f"Range={upstream_headers.get('Range')!r}"
                )
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(
                        stream.url,
                        headers=upstream_headers,
                    ) as upstream_response:
                        # TEMP DEBUG LOGGING
                        logger.info(
                            f"AudioProxyServer: outbound GET headers received "
                            f"url={stream.url} Range={upstream_headers.get('Range')!r} "
                            f"status={upstream_response.status} "
                            f"elapsed={time.monotonic() - fetch_start:.3f}s"
                        )
                        if upstream_response.status not in (200, 206):
                            # An expired signed URL surfaces as an error *status*
                            # (401/403/410), not an exception — refresh and retry
                            # once before failing the renderer's request.
                            if (
                                response is None
                                and not expired_url_retry_done
                                and upstream_response.status in (401, 403, 410)
                            ):
                                expired_url_retry_done = True
                                logger.info(
                                    f"Upstream {upstream_response.status} for track "
                                    f"{track.track_id} — URL likely expired; fetching "
                                    "a fresh URL and retrying"
                                )
                                refreshed = await self._resolve_stream(track, force=True)
                                if refreshed is not None:
                                    stream = refreshed
                                    continue
                            logger.warning(
                                f"Upstream error for track {track.track_id}: "
                                f"{upstream_response.status}"
                            )
                            if response is None:
                                return web.Response(
                                    status=502,
                                    text=f"Upstream error: {upstream_response.status}",
                                )
                            # Headers already sent — nothing more we can do
                            return response

                        if bytes_sent and upstream_response.status != 206:
                            # Upstream ignored our resume Range; restarting from
                            # byte 0 would corrupt the audio stream
                            logger.error(
                                f"Upstream ignored resume Range for track {track.track_id}; "
                                "aborting stream"
                            )
                            assert response is not None
                            return response

                        if response is None:
                            # First successful connection: send headers to client
                            status = 206 if upstream_response.status == 206 else 200
                            response_headers: Dict[str, str] = {
                                "Content-Type": track.content_type,
                                "Accept-Ranges": "bytes",
                            }
                            if "Content-Length" in upstream_response.headers:
                                cl = upstream_response.headers["Content-Length"]
                                response_headers["Content-Length"] = cl
                                expected_bytes = int(cl) if cl.isdigit() else None
                            if "Content-Range" in upstream_response.headers:
                                response_headers["Content-Range"] = upstream_response.headers[
                                    "Content-Range"
                                ]

                            logger.debug(
                                f"Streaming track {track.track_id}, headers: {response_headers}"
                            )
                            response = web.StreamResponse(
                                status=status,
                                headers=response_headers,
                            )
                            await response.prepare(request)

                        # Stream chunks to client
                        async for chunk in upstream_response.content.iter_chunked(
                            STREAM_CHUNK_SIZE
                        ):
                            try:
                                await response.write(chunk)
                                bytes_sent += len(chunk)
                            except (ConnectionResetError, ConnectionError):
                                self._log_stream_end(
                                    track=track,
                                    url=stream.url,
                                    bytes_sent=bytes_sent,
                                    expected_bytes=expected_bytes,
                                    elapsed=time.monotonic() - stream_start,
                                    is_range=is_range,
                                    completed=False,
                                )
                                return response

                await response.write_eof()
                self._log_stream_end(
                    track=track,
                    url=stream.url,
                    bytes_sent=bytes_sent,
                    expected_bytes=expected_bytes,
                    elapsed=time.monotonic() - stream_start,
                    is_range=is_range,
                    completed=True,
                )
                return response

            except asyncio.CancelledError:
                logger.debug(f"Stream cancelled for track {track.track_id}")
                raise
            except (ClientError, asyncio.TimeoutError) as e:
                # Upstream-side failure — retry with a Range resume
                retries += 1
                if retries > MAX_UPSTREAM_RETRIES:
                    logger.error(
                        f"Upstream failed for track {track.track_id} after "
                        f"{MAX_UPSTREAM_RETRIES} retries: {type(e).__name__}: {e}"
                    )
                    if response is None:
                        return web.Response(status=502, text=f"Upstream error: {e}")
                    self._log_stream_end(
                        track=track,
                        url=stream.url,
                        bytes_sent=bytes_sent,
                        expected_bytes=expected_bytes,
                        elapsed=time.monotonic() - stream_start,
                        is_range=is_range,
                        completed=False,
                    )
                    return response
                logger.warning(
                    f"Upstream connection lost for track {track.track_id} after "
                    f"{bytes_sent} bytes ({type(e).__name__}: {e}); "
                    f"reconnecting ({retries}/{MAX_UPSTREAM_RETRIES})"
                )
                await asyncio.sleep(UPSTREAM_RETRY_DELAY_SECONDS * retries)
                refreshed = await self._resolve_stream(track, force=True)
                if refreshed is not None:
                    stream = refreshed
            except (ConnectionResetError, ConnectionError) as e:
                # Client disconnected - this is normal when Sonos probes or seeks
                logger.debug(
                    f"Client connection closed for track {track.track_id}: {type(e).__name__}"
                )
                if response is not None:
                    return response
                return web.Response(status=499, text="Client closed connection")
            except Exception as e:
                logger.error(f"Proxy error for track {track.track_id}: {type(e).__name__}: {e}")
                logger.error(f"URL was: {stream.url[:100]}...")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
                if response is not None:
                    return response
                return web.Response(status=502, text=f"Proxy error: {e}")

    def _log_stream_end(
        self,
        track: RegisteredTrack,
        url: str,
        bytes_sent: int,
        expected_bytes: Optional[int],
        elapsed: float,
        is_range: bool,
        completed: bool,
    ) -> None:
        """Log the outcome of a proxied stream with throughput diagnostics.

        Distinguishes a genuine mid-stream drop (renderer gave up or its buffer
        underran) from benign disconnects (a Sonos probe, a seek, or a finished
        transfer). Average throughput is included so we can tell *which*: a low
        Mbit/s well under the stream bitrate points at the proxy/network not
        keeping the device's buffer full; a high rate that simply stops points
        at the renderer being unable to sustain decode/output at this quality.
        """
        rate_mbps = (bytes_sent * 8 / elapsed / 1_000_000) if elapsed > 0 else 0.0
        pct = f"{bytes_sent / expected_bytes * 100:.1f}%" if expected_bytes else "?"

        # TEMP DEBUG LOGGING
        logger.info(
            f"AudioProxyServer: outbound GET end url={url} completed={completed} "
            f"bytes_sent={bytes_sent} expected_bytes={expected_bytes} "
            f"elapsed={elapsed:.3f}s"
        )

        # A mid-transfer drop: a full-body request (not a range/seek) that ended
        # well short of the advertised length. This is the stutter/stop symptom.
        short = expected_bytes is not None and bytes_sent < expected_bytes * 0.95
        if not completed and short and not is_range:
            logger.warning(
                "Renderer dropped track %s mid-stream: sent %d/%d bytes (%s) in "
                "%.1fs (%.2f Mbit/s avg). Suspect buffer underrun or renderer "
                "cannot sustain this quality.",
                track.track_id,
                bytes_sent,
                expected_bytes,
                pct,
                elapsed,
                rate_mbps,
            )
        elif completed:
            logger.debug(
                "Finished streaming track %s: %d bytes in %.1fs (%.2f Mbit/s avg)",
                track.track_id,
                bytes_sent,
                elapsed,
                rate_mbps,
            )
        else:
            logger.debug(
                "Client disconnected after %d bytes (%s) for track %s "
                "(range=%s, %.1fs, %.2f Mbit/s avg)",
                bytes_sent,
                pct,
                track.track_id,
                is_range,
                elapsed,
                rate_mbps,
            )

    def _get_local_ip(self) -> str:
        """Get local IP address for proxy URL."""
        try:
            # Connect to external address to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

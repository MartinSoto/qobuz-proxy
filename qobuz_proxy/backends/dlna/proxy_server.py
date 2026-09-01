"""
Audio Proxy Server.

HTTP server that proxies audio streams from Qobuz CDN to DLNA devices. Owns
the format/transcoding decision transparently: given a track and a device's
capabilities, resolve_track() decides whether Qobuz's native format passes
straight through, gets downsampled on the fly, or falls back to a safe CD-
tier request — and hands back one proxy URL either way. The renderer never
knows or cares which; it just GETs a URL.

All actual Qobuz CDN URL fetching/caching lives one layer down, in
QobuzStreamResolver (see playback/stream_resolver.py) — this class never
talks to the Qobuz API directly, only to that shared resolver.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
import traceback
from dataclasses import dataclass
from typing import Callable, Dict, Optional, TYPE_CHECKING

from aiohttp import web, ClientError, ClientSession, ClientTimeout

from .capabilities import (
    DLNACapabilities,
    QOBUZ_QUALITY_MP3,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_192K,
)
from .transcoding_reader import BYTES_PER_SAMPLE_24BIT, WAV_HEADER_SIZE, TranscodingFlacReader

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
    every request that needs one, so this dataclass never goes stale."""

    track_id: str
    format_id: int
    content_type: str
    # Set when this track's native format exceeds what the device can
    # actually handle (see resolve_track) — served via TranscodingFlacReader
    # as downsampled PCM/WAV instead of a plain pass-through of the source
    # FLAC. None is the common case: pass through unchanged.
    transcode_to_sample_rate: Optional[int] = None


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
    - Format/quality decisions (resolve_track) and on-the-fly downsampling
    - Qobuz URL expiration (via the shared QobuzStreamResolver's own cache)
    - HTTP range requests for seeking
    - Streaming without full buffering

    Usage:
        proxy = AudioProxyServer(resolver=stream_resolver, host="0.0.0.0", port=7120)
        await proxy.start()

        resolved = await proxy.resolve_track(track_id, capabilities, hires_downsampling=True)
        # resolved.proxy_url = "http://192.168.1.100:7120/audio/12345.flac" (or .wav)

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

        # Cooperative stream supersession — see _claim_stream/_stream_superseded.
        self._active_streams: Dict[str, tuple[int, web.Request]] = {}
        self._next_generation = 0

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
        self._active_streams.clear()
        logger.info("Audio proxy server stopped")

    # =========================================================================
    # Format/quality resolution
    # =========================================================================

    @staticmethod
    def _ceiling_tier_for(capabilities: Optional[DLNACapabilities]) -> int:
        """The initial Qobuz format tier to request for a track.

        Deliberately the highest tier the device's *format* (FLAC support,
        bit depth) could ever make use of, ignoring its sample-rate cap —
        so the fits/downsample/fallback decision in resolve_track() is
        based on the track's true native format (what Qobuz actually hands
        back), not a capability-clamped guess. A device with real 24-bit
        support always gets asked for the 192k ceiling even if its own
        max_sample_rate is much lower: many hi-res tracks are natively
        mastered at or below that cap anyway, and on the ones that aren't,
        resolve_track downsamples on the fly instead of falling all the
        way back to 16-bit.
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
        hires_downsampling: bool,
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
        fit and on-the-fly downsampling is enabled, transcode it down to
        the device's own sample-rate cap. Otherwise, fall back to a plain
        CD-quality (16/44) request — always within reach of anything that
        speaks FLAC at all.

        Args:
            track_id: Qobuz track ID
            capabilities: Device capabilities (None if never discovered)
            hires_downsampling: Whether on-the-fly downsampling is enabled
            proxy_key: Optional key for the proxy URL path (defaults to
                track_id). Use a unique key like "{track_id}_{queue_item_id}"
                to produce distinct proxy URLs for duplicate tracks in a
                queue (gapless preload).
            forced_format_id: When set, used as the initial request instead
                of the capability-derived ceiling (a manual/app-driven
                quality override) — the fits/transcode/fallback decision
                still applies on top of it.

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

        if (
            hires_downsampling
            and capabilities is not None
            and capabilities.supports_flac
            and capabilities.max_bit_depth >= 24
        ):
            return self._register_transcode(
                track_id, stream, capabilities.max_sample_rate, proxy_key
            )

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

    def _register_transcode(
        self,
        track_id: str,
        stream: "ResolvedStream",
        target_sample_rate: int,
        proxy_key: Optional[str],
    ) -> ResolvedTrack:
        key = proxy_key or track_id
        self._tracks[key] = RegisteredTrack(
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
    # Cooperative stream supersession
    # =========================================================================

    def _claim_stream(self, proxy_key: str, request: web.Request) -> int:
        """Claim the right to serve `proxy_key`'s stream, superseding
        whatever request (if any) was previously claiming it.

        A renderer that fires a new request for a track it's already
        streaming reproducibly does so on *every* seek: a GET-before-Range
        probe immediately followed by the real Range request, and/or the
        previous seek's connection still draining. For however long it
        keeps both sockets open, it's genuinely receiving two different,
        both-valid positions of the same track at once — so the previous
        request's transport is force-closed immediately here, rather than
        only marked stale for its own loop to notice on its own schedule.

        Returns a generation id the caller must check via
        _stream_superseded() between its own loop iterations — never mid-
        call — to stop cleanly once superseded. This never touches the
        generator/thread doing decode work, only the socket, so it can't
        reintroduce the "generator already executing" crash a prior
        Task.cancel()-based approach caused.
        """
        self._next_generation += 1
        generation = self._next_generation

        previous = self._active_streams.get(proxy_key)
        self._active_streams[proxy_key] = (generation, request)

        if previous is not None:
            _, previous_request = previous
            if previous_request is not request:
                transport = previous_request.transport
                if transport is not None and not transport.is_closing():
                    transport.close()

        return generation

    def _stream_superseded(self, proxy_key: str, generation: int) -> bool:
        """Whether a newer request has claimed proxy_key since `generation`
        was issued (or the key was never claimed at all)."""
        current = self._active_streams.get(proxy_key)
        if current is None:
            return True
        current_generation, _ = current
        return current_generation != generation

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
            f"range={request.headers.get('Range')!r} "
            f"transcode={track.transcode_to_sample_rate}"
        )

        if track.transcode_to_sample_rate is not None:
            if request.method == "HEAD":
                return await self._handle_transcoded_head_probe(track)
            return await self._transcode_stream(request, track, proxy_key)

        # HEAD probes (Denon/HEOS send one before every GET) route here too via
        # add_get's implicit HEAD support. Answer them headers-only — streaming
        # the body just to have aiohttp discard it downloads the whole track
        # from the CDN.
        if request.method == "HEAD":
            return await self._handle_head_probe(track)

        # Forward request to Qobuz CDN
        return await self._proxy_stream(request, track, proxy_key)

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
            async with ClientSession(timeout=timeout) as session:
                async with session.head(stream.url, allow_redirects=True) as upstream:
                    cl = upstream.headers.get("Content-Length")
                    if upstream.status in (200, 206) and cl and cl.isdigit():
                        headers["Content-Length"] = cl
        except Exception as e:
            # A probe answer without Content-Length is still useful; renderers
            # mostly check availability and type here.
            logger.debug(f"Upstream HEAD failed for track {track.track_id}: {e}")
        return web.Response(status=200, headers=headers)

    def _make_sync_url_refresher(self, track: RegisteredTrack) -> Callable[[], Optional[str]]:
        """A synchronous callable, safe to invoke from a worker thread, that
        refreshes the track's CDN URL via the (async) resolver and returns
        the new URL. Bridges LazyHttpFlacSource's synchronous retry-on-
        expired-URL path (see its module docstring) back onto the event
        loop, where the real refresh happens — long tracks can easily
        outlive a signed URL's TTL mid-stream, same risk _proxy_stream
        already handles for the pass-through path.
        """
        loop = asyncio.get_event_loop()

        def _refresh() -> Optional[str]:
            future = asyncio.run_coroutine_threadsafe(self._resolve_stream(track, force=True), loop)
            try:
                refreshed = future.result(timeout=REQUEST_TIMEOUT_SECONDS)
            except Exception as e:
                logger.warning(
                    f"URL refresh (from transcode thread) failed for track {track.track_id}: {e}"
                )
                return None
            return refreshed.url if refreshed else None

        return _refresh

    async def _handle_transcoded_head_probe(self, track: RegisteredTrack) -> web.Response:
        """Answer a HEAD probe for a downsampled track with the *virtual*
        (post-transcode) WAV's Content-Length, computed from the source's
        STREAMINFO alone — cheap, no decoding."""
        headers = {"Content-Type": "audio/wav", "Accept-Ranges": "bytes"}
        assert track.transcode_to_sample_rate is not None
        stream = await self._resolve_stream(track)
        if stream is None:
            return web.Response(status=200, headers=headers)
        try:
            reader = await asyncio.to_thread(
                TranscodingFlacReader,
                stream.url,
                track.transcode_to_sample_rate,
                refresh_url=self._make_sync_url_refresher(track),
            )
            headers["Content-Length"] = str(reader.content_length)
            logger.debug(
                f"Transcoded HEAD probe for track {track.track_id}: "
                f"Content-Length={reader.content_length}"
            )
        except Exception as e:
            logger.debug(f"Transcoded HEAD probe failed for track {track.track_id}: {e}")
        return web.Response(status=200, headers=headers)

    async def _transcode_stream(
        self,
        request: web.Request,
        track: RegisteredTrack,
        proxy_key: str,
    ) -> web.StreamResponse:
        """Serve a track downsampled to track.transcode_to_sample_rate as
        PCM/WAV — see TranscodingFlacReader. A fresh reader is opened per
        request (a couple of small upstream requests: STREAMINFO, then a
        seek if this is a Range request) rather than cached across
        requests — simpler, and normal playback of one track is one
        long-lived GET plus at most a few seeks, not a flood of tiny reads.
        """
        assert track.transcode_to_sample_rate is not None
        generation = self._claim_stream(proxy_key, request)

        stream = await self._resolve_stream(track)
        if stream is None:
            return web.Response(status=502, text="Failed to resolve streaming URL")

        try:
            reader = await asyncio.to_thread(
                TranscodingFlacReader,
                stream.url,
                track.transcode_to_sample_rate,
                refresh_url=self._make_sync_url_refresher(track),
            )
        except Exception as e:
            logger.error(f"Failed to open transcoding source for track {track.track_id}: {e}")
            return web.Response(status=502, text="Failed to open source stream")

        range_header = request.headers.get("Range")
        start_byte = _parse_range_start(range_header) if range_header else 0
        start_byte = max(0, min(start_byte, reader.content_length))

        # We're simulating a plain static file on disk — the same thing a
        # dumb NAS would serve. The renderer parsed our WAV header once (it
        # always fetches from byte 0 first) and finds its own alignment
        # from there; a static file server never needs to "help" with
        # that, it only ever needs to hand back the literal bytes that
        # exist at the requested offset. So: whatever byte the renderer
        # asks for, that's exactly what we declare *and* exactly what the
        # response starts with — never a rounded/corrected position.
        #
        # The one thing we can't avoid: our decoder only ever produces
        # whole PCM frames, so reconstructing the *true* bytes at an
        # arbitrary offset means decoding from the containing frame and
        # discarding the leading bytes that fall before the requested
        # byte before writing anything out — exactly what reading an
        # arbitrary byte offset from a real file on disk would hand back,
        # nothing more.
        bytes_per_frame = reader.channels * BYTES_PER_SAMPLE_24BIT
        if start_byte < WAV_HEADER_SIZE:
            aligned_start_byte = start_byte
        else:
            data_offset = start_byte - WAV_HEADER_SIZE
            aligned_start_byte = (
                WAV_HEADER_SIZE + (data_offset // bytes_per_frame) * bytes_per_frame
            )
        leading_trim = start_byte - aligned_start_byte

        remaining = reader.content_length - start_byte

        headers = {"Content-Type": "audio/wav", "Accept-Ranges": "bytes"}
        if range_header:
            headers["Content-Range"] = (
                f"bytes {start_byte}-{max(start_byte, reader.content_length - 1)}"
                f"/{reader.content_length}"
            )
        response = web.StreamResponse(status=206 if range_header else 200, headers=headers)
        response.content_length = remaining

        bytes_written = 0
        stream_start = time.monotonic()
        outcome = "completed"
        try:
            # response.prepare() can itself raise if this request's
            # transport was already force-closed by a newer claim on the
            # same key while we were still opening the reader above (see
            # _claim_stream) — must be caught here, not left to escape the
            # handler.
            await response.prepare(request)

            # Drive the reader's (synchronous, blocking) generator one step
            # at a time from a worker thread — each to_thread(next, ...)
            # call runs entirely inside that call, so if a newer request
            # supersedes this one and we simply stop calling next(), nothing
            # is left running in the background (no orphaned thread, no
            # stuck queue), and gen.close() below never races a generator
            # still genuinely executing.
            gen = reader.stream_from(aligned_start_byte)
            try:
                while True:
                    if self._stream_superseded(proxy_key, generation):
                        outcome = "superseded"
                        break
                    chunk = await asyncio.to_thread(next, gen, None)
                    if chunk is None:
                        break
                    if self._stream_superseded(proxy_key, generation):
                        outcome = "superseded"
                        break
                    if leading_trim:
                        if len(chunk) <= leading_trim:
                            leading_trim -= len(chunk)
                            continue
                        chunk = chunk[leading_trim:]
                        leading_trim = 0
                    await response.write(chunk)
                    bytes_written += len(chunk)
            finally:
                gen.close()
        except (ConnectionResetError, asyncio.CancelledError):
            outcome = "disconnected"
            logger.debug(f"Client disconnected mid-transcode for track {track.track_id}")
        except Exception as e:
            outcome = "error"
            logger.error(f"Transcoding stream error for track {track.track_id}: {e}")

        logger.debug(
            f"Transcode response for track {track.track_id} done: {outcome}, "
            f"{bytes_written} bytes in {time.monotonic() - stream_start:.1f}s"
        )
        return response

    async def _proxy_stream(
        self,
        request: web.Request,
        track: RegisteredTrack,
        proxy_key: str,
    ) -> web.StreamResponse:
        """Proxy the audio stream from Qobuz CDN.

        If the upstream connection dies mid-stream (the CDN occasionally aborts
        long-running transfers with a short read), reconnect with a Range
        request from the last byte delivered to the client instead of dropping
        the renderer's stream mid-track.
        """
        generation = self._claim_stream(proxy_key, request)

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
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(
                        stream.url,
                        headers=upstream_headers,
                    ) as upstream_response:
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
                            if self._stream_superseded(proxy_key, generation):
                                logger.debug(
                                    f"Stream for track {track.track_id} superseded by a "
                                    "newer request; stopping"
                                )
                                return response
                            try:
                                await response.write(chunk)
                                bytes_sent += len(chunk)
                            except (ConnectionResetError, ConnectionError):
                                self._log_stream_end(
                                    track=track,
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

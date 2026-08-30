"""Validates the core assumption behind hi-res downsampling: soundfile
(libFLAC) can seek within a FLAC file served only through CDNBlockCache-
backed Range requests — using its own seektable/frame-search logic —
without us downloading the file up front, and without any FLAC-specific
code of our own. See lazy_flac_source.py's module docstring for the full
rationale.

All of the actual CDN behavior LazyHttpFlacSource used to own directly
(retry-on-transient-failure, refresh-on-expired-URL, connection reuse) now
lives in CDNBlockCache and is covered by test_dlna_cdn_block_cache.py —
this file only has to prove the bridge itself: that a decoder driving
LazyHttpFlacSource gets correct bytes at the right positions, without a
full download, and that two sources sharing one cache actually share its
benefit.

This spins up a real local HTTP server and does a real FLAC encode/decode
round trip (no mocks on the decode path) — the thing being validated here
is genuinely "does this work," not "does this call the right mock."
"""

import asyncio
import io
import socket
from unittest.mock import AsyncMock

import numpy as np
import pytest
import soundfile as sf
from aiohttp import web

from qobuz_proxy.backends.dlna.cdn_block_cache import CDNBlockCache, CDNBlockFetchError
from qobuz_proxy.backends.dlna.lazy_flac_source import LazyHttpFlacSource
from qobuz_proxy.playback.stream_resolver import ResolvedStream

# LazyHttpFlacSource does blocking I/O (bridged onto the event loop via
# run_coroutine_threadsafe), same as the real usage (always from a worker
# thread — see its module docstring). The test server it talks to runs on
# *this* event loop, so every call into the source (or anything that
# drives it, like sf.SoundFile) must go through a thread here too, or the
# blocking bridge call would deadlock waiting for a response only this
# same, now-blocked, event loop could ever produce.

SAMPLE_RATE = 48000
DURATION_SECONDS = 2.0
CHANNELS = 2
FORMAT_ID = 6  # arbitrary — these tests don't exercise quality selection
BLOCK_SIZE = 32 * 1024


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_test_flac() -> tuple[bytes, np.ndarray]:
    """A short, deterministic stereo FLAC — a different tone per channel so
    a decoded slice can be checked against the exact source position, not
    just "some" audio."""
    n = int(DURATION_SECONDS * SAMPLE_RATE)
    t = np.arange(n) / SAMPLE_RATE
    left = np.sin(2 * np.pi * 440.0 * t)
    right = np.sin(2 * np.pi * 880.0 * t)
    audio = np.stack([left, right], axis=1).astype("float32")

    buf = io.BytesIO()
    sf.write(buf, audio, SAMPLE_RATE, format="FLAC", subtype="PCM_24")
    return buf.getvalue(), audio


def _stream(url: str) -> ResolvedStream:
    return ResolvedStream(
        url=url, blob="", format_id=FORMAT_ID, sample_rate=44100, bit_depth=16, fetched_at=0.0
    )


def _resolver_for(url: str) -> AsyncMock:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(url))
    return resolver


class RangeServingUpstream:
    """A real HTTP server serving one fixed payload with standard Range
    support — a stand-in for a Qobuz-CDN-style signed streaming URL."""

    def __init__(self, payload: bytes):
        self._payload = payload
        self._runner: web.AppRunner | None = None
        self.requests: list[tuple[str, str | None]] = []  # (method, Range header)

    async def _handle(self, request: web.Request) -> web.Response:
        rng = request.headers.get("Range")
        self.requests.append((request.method, rng))

        if request.method == "HEAD":
            return web.Response(
                status=200,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(len(self._payload)),
                },
            )

        start = int(rng.removeprefix("bytes=").split("-")[0]) if rng else 0
        body = self._payload[start:]
        status = 206 if rng else 200
        headers = {"Accept-Ranges": "bytes", "Content-Length": str(len(body))}
        if rng:
            headers["Content-Range"] = (
                f"bytes {start}-{len(self._payload) - 1}/{len(self._payload)}"
            )
        return web.Response(status=status, body=body, headers=headers)

    async def start(self) -> str:
        app = web.Application()
        # add_get already registers a HEAD route (routed to the same
        # handler) — an explicit add_route("HEAD", ...) would collide with it.
        app.router.add_get("/file.flac", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        port = _free_port()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        return f"http://127.0.0.1:{port}/file.flac"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


def _new_source(cache: CDNBlockCache, loop: asyncio.AbstractEventLoop, track_id: str = "42"):
    return LazyHttpFlacSource(cache, track_id, FORMAT_ID, loop)


class TestLazyHttpFlacSource:
    async def test_sequential_read_decodes_correctly(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            cache = CDNBlockCache(resolver=_resolver_for(url), block_size=BLOCK_SIZE)
            loop = asyncio.get_event_loop()

            def _read() -> tuple[np.ndarray, int]:
                source = _new_source(cache, loop)
                handle = sf.SoundFile(source)
                return handle.read(5000, dtype="float32"), handle.frames

            decoded, frames = await asyncio.to_thread(_read)

            assert frames == len(original)
            np.testing.assert_allclose(decoded, original[:5000], atol=2e-4)
        finally:
            await cache.close()
            await server.stop()

    async def test_seek_reads_the_correct_position_without_full_download(self) -> None:
        """The actual thing this whole feature hinges on: seeking must land
        on the real source position, and must not have required fetching
        the whole file to get there."""
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            cache = CDNBlockCache(resolver=_resolver_for(url), block_size=BLOCK_SIZE)
            loop = asyncio.get_event_loop()
            target_frame = int(len(original) * 0.6)  # well past the header

            def _seek_and_read() -> np.ndarray:
                source = _new_source(cache, loop)
                handle = sf.SoundFile(source)
                handle.seek(target_frame)
                return handle.read(2000, dtype="float32")

            decoded = await asyncio.to_thread(_seek_and_read)

            np.testing.assert_allclose(
                decoded, original[target_frame : target_frame + 2000], atol=2e-4
            )
            # What actually matters: how much of the file our own request
            # pattern chose to fetch and retain — not bytes physically over
            # the wire, which loopback/OS buffering can make look complete
            # even when the client stopped consuming immediately (an open
            # connection's remaining body can fully land in the kernel
            # socket buffer for a file this small, well before this test
            # ever decides to abandon it).
            assert cache.cached_bytes < len(flac_bytes) * 0.5, (
                f"expected a partial fetch, but cached {cache.cached_bytes} of "
                f"{len(flac_bytes)} bytes — looks like the whole file got fetched"
            )
        finally:
            await cache.close()
            await server.stop()

    async def test_multiple_seeks_each_land_correctly(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            cache = CDNBlockCache(resolver=_resolver_for(url), block_size=BLOCK_SIZE)
            loop = asyncio.get_event_loop()

            def _seek_around() -> list[np.ndarray]:
                source = _new_source(cache, loop)
                handle = sf.SoundFile(source)
                results = []
                for frac in (0.1, 0.75, 0.3, 0.9):
                    target_frame = int(len(original) * frac)
                    handle.seek(target_frame)
                    results.append(handle.read(1000, dtype="float32"))
                return results

            decoded_chunks = await asyncio.to_thread(_seek_around)

            for frac, decoded in zip((0.1, 0.75, 0.3, 0.9), decoded_chunks):
                target_frame = int(len(original) * frac)
                np.testing.assert_allclose(
                    decoded, original[target_frame : target_frame + 1000], atol=2e-4
                )
        finally:
            await cache.close()
            await server.stop()

    async def test_total_size_matches_content_length(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            cache = CDNBlockCache(resolver=_resolver_for(url), block_size=BLOCK_SIZE)
            loop = asyncio.get_event_loop()
            source = await asyncio.to_thread(_new_source, cache, loop)
            assert source._total_size == len(flac_bytes)
            assert any(method == "HEAD" for method, _rng in server.requests)
        finally:
            await cache.close()
            await server.stop()

    async def test_reads_past_eof_return_empty(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            cache = CDNBlockCache(resolver=_resolver_for(url), block_size=BLOCK_SIZE)
            loop = asyncio.get_event_loop()

            def _read_past_eof() -> bytes:
                source = _new_source(cache, loop)
                source.seek(0, 2)  # SEEK_END
                return source.read(100)

            assert await asyncio.to_thread(_read_past_eof) == b""
        finally:
            await cache.close()
            await server.stop()

    async def test_missing_content_length_raises(self) -> None:
        app = web.Application()

        async def _no_length(request: web.Request) -> web.StreamResponse:
            # Chunked transfer encoding — aiohttp omits Content-Length for
            # this, unlike a plain Response (which always computes one).
            resp = web.StreamResponse(status=200)
            resp.enable_chunked_encoding()
            await resp.prepare(request)
            await resp.write(b"x")
            return resp

        app.router.add_get("/file.flac", _no_length)
        runner = web.AppRunner(app)
        await runner.setup()
        port = _free_port()
        site = web.TCPSite(runner, "127.0.0.1", port)
        await site.start()
        try:
            cache = CDNBlockCache(
                resolver=_resolver_for(f"http://127.0.0.1:{port}/file.flac"),
                block_size=BLOCK_SIZE,
            )
            loop = asyncio.get_event_loop()
            with pytest.raises(CDNBlockFetchError):
                await asyncio.to_thread(_new_source, cache, loop)
        finally:
            await cache.close()
            await runner.cleanup()


class TestSharedCacheAcrossSources:
    """The reason CDNBlockCache moved out from under one LazyHttpFlacSource
    at all: two independent sources for the same (track_id, format_id)
    should actually share fetched blocks, not each pay for their own."""

    async def test_second_source_reuses_the_first_sources_cached_blocks(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            cache = CDNBlockCache(resolver=_resolver_for(url), block_size=BLOCK_SIZE)
            loop = asyncio.get_event_loop()

            def _read_from_zero() -> np.ndarray:
                source = _new_source(cache, loop)
                handle = sf.SoundFile(source)
                return handle.read(2000, dtype="float32")

            first = await asyncio.to_thread(_read_from_zero)
            np.testing.assert_allclose(first, original[:2000], atol=2e-4)

            requests_before = len(server.requests)

            second = await asyncio.to_thread(_read_from_zero)
            np.testing.assert_allclose(second, original[:2000], atol=2e-4)

            # The second source's HEAD (size discovery) and initial block
            # read both hit the cache — no new upstream request at all.
            assert len(server.requests) == requests_before
        finally:
            await cache.close()
            await server.stop()

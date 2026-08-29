"""Validates the core assumption behind hi-res downsampling: soundfile
(libFLAC) can seek within a FLAC file served only through HTTP Range
requests — using its own seektable/frame-search logic — without us
downloading the file up front, and without any FLAC-specific code of our
own. See lazy_flac_source.py's module docstring for the full rationale.

This spins up a real local HTTP server and does a real FLAC encode/decode
round trip (no mocks on the decode path) — the thing being validated here
is genuinely "does this work," not "does this call the right mock."
"""

import asyncio
import io
import socket
import urllib.error

import numpy as np
import pytest
import soundfile as sf
from aiohttp import web

from qobuz_proxy.backends.dlna.lazy_flac_source import LazyHttpFlacSource

# LazyHttpFlacSource does blocking I/O (urllib), same as the real usage
# (always from a worker thread — see its module docstring). The test server
# it talks to runs on *this* event loop, so every call into the source (or
# anything that drives it, like sf.SoundFile) must go through a thread here
# too, or the blocking client call would deadlock waiting for a response
# only this same, now-blocked, event loop could ever produce.

SAMPLE_RATE = 48000
DURATION_SECONDS = 2.0
CHANNELS = 2


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

        if not rng:
            return web.Response(
                status=200,
                body=self._payload,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(len(self._payload)),
                },
            )

        start_s, end_s = rng.removeprefix("bytes=").split("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(self._payload) - 1
        end = min(end, len(self._payload) - 1)
        body = self._payload[start : end + 1]
        return web.Response(
            status=206,
            body=body,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(len(body)),
                "Content-Range": f"bytes {start}-{end}/{len(self._payload)}",
            },
        )

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

    @property
    def bytes_served(self) -> int:
        return sum(
            len(self._payload) if rng is None else _range_len(rng, len(self._payload))
            for method, rng in self.requests
            if method == "GET"
        )


def _range_len(range_header: str, total: int) -> int:
    start_s, end_s = range_header.removeprefix("bytes=").split("-")
    start = int(start_s)
    end = int(end_s) if end_s else total - 1
    return min(end, total - 1) - start + 1


class TestLazyHttpFlacSource:
    async def test_sequential_read_decodes_correctly(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:

            def _read() -> tuple[np.ndarray, int]:
                source = LazyHttpFlacSource(url, chunk_size=32 * 1024)
                handle = sf.SoundFile(source)
                return handle.read(5000, dtype="float32"), handle.frames

            decoded, frames = await asyncio.to_thread(_read)

            assert frames == len(original)
            np.testing.assert_allclose(decoded, original[:5000], atol=2e-4)
        finally:
            await server.stop()

    async def test_seek_reads_the_correct_position_without_full_download(self) -> None:
        """The actual thing this whole feature hinges on: seeking must land
        on the real source position, and must not have required fetching
        the whole file to get there."""
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            target_frame = int(len(original) * 0.6)  # well past the header

            def _seek_and_read() -> np.ndarray:
                source = LazyHttpFlacSource(url, chunk_size=32 * 1024)
                handle = sf.SoundFile(source)
                handle.seek(target_frame)
                return handle.read(2000, dtype="float32")

            decoded = await asyncio.to_thread(_seek_and_read)

            np.testing.assert_allclose(
                decoded, original[target_frame : target_frame + 2000], atol=2e-4
            )
            assert server.bytes_served < len(flac_bytes) * 0.5, (
                f"expected a partial fetch, but served {server.bytes_served} of "
                f"{len(flac_bytes)} bytes — looks like a full download happened"
            )
        finally:
            await server.stop()

    async def test_multiple_seeks_each_land_correctly(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:

            def _seek_around() -> list[np.ndarray]:
                source = LazyHttpFlacSource(url, chunk_size=32 * 1024)
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
            await server.stop()

    async def test_total_size_matches_content_length(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:
            source = await asyncio.to_thread(LazyHttpFlacSource, url)
            assert source._total_size == len(flac_bytes)
            assert any(method == "HEAD" for method, _rng in server.requests)
        finally:
            await server.stop()

    async def test_reads_past_eof_return_empty(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        try:

            def _read_past_eof() -> bytes:
                source = LazyHttpFlacSource(url)
                source.seek(0, 2)  # SEEK_END
                return source.read(100)

            assert await asyncio.to_thread(_read_past_eof) == b""
        finally:
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
            with pytest.raises(OSError):
                await asyncio.to_thread(LazyHttpFlacSource, f"http://127.0.0.1:{port}/file.flac")
        finally:
            await runner.cleanup()


class ExpiringUpstream:
    """A stand-in for a Qobuz signed URL that has gone stale: rejects the
    original token with 403 (the actual status Qobuz's CDN uses for an
    expired signature) but serves a fresh one normally — same shape as
    test_proxy_server.py's ExpiredUrlUpstream, for the decode side."""

    def __init__(self, payload: bytes):
        self._payload = payload
        self._runner: web.AppRunner | None = None
        self.rejected_requests = 0

    async def _handle(self, request: web.Request) -> web.Response:
        if request.query.get("token") != "fresh":
            self.rejected_requests += 1
            return web.Response(status=403, text="expired")

        rng = request.headers.get("Range")
        if request.method == "HEAD":
            return web.Response(
                status=200,
                headers={"Accept-Ranges": "bytes", "Content-Length": str(len(self._payload))},
            )
        if not rng:
            return web.Response(
                status=200,
                body=self._payload,
                headers={"Accept-Ranges": "bytes", "Content-Length": str(len(self._payload))},
            )
        start_s, end_s = rng.removeprefix("bytes=").split("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(self._payload) - 1
        end = min(end, len(self._payload) - 1)
        body = self._payload[start : end + 1]
        return web.Response(
            status=206,
            body=body,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(len(body)),
                "Content-Range": f"bytes {start}-{end}/{len(self._payload)}",
            },
        )

    async def start(self) -> str:
        app = web.Application()
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


class TestUrlRefreshOnExpiry:
    """A long track can easily outlive a Qobuz signed URL's TTL mid-stream
    — see lazy_flac_source.py's EXPIRED_URL_STATUS_CODES and
    proxy_server.py's _make_sync_url_refresher for the full rationale."""

    async def test_refreshes_and_retries_once_on_expired_status(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = ExpiringUpstream(flac_bytes)
        stale_url = await server.start()
        fresh_url = f"{stale_url}?token=fresh"
        refresh_calls = 0

        def refresh() -> str:
            nonlocal refresh_calls
            refresh_calls += 1
            return fresh_url

        try:
            source = await asyncio.to_thread(
                LazyHttpFlacSource, f"{stale_url}?token=stale", refresh_url=refresh
            )
            assert source._total_size == len(flac_bytes)
            assert refresh_calls == 1
            assert server.rejected_requests == 1  # the one stale HEAD, then success
        finally:
            await server.stop()

    async def test_gives_up_without_a_refresh_callback(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = ExpiringUpstream(flac_bytes)
        stale_url = await server.start()

        try:
            with pytest.raises(urllib.error.HTTPError):
                await asyncio.to_thread(LazyHttpFlacSource, f"{stale_url}?token=stale")
        finally:
            await server.stop()

    async def test_gives_up_when_refresh_returns_nothing(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = ExpiringUpstream(flac_bytes)
        stale_url = await server.start()

        try:
            with pytest.raises(urllib.error.HTTPError):
                await asyncio.to_thread(
                    LazyHttpFlacSource, f"{stale_url}?token=stale", refresh_url=lambda: None
                )
        finally:
            await server.stop()


class FlakyThenHealthyUpstream:
    """Fails the first N requests with a connection reset, then serves
    normally — a stand-in for a transient CDN blip (dropped connection,
    a momentary 502/503) rather than a genuinely dead URL."""

    def __init__(self, payload: bytes, fail_count: int):
        self._payload = payload
        self._fail_count = fail_count
        self._seen = 0
        self._runner: web.AppRunner | None = None

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        self._seen += 1
        if self._seen <= self._fail_count:
            # Abruptly close the connection without a valid HTTP response —
            # surfaces to urllib as a connection-level failure, not a
            # parseable HTTP status.
            assert request.transport is not None
            request.transport.close()
            return web.Response(status=499)

        rng = request.headers.get("Range")
        if request.method == "HEAD":
            return web.Response(
                status=200,
                headers={"Accept-Ranges": "bytes", "Content-Length": str(len(self._payload))},
            )
        if not rng:
            return web.Response(
                status=200,
                body=self._payload,
                headers={"Accept-Ranges": "bytes", "Content-Length": str(len(self._payload))},
            )
        start_s, end_s = rng.removeprefix("bytes=").split("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(self._payload) - 1
        end = min(end, len(self._payload) - 1)
        body = self._payload[start : end + 1]
        return web.Response(
            status=206,
            body=body,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(len(body)),
                "Content-Range": f"bytes {start}-{end}/{len(self._payload)}",
            },
        )

    async def start(self) -> str:
        app = web.Application()
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


class AlwaysFailingUpstream:
    """Rejects every single request with a connection reset — a genuinely
    persistent failure, never recovers."""

    def __init__(self) -> None:
        self._runner: web.AppRunner | None = None
        self.request_count = 0

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        self.request_count += 1
        assert request.transport is not None
        request.transport.close()
        return web.Response(status=499)

    async def start(self) -> str:
        app = web.Application()
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


class TestTransientConnectionRetry:
    """A dropped connection, a stalled socket, a momentary CDN 5xx — none
    of these mean the URL is dead, unlike an expired-signature status. A
    brief, bounded retry should recover from these instead of aborting
    the whole transcode request over one blip — see MAX_CONNECTION_RETRIES."""

    async def test_recovers_from_a_transient_failure_within_the_retry_budget(self) -> None:
        flac_bytes, _original = _make_test_flac()
        # Fails twice, succeeds on the third try — within MAX_CONNECTION_RETRIES.
        server = FlakyThenHealthyUpstream(flac_bytes, fail_count=2)
        url = await server.start()
        try:
            source = await asyncio.to_thread(LazyHttpFlacSource, url)
            assert source._total_size == len(flac_bytes)
        finally:
            await server.stop()

    async def test_gives_up_after_exhausting_the_retry_budget_with_a_clear_error(self) -> None:
        server = AlwaysFailingUpstream()
        url = await server.start()
        try:
            with pytest.raises(OSError) as exc_info:
                await asyncio.to_thread(LazyHttpFlacSource, url)
            # The whole point: a clear, specific message — not just
            # whatever terse text the underlying connection error carried
            # ("Connection lost" and similar tell an operator nothing on
            # their own).
            message = str(exc_info.value)
            assert "Qobuz CDN" in message
            assert "retries" in message
            # Retried the full budget, not given up after the first failure.
            assert server.request_count == 4  # 1 initial + MAX_CONNECTION_RETRIES
        finally:
            await server.stop()

"""Tests for AudioProxyServer upstream retry behavior."""

import socket
from unittest.mock import AsyncMock, MagicMock

import aiohttp
from aiohttp import web

from qobuz_proxy.backends.dlna.proxy_server import AudioProxyServer, RegisteredTrack
from qobuz_proxy.playback.stream_resolver import ResolvedStream

PAYLOAD = bytes(range(256)) * 1024  # 256 KiB deterministic payload
ABORT_AFTER = 100_000
FORMAT_ID = 6  # arbitrary — these tests don't exercise resolve_track's decision tree


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _stream(url: str) -> ResolvedStream:
    return ResolvedStream(
        url=url, blob="", format_id=FORMAT_ID, sample_rate=44100, bit_depth=16, fetched_at=0.0
    )


def _register(proxy: AudioProxyServer, track_id: str) -> None:
    """Register a track directly, bypassing resolve_track's capability-
    driven decision tree — these tests only care about the byte-serving/
    retry path, given a fixed format_id the mocked resolver already knows
    how to answer for."""
    proxy._tracks[track_id] = RegisteredTrack(
        track_id=track_id, format_id=FORMAT_ID, content_type="audio/flac"
    )


class FlakyUpstream:
    """Fake CDN that aborts the first full-body request mid-stream.

    Subsequent requests (e.g. Range resumes) are served completely, mimicking
    the Akamai behavior seen in issue #10 where a long-running stream dies
    with a short read but a fresh ranged request succeeds.
    """

    def __init__(self):
        self.range_headers: list = []  # Range header of each request (None = full body)
        self._aborted_once = False
        self._runner = None

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        rng = request.headers.get("Range")
        self.range_headers.append(rng)

        start = 0
        if rng:
            start = int(rng.removeprefix("bytes=").split("-")[0])
        body = PAYLOAD[start:]

        if rng is None and not self._aborted_once:
            self._aborted_once = True
            resp = web.StreamResponse(
                status=200,
                headers={"Content-Length": str(len(body)), "Accept-Ranges": "bytes"},
            )
            await resp.prepare(request)
            await resp.write(body[:ABORT_AFTER])
            # Kill the connection without delivering the advertised length
            assert request.transport is not None
            request.transport.close()
            return resp

        headers = {"Accept-Ranges": "bytes", "Content-Length": str(len(body))}
        status = 200
        if rng:
            status = 206
            headers["Content-Range"] = f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}"
        return web.Response(status=status, body=body, headers=headers)

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/file", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        port = _free_port()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        return f"http://127.0.0.1:{port}/file"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


class MethodRecordingUpstream:
    """Fake CDN that records the HTTP method of every request."""

    def __init__(self):
        self.methods: list = []
        self._runner = None

    async def _handle(self, request: web.Request) -> web.Response:
        self.methods.append(request.method)
        return web.Response(
            status=200,
            body=PAYLOAD,
            headers={"Accept-Ranges": "bytes", "Content-Length": str(len(PAYLOAD))},
        )

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/file", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        port = _free_port()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        return f"http://127.0.0.1:{port}/file"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


async def test_head_probe_does_not_download_the_track():
    """Regression for BUG-29: a renderer HEAD probe must be answered from
    upstream headers, not by streaming (and discarding) the whole file."""
    upstream = MethodRecordingUpstream()
    upstream_url = await upstream.start()

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(upstream_url))

    port = _free_port()
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(f"http://127.0.0.1:{port}/audio/42.flac") as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/flac"
                assert resp.headers["Accept-Ranges"] == "bytes"
                # Regression: web.Response computes Content-Length from its
                # body automatically and raises if you assign to
                # .content_length afterwards — that RuntimeError was
                # silently swallowed here, so HEAD probes never actually
                # reported a Content-Length at all.
                assert resp.headers["Content-Length"] == str(len(PAYLOAD))
    finally:
        await proxy.stop()
        await upstream.stop()

    # The CDN saw only a HEAD — never a body-transferring GET
    assert upstream.methods == ["HEAD"]


class ExpiredUrlUpstream:
    """Fake CDN that rejects stale signed URLs with 403 but serves fresh ones."""

    def __init__(self):
        self.requests: list = []  # token query param of each request
        self._runner = None

    async def _handle(self, request: web.Request) -> web.Response:
        token = request.query.get("token", "")
        self.requests.append(token)
        if token != "fresh":
            return web.Response(status=403, text="URL signature expired")
        return web.Response(
            status=200,
            body=PAYLOAD,
            headers={"Accept-Ranges": "bytes", "Content-Length": str(len(PAYLOAD))},
        )

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/file", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        port = _free_port()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        return f"http://127.0.0.1:{port}/file"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


async def test_refreshes_url_and_retries_on_upstream_403():
    """An expired signed URL (CDN 403) must trigger a forced refresh, not a 502."""
    upstream = ExpiredUrlUpstream()
    base_url = await upstream.start()

    async def _resolve(track_id, format_id, force=False):
        return _stream(f"{base_url}?token={'fresh' if force else 'stale'}")

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=_resolve)

    port = _free_port()
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"http://127.0.0.1:{port}/audio/42.flac") as resp:
                assert resp.status == 200
                body = await resp.read()
    finally:
        await proxy.stop()
        await upstream.stop()

    assert body == PAYLOAD
    assert upstream.requests == ["stale", "fresh"]
    resolver.resolve.assert_awaited_with("42", FORMAT_ID, force=True)


async def test_returns_502_when_refresh_fails_after_403():
    """If no fresh URL can be fetched, the 403 surfaces as a single 502 (no retry loop)."""
    upstream = ExpiredUrlUpstream()
    base_url = await upstream.start()

    async def _resolve(track_id, format_id, force=False):
        if force:
            return None  # refresh failed
        return _stream(f"{base_url}?token=stale")

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=_resolve)

    port = _free_port()
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"http://127.0.0.1:{port}/audio/42.flac") as resp:
                assert resp.status == 502
    finally:
        await proxy.stop()
        await upstream.stop()

    assert upstream.requests == ["stale"]


async def test_resumes_upstream_after_midstream_failure():
    """A mid-stream upstream failure must not kill the renderer's stream."""
    upstream = FlakyUpstream()
    upstream_url = await upstream.start()

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(upstream_url))

    port = _free_port()
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=port)
    await proxy.start()
    body = b""
    try:
        _register(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"http://127.0.0.1:{port}/audio/42.flac") as resp:
                assert resp.status == 200
                try:
                    body = await resp.read()
                except (aiohttp.ClientPayloadError, aiohttp.ServerTimeoutError, TimeoutError):
                    pass  # truncated/stalled stream — the assertion below reports it
    finally:
        await proxy.stop()
        await upstream.stop()

    assert body == PAYLOAD
    # The proxy must have resumed with a Range request, not restarted from zero
    assert len(upstream.range_headers) == 2
    resume = upstream.range_headers[1]
    assert resume is not None and resume.startswith("bytes=")
    assert int(resume.removeprefix("bytes=").split("-")[0]) > 0


def _mock_request() -> MagicMock:
    """A stand-in for aiohttp's web.Request — _claim_stream only ever
    touches .transport on it."""
    request = MagicMock()
    request.transport = MagicMock()
    request.transport.is_closing.return_value = False
    return request


class TestStreamSupersession:
    """A renderer that fires a new request for a track it's already
    streaming reproducibly does so on *every* seek (confirmed directly,
    including the very first request of a session — not an occasional
    timing fluke): a GET-before-Range probe immediately followed by the
    real Range request, and/or the previous seek's connection still
    draining. Measured directly, the renderer itself closes the old
    connection first almost every time anyway — which means for however
    long it keeps both sockets open, it's genuinely receiving two
    different, both-valid positions of the same track at once. So
    _claim_stream force-closes the previous request's transport
    immediately, rather than only marking it stale for the old loop to
    notice on its own schedule (see proxy_server.py's docstring for the
    full rationale, including why this doesn't reintroduce the
    "generator already executing" crash Task.cancel() caused: it never
    touches the generator/thread doing the decode work, only the socket)."""

    def test_claiming_a_key_supersedes_the_previous_claim(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        first = proxy._claim_stream("42", _mock_request())
        assert not proxy._stream_superseded("42", first)

        second = proxy._claim_stream("42", _mock_request())

        assert proxy._stream_superseded("42", first)
        assert not proxy._stream_superseded("42", second)

    def test_unrelated_keys_do_not_interfere(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        a = proxy._claim_stream("A", _mock_request())
        proxy._claim_stream("B", _mock_request())

        assert not proxy._stream_superseded("A", a)  # B claiming doesn't touch A

    def test_claiming_over_an_existing_stream_closes_its_transport(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        first_request = _mock_request()
        proxy._claim_stream("42", first_request)
        first_request.transport.close.assert_not_called()

        proxy._claim_stream("42", _mock_request())

        first_request.transport.close.assert_called_once()

    def test_first_claim_for_a_key_closes_nothing(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        request = _mock_request()
        proxy._claim_stream("42", request)

        request.transport.close.assert_not_called()

    def test_claiming_with_the_same_request_object_does_not_close_itself(self) -> None:
        """Defensive: a request can't supersede itself."""
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        request = _mock_request()
        proxy._claim_stream("42", request)
        proxy._claim_stream("42", request)

        request.transport.close.assert_not_called()

    def test_does_not_close_a_transport_already_closing(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        first_request = _mock_request()
        first_request.transport.is_closing.return_value = True
        proxy._claim_stream("42", first_request)

        proxy._claim_stream("42", _mock_request())

        first_request.transport.close.assert_not_called()

    def test_unrelated_keys_do_not_close_each_others_transport(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        request_a = _mock_request()
        proxy._claim_stream("A", request_a)

        proxy._claim_stream("B", _mock_request())

        request_a.transport.close.assert_not_called()

    def test_unclaimed_key_reports_superseded(self) -> None:
        resolver = AsyncMock()
        proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)

        assert proxy._stream_superseded("never-claimed", 1)

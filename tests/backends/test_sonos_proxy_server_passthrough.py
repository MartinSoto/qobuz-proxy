"""End-to-end tests for SonosAudioProxyServer's plain CDN-passthrough
path (PassthroughStreamHandler, backed by CDNBlockCache): a real
SonosAudioProxyServer, a real fake-CDN upstream, and a real HTTP client —
proving a GET/HEAD/Range request against a format=flac/mp3 proxy URL
serves back the exact source bytes, unmodified, read through the shared
block cache rather than a raw per-request upstream connection. See
test_sonos_proxy_server_transcode.py for the format=wav counterpart.
"""

import socket

import aiohttp
from aiohttp import web
from unittest.mock import AsyncMock

from qobuz_proxy.backends.dlna.sonos.proxy_server import SonosAudioProxyServer
from qobuz_proxy.playback.stream_resolver import ResolvedStream

FORMAT_ID = 6  # CD tier — arbitrary; these tests build URLs directly, bypassing resolve_track


def _stream(url: str, blob: str = "") -> ResolvedStream:
    return ResolvedStream(
        url=url, blob=blob, format_id=FORMAT_ID, sample_rate=44100, bit_depth=16, fetched_at=0.0
    )


def _resolver(url: str) -> AsyncMock:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(url))
    return resolver


def _passthrough_url(
    proxy: SonosAudioProxyServer,
    track_id: str,
    *,
    fmt: str = "flac",
    depth: int = 16,
    rate: int = 44100,
    item: str | None = None,
) -> str:
    return (
        f"{proxy.base_url}/audio/{track_id}?format={fmt}&depth={depth}"
        f"&rate={rate}&item={item or track_id}"
    )


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class FakeCdnUpstream:
    """A stand-in Qobuz CDN: serves one fixed payload with Range support."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._runner: web.AppRunner | None = None
        self.requests: list[tuple[str, str | None]] = []  # (method, Range)

    async def _handle(self, request: web.Request) -> web.Response:
        rng = request.headers.get("Range")
        self.requests.append((request.method, rng))

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
        app.router.add_get("/track.flac", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        port = _free_port()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        return f"http://127.0.0.1:{port}/track.flac"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


async def test_get_serves_the_exact_source_bytes():
    payload = bytes(range(256)) * 100  # 25.6KB, arbitrary
    upstream = FakeCdnUpstream(payload)
    upstream_url = await upstream.start()

    port = _free_port()
    proxy = SonosAudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = _passthrough_url(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/flac"
                assert resp.headers["Accept-Ranges"] == "bytes"
                assert int(resp.headers["Content-Length"]) == len(payload)
                body = await resp.read()
        assert body == payload
    finally:
        await proxy.stop()
        await upstream.stop()


async def test_mp3_format_id_maps_to_mpeg_content_type():
    payload = b"\x00" * 1000
    upstream = FakeCdnUpstream(payload)
    upstream_url = await upstream.start()

    port = _free_port()
    proxy = SonosAudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = _passthrough_url(proxy, "42", fmt="mp3", depth=16, rate=44100)
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/mpeg"
                body = await resp.read()
        assert body == payload
    finally:
        await proxy.stop()
        await upstream.stop()


async def test_head_probe_reports_content_length_without_downloading_body():
    payload = bytes(range(256)) * 100
    upstream = FakeCdnUpstream(payload)
    upstream_url = await upstream.start()

    port = _free_port()
    proxy = SonosAudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = _passthrough_url(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(proxy_url) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/flac"
                assert int(resp.headers["Content-Length"]) == len(payload)
    finally:
        await proxy.stop()
        await upstream.stop()

    # HEAD only ever needs the upstream's own Content-Length — never a body GET.
    assert all(method == "HEAD" for method, _rng in upstream.requests)


async def test_range_request_serves_the_exact_requested_slice():
    payload = bytes(range(256)) * 100
    upstream = FakeCdnUpstream(payload)
    upstream_url = await upstream.start()

    port = _free_port()
    proxy = SonosAudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = _passthrough_url(proxy, "42")
        start_byte = 10_000
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url, headers={"Range": f"bytes={start_byte}-"}) as resp:
                assert resp.status == 206
                assert (
                    resp.headers["Content-Range"]
                    == f"bytes {start_byte}-{len(payload) - 1}/{len(payload)}"
                )
                assert int(resp.headers["Content-Length"]) == len(payload) - start_byte
                body = await resp.read()
        assert body == payload[start_byte:]
    finally:
        await proxy.stop()
        await upstream.stop()


async def test_two_registrations_of_the_same_track_share_cached_blocks():
    """Both the "current" and a gapless "next" registration of the same
    track (different `item`, same track_id) read through the one shared
    CDNBlockCache — a second full-file GET for the same track must not
    refetch blocks the first already cached."""
    payload = bytes(range(256)) * 100
    upstream = FakeCdnUpstream(payload)
    upstream_url = await upstream.start()

    port = _free_port()
    proxy = SonosAudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        first_url = _passthrough_url(proxy, "42", item="42_0")
        second_url = _passthrough_url(proxy, "42", item="42_7")

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(first_url) as resp:
                first_body = await resp.read()
            requests_after_first = len(upstream.requests)
            async with session.get(second_url) as resp:
                second_body = await resp.read()

        assert first_body == payload
        assert second_body == payload
        # No new upstream requests — everything came out of the cache.
        assert len(upstream.requests) == requests_after_first
    finally:
        await proxy.stop()
        await upstream.stop()

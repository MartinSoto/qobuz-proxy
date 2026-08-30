"""End-to-end tests for AudioProxyServer's downsampling path: a real
AudioProxyServer, a real fake-CDN upstream serving a real FLAC file, and a
real HTTP client — proving a Sonos-style GET/HEAD/Range request against a
track registered with transcode_to_sample_rate actually gets back correct,
byte-exact-seekable WAV audio. See test_transcoding_reader.py for the
underlying engine's own (lower-level) tests.
"""

import asyncio
import io
import socket

import aiohttp
import numpy as np
import soundfile as sf
import soxr
from aiohttp import web
from unittest.mock import AsyncMock

from qobuz_proxy.backends.dlna.proxy_server import AudioProxyServer, RegisteredTrack
from qobuz_proxy.backends.dlna.transcoding_reader import WAV_HEADER_SIZE
from qobuz_proxy.playback.stream_resolver import ResolvedStream

FORMAT_ID = 27  # arbitrary — these tests register tracks directly, bypassing resolve_track


def _stream(url: str, blob: str = "") -> ResolvedStream:
    return ResolvedStream(
        url=url,
        blob=blob,
        format_id=FORMAT_ID,
        sample_rate=SOURCE_SAMPLE_RATE,
        bit_depth=24,
        fetched_at=0.0,
    )


def _resolver(url: str, blob: str = "") -> AsyncMock:
    """A resolver mock that always answers with the same (track_id, format_id)
    resolution, regardless of force — matches these tests' single-URL upstreams."""
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(url, blob))
    return resolver


def _register_transcoded(
    proxy: AudioProxyServer, track_id: str, proxy_key: str | None = None
) -> None:
    """Register a track directly for transcoding, bypassing resolve_track's
    capability-driven decision tree — these tests only care about the
    transcode-serving path, given a resolver that already knows how to
    answer for FORMAT_ID."""
    key = proxy_key or track_id
    proxy._tracks[key] = RegisteredTrack(
        track_id=track_id,
        format_id=FORMAT_ID,
        content_type="audio/wav",
        transcode_to_sample_rate=TARGET_SAMPLE_RATE,
    )


SOURCE_SAMPLE_RATE = 96000
TARGET_SAMPLE_RATE = 48000
# Long enough that the resulting FLAC comfortably exceeds
# LazyHttpFlacSource's default 256KB read-ahead chunk — otherwise even a
# single-chunk fetch would cover the "whole" file and the partial-fetch
# assertions below wouldn't actually prove anything.
DURATION_SECONDS = 20.0
CHANNELS = 2


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_test_flac() -> tuple[bytes, np.ndarray]:
    n = int(DURATION_SECONDS * SOURCE_SAMPLE_RATE)
    t = np.arange(n) / SOURCE_SAMPLE_RATE
    left = 0.5 * np.sin(2 * np.pi * 440.0 * t)
    right = 0.5 * np.sin(2 * np.pi * 880.0 * t)
    audio = np.stack([left, right], axis=1).astype("float32")
    buf = io.BytesIO()
    sf.write(buf, audio, SOURCE_SAMPLE_RATE, format="FLAC", subtype="PCM_24")
    return buf.getvalue(), audio


def _pcm24_bytes_to_float(data: bytes, channels: int) -> np.ndarray:
    raw = np.frombuffer(data, dtype="u1").reshape(-1, 3)
    sign_bit_set = (raw[:, 2] & 0x80) != 0
    padded = np.zeros((raw.shape[0], 4), dtype="u1")
    padded[:, :3] = raw
    padded[sign_bit_set, 3] = 0xFF
    as_int32 = padded.view("<i4").reshape(-1)
    floats = as_int32.astype("float64") / 8_388_607.0
    return floats.reshape(-1, channels).astype("float32")


class FakeCdnUpstream:
    """A stand-in Qobuz CDN: serves one fixed FLAC payload with Range
    support, and records every request it receives."""

    def __init__(self, payload: bytes, delay: float = 0.0):
        self._payload = payload
        self._delay = delay  # artificial per-request latency, for concurrency tests
        self._runner: web.AppRunner | None = None
        self.requests: list[tuple[str, str | None]] = []  # (method, Range)

    async def _handle(self, request: web.Request) -> web.Response:
        if self._delay:
            await asyncio.sleep(self._delay)
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


async def test_get_serves_correct_downsampled_wav_audio():
    flac_bytes, original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()
    expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    port = _free_port()
    proxy = AudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        proxy_url = f"{proxy.base_url}/audio/42.wav"
        assert proxy_url.endswith(".wav")

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/wav"
                assert resp.headers["Accept-Ranges"] == "bytes"
                body = await resp.read()

        assert body[:4] == b"RIFF"
        decoded = _pcm24_bytes_to_float(body[WAV_HEADER_SIZE:], CHANNELS)
        assert len(decoded) == len(expected)
        np.testing.assert_allclose(decoded, expected, atol=2e-4)
    finally:
        await proxy.stop()
        await upstream.stop()


async def test_concurrent_requests_for_the_same_track_both_complete_without_the_server_raising(
    caplog,
):
    """Two requests for the same registered track (e.g. a renderer's
    GET-before-Range probe immediately followed by the real Range request)
    run fully independently now — there's no cooperative supersession
    cutting either one short — so both must decode/stream to completion
    without the server raising, each getting correct audio back.
    """
    import logging

    flac_bytes, original = _make_test_flac()
    # A small delay before each upstream response widens the window in
    # which the first request is genuinely still inside a to_thread()
    # decode/fetch call when the second one starts.
    upstream = FakeCdnUpstream(flac_bytes, delay=0.02)
    upstream_url = await upstream.start()
    expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    port = _free_port()
    proxy = AudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        proxy_url = f"{proxy.base_url}/audio/42.wav"

        with caplog.at_level(logging.ERROR):
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                first = asyncio.ensure_future(session.get(proxy_url))
                await asyncio.sleep(0.01)  # first request is now mid to_thread() decode

                async with session.get(proxy_url) as second_resp:
                    assert second_resp.status == 200
                    second_body = await second_resp.read()

                # Both requests must complete cleanly; the point is
                # awaiting the first must not hang, and (checked below via
                # caplog) the server must never have raised handling either.
                first_resp = await asyncio.wait_for(first, timeout=10)
                await first_resp.read()
                first_resp.close()
    finally:
        await proxy.stop()
        await upstream.stop()

    assert "generator already executing" not in caplog.text
    assert "Error handling request" not in caplog.text

    decoded = _pcm24_bytes_to_float(second_body[WAV_HEADER_SIZE:], CHANNELS)
    assert len(decoded) == len(expected)
    np.testing.assert_allclose(decoded, expected, atol=2e-4)


async def test_head_probe_reports_correct_content_length_without_full_download():
    flac_bytes, original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()
    expected_target_frames = round(len(original) * TARGET_SAMPLE_RATE / SOURCE_SAMPLE_RATE)
    expected_content_length = WAV_HEADER_SIZE + expected_target_frames * CHANNELS * 3

    port = _free_port()
    proxy = AudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        proxy_url = f"{proxy.base_url}/audio/42.wav"
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(proxy_url) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/wav"
                assert int(resp.headers["Content-Length"]) == expected_content_length
    finally:
        await proxy.stop()
        await upstream.stop()

    # A HEAD probe only ever needs the source's STREAMINFO — never the
    # full 96kHz source file.
    assert all(method != "GET" or rng is not None for method, rng in upstream.requests)


async def test_range_request_seeks_to_the_correct_audio():
    flac_bytes, original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()
    expected_full = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    port = _free_port()
    proxy = AudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        proxy_url = f"{proxy.base_url}/audio/42.wav"
        total_target_frames = len(expected_full)
        target_frame = int(total_target_frames * 0.5)
        bytes_per_frame = CHANNELS * 3
        start_byte = WAV_HEADER_SIZE + target_frame * bytes_per_frame

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url, headers={"Range": f"bytes={start_byte}-"}) as resp:
                assert resp.status == 206
                content_range = resp.headers["Content-Range"]
                assert content_range.startswith(f"bytes {start_byte}-")
                assert content_range.endswith(
                    f"/{WAV_HEADER_SIZE + total_target_frames * bytes_per_frame}"
                )
                # Read only a bounded amount and stop — resp.read() would
                # consume the *entire* remaining stream, decoding/
                # resampling all the way to EOF server-side and defeating
                # the "no full download" point of this test.
                body = await resp.content.readexactly(4000 * bytes_per_frame)

        # The whole point: this must not have required downloading the full
        # (~source-rate) file to serve a seek near the middle. Measured as
        # what the cache actually chose to fetch and retain (cached_bytes),
        # not bytes physically over the wire — CDNBlockCache deliberately
        # sends an open-ended Range so it can keep a connection open for
        # reuse (see its module docstring), and on a fast loopback
        # connection the remainder of a file this small can land in the
        # kernel socket buffer well before the client ever abandons that
        # connection, making a wire-level byte count an unreliable signal.
        assert proxy._transcode._cache.cached_bytes < len(flac_bytes) * 0.5
    finally:
        await proxy.stop()
        await upstream.stop()

    decoded = _pcm24_bytes_to_float(body, CHANNELS)
    expected_slice = expected_full[target_frame : target_frame + len(decoded)]
    settle = 200  # fresh-resample-run edge transient — see transcoding_reader.py
    np.testing.assert_allclose(decoded[settle:], expected_slice[settle:], atol=5e-4)


async def test_misaligned_range_request_serves_exact_bytes_like_a_static_file():
    """We're simulating a plain static file on disk, the same thing a
    dumb NAS would serve — the renderer parses our WAV header once (it
    always fetches from byte 0 first) and finds its own alignment from
    there; the server's only job is to hand back the literal bytes that
    exist at the requested offset, exactly as asked, never a rounded or
    "corrected" position.

    Confirmed directly against a real device: it computes seek byte
    offsets on a fixed grid unrelated to our 24-bit frame size (so most
    requests don't land on a true sample boundary), and it genuinely
    expects Content-Range to echo back exactly the byte it requested — a
    prior version of this fix that "helpfully" declared a different,
    frame-aligned start instead made every one of those requests play
    back as white noise, 100% consistently, while requests that already
    happened to land on a frame boundary played clean. So: never declare
    anything but the exact requested byte, and the response body must be
    byte-for-byte identical to the same slice of the full file — proven
    here by comparing a misaligned Range response directly against the
    corresponding slice of an aligned one, the same invariant a real
    static file server satisfies for free."""
    flac_bytes, original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()
    expected_full = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    port = _free_port()
    proxy = AudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        proxy_url = f"{proxy.base_url}/audio/42.wav"
        bytes_per_frame = CHANNELS * 3  # 6 — so +1..+5 are all genuinely misaligned
        target_frame = int(len(expected_full) * 0.5)
        aligned_start_byte = WAV_HEADER_SIZE + target_frame * bytes_per_frame
        requested_start_byte = aligned_start_byte + 2  # lands mid-sample, not on it
        read_len = 4000 * bytes_per_frame

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Ground truth: a Range request that already lands exactly on
            # a frame boundary, so nothing needs trimming.
            async with session.get(
                proxy_url, headers={"Range": f"bytes={aligned_start_byte}-"}
            ) as aligned_resp:
                assert aligned_resp.status == 206
                assert aligned_resp.headers["Content-Range"].startswith(
                    f"bytes {aligned_start_byte}-"
                )
                ground_truth = await aligned_resp.content.readexactly(read_len + 2)

            async with session.get(
                proxy_url, headers={"Range": f"bytes={requested_start_byte}-"}
            ) as resp:
                assert resp.status == 206
                content_range = resp.headers["Content-Range"]
                # Must echo back exactly the byte the renderer asked for —
                # never a rounded/corrected one. That's the entire point.
                assert content_range.startswith(f"bytes {requested_start_byte}-")
                assert requested_start_byte != aligned_start_byte  # test is actually exercising it

                declared_length = int(resp.headers["Content-Length"])
                body = await resp.content.readexactly(read_len)
    finally:
        await proxy.stop()
        await upstream.stop()

    # The literal invariant a static file server satisfies: reading from
    # byte N+2 gives exactly the same bytes as reading from byte N and
    # skipping 2 — regardless of where N falls relative to any internal
    # sample-frame boundary.
    assert body == ground_truth[2 : 2 + read_len]

    # And that ground-truth read is itself genuinely correct audio, not
    # just internally self-consistent nonsense.
    decoded = _pcm24_bytes_to_float(
        ground_truth[: read_len - (read_len % bytes_per_frame)], CHANNELS
    )
    expected_slice = expected_full[target_frame : target_frame + len(decoded)]
    settle = 200  # fresh-resample-run edge transient — see transcoding_reader.py
    np.testing.assert_allclose(decoded[settle:], expected_slice[settle:], atol=5e-4)

    total_content_length = WAV_HEADER_SIZE + len(expected_full) * bytes_per_frame
    assert declared_length == total_content_length - requested_start_byte


async def test_content_type_wav_route_registered_without_extension_too():
    """The proxy URL always carries an explicit extension, but the bare
    (no-extension) route must still resolve a transcoded track correctly
    if ever hit directly."""
    flac_bytes, _original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()

    port = _free_port()
    proxy = AudioProxyServer(resolver=_resolver(upstream_url), host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(f"http://127.0.0.1:{port}/audio/42") as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == "audio/wav"
    finally:
        await proxy.stop()
        await upstream.stop()


class ExpiringFlacUpstream:
    """A Qobuz-style signed URL that has gone stale: rejects the original
    token with 403 but serves the same FLAC normally for a fresh one — the
    scenario a long track can hit mid-stream (see
    proxy_server.py's _make_sync_url_refresher)."""

    def __init__(self, payload: bytes):
        self._payload = payload
        self._runner: web.AppRunner | None = None
        self.rejected = 0

    async def _handle(self, request: web.Request) -> web.Response:
        if request.query.get("token") != "fresh":
            self.rejected += 1
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


async def test_expired_url_is_refreshed_and_transcoding_still_succeeds():
    """A long track can outlive the signed URL's TTL mid-playback — the
    proxy must refresh and keep serving correct downsampled audio, the
    same protection _proxy_stream already has for the pass-through path."""
    flac_bytes, original = _make_test_flac()
    upstream = ExpiringFlacUpstream(flac_bytes)
    base_url = await upstream.start()
    stale_url = f"{base_url}?token=stale"
    fresh_url = f"{base_url}?token=fresh"
    expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    async def _resolve(track_id, format_id, force=False):
        return _stream(fresh_url if force else stale_url)

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=_resolve)

    port = _free_port()
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        _register_transcoded(proxy, "42")
        proxy_url = f"{proxy.base_url}/audio/42.wav"
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url) as resp:
                assert resp.status == 200
                body = await resp.read()
    finally:
        await proxy.stop()
        await upstream.stop()

    assert upstream.rejected >= 1  # the stale token really was tried first
    decoded = _pcm24_bytes_to_float(body[WAV_HEADER_SIZE:], CHANNELS)
    assert len(decoded) == len(expected)
    np.testing.assert_allclose(decoded, expected, atol=2e-4)
    resolver.resolve.assert_awaited_with("42", FORMAT_ID, force=True)


async def test_gapless_preloaded_track_gets_the_same_url_refresh_protection():
    """A gapless-armed track (DLNABackend.set_next_track) is registered
    under a composite proxy_key="{track_id}_{queue_item_id}" like any
    other track, and served through the same _handle_audio ->
    _transcode_stream dispatch — there's no separate code path for it, so
    it gets no less (and no more) URL-refresh protection. Worth proving
    directly rather than just asserting it: a gapless-armed track is often
    the *likelier* one to actually hit an expired URL, since it can sit
    registered for however long the current track has left to play,
    unlike a track just started with play() (registered right as it's
    first requested)."""
    flac_bytes, original = _make_test_flac()
    upstream = ExpiringFlacUpstream(flac_bytes)
    base_url = await upstream.start()
    stale_url = f"{base_url}?token=stale"
    fresh_url = f"{base_url}?token=fresh"
    expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    async def _resolve(track_id, format_id, force=False):
        return _stream(fresh_url if force else stale_url)

    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=_resolve)

    port = _free_port()
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        # Mirrors DLNABackend.set_next_track's registration exactly:
        # proxy_key = f"{track_id}_{queue_item_id}".
        _register_transcoded(proxy, "42", proxy_key="42_7")
        proxy_url = f"{proxy.base_url}/audio/42_7.wav"

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url) as resp:
                assert resp.status == 200
                body = await resp.read()
    finally:
        await proxy.stop()
        await upstream.stop()

    assert upstream.rejected >= 1
    decoded = _pcm24_bytes_to_float(body[WAV_HEADER_SIZE:], CHANNELS)
    assert len(decoded) == len(expected)
    np.testing.assert_allclose(decoded, expected, atol=2e-4)
    # Refreshed by the *track_id*, not the composite proxy key — matches
    # what the resolver actually indexes tracks by.
    resolver.resolve.assert_awaited_with("42", FORMAT_ID, force=True)

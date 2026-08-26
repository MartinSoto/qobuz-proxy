"""End-to-end tests for AudioProxyServer's downsampling path: a real
AudioProxyServer, a real fake-CDN upstream serving a real FLAC file, and a
real HTTP client — proving a Sonos-style GET/HEAD/Range request against a
track registered with transcode_to_sample_rate actually gets back correct,
byte-exact-seekable WAV audio. See test_transcoding_reader.py for the
underlying engine's own (lower-level) tests.
"""

import io
import socket

import aiohttp
import numpy as np
import soundfile as sf
import soxr
from aiohttp import web
from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends.dlna.proxy_server import AudioProxyServer
from qobuz_proxy.backends.dlna.transcoding_reader import WAV_HEADER_SIZE

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

    def __init__(self, payload: bytes):
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


async def test_get_serves_correct_downsampled_wav_audio():
    flac_bytes, original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()
    expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    provider = MagicMock()
    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = proxy.register_track(
            "42", upstream_url, "audio/flac", transcode_to_sample_rate=TARGET_SAMPLE_RATE
        )
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


async def test_head_probe_reports_correct_content_length_without_full_download():
    flac_bytes, original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()
    expected_target_frames = round(len(original) * TARGET_SAMPLE_RATE / SOURCE_SAMPLE_RATE)
    expected_content_length = WAV_HEADER_SIZE + expected_target_frames * CHANNELS * 3

    provider = MagicMock()
    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = proxy.register_track(
            "42", upstream_url, "audio/flac", transcode_to_sample_rate=TARGET_SAMPLE_RATE
        )
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

    provider = MagicMock()
    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = proxy.register_track(
            "42", upstream_url, "audio/flac", transcode_to_sample_rate=TARGET_SAMPLE_RATE
        )
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
    finally:
        await proxy.stop()
        await upstream.stop()

    decoded = _pcm24_bytes_to_float(body, CHANNELS)
    expected_slice = expected_full[target_frame : target_frame + len(decoded)]
    settle = 200  # fresh-resample-run edge transient — see transcoding_reader.py
    np.testing.assert_allclose(decoded[settle:], expected_slice[settle:], atol=5e-4)

    # The whole point: this must not have required downloading the full
    # (~source-rate) file to serve a seek near the middle.
    total_fetched = sum(
        len(flac_bytes) if rng is None else _range_len(rng, len(flac_bytes))
        for method, rng in upstream.requests
        if method == "GET"
    )
    assert total_fetched < len(flac_bytes) * 0.5


def _range_len(range_header: str, total: int) -> int:
    start_s, end_s = range_header.removeprefix("bytes=").split("-")
    start = int(start_s)
    end = int(end_s) if end_s else total - 1
    return min(end, total - 1) - start + 1


async def test_content_type_wav_route_registered_without_extension_too():
    """register_track's returned URL always carries an explicit extension,
    but the bare (no-extension) route must still resolve a transcoded
    track correctly if ever hit directly."""
    flac_bytes, _original = _make_test_flac()
    upstream = FakeCdnUpstream(flac_bytes)
    upstream_url = await upstream.start()

    provider = MagicMock()
    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy.register_track(
            "42", upstream_url, "audio/flac", transcode_to_sample_rate=TARGET_SAMPLE_RATE
        )
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

    provider = MagicMock()
    provider.get_streaming_url = AsyncMock(return_value=fresh_url)

    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = proxy.register_track(
            "42", stale_url, "audio/flac", transcode_to_sample_rate=TARGET_SAMPLE_RATE
        )
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
    provider.get_streaming_url.assert_awaited_with("42", force=True)


async def test_gapless_preloaded_track_gets_the_same_url_refresh_protection():
    """A gapless-armed track (DLNABackend.set_next_track) is registered via
    the exact same register_track()/proxy_key="{track_id}_{queue_item_id}"
    path as any other track, and served through the same _handle_audio ->
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

    provider = MagicMock()
    provider.get_streaming_url = AsyncMock(return_value=fresh_url)

    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        # Mirrors DLNABackend.set_next_track's registration exactly:
        # proxy_key = f"{track_id}_{queue_item_id}".
        proxy_url = proxy.register_track(
            "42",
            stale_url,
            "audio/flac",
            proxy_key="42_7",
            transcode_to_sample_rate=TARGET_SAMPLE_RATE,
        )
        assert "42_7" in proxy_url

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
    # what the url_provider actually indexes tracks by.
    provider.get_streaming_url.assert_awaited_with("42", force=True)


async def test_a_track_armed_long_before_being_requested_is_proactively_refreshed():
    """The likelier real-world way a gapless-armed track hits an expired
    URL: it can sit registered for however long the *current* track still
    has left to play, not just outlive a TTL mid-stream. _handle_audio's
    freshness check (is_url_expired) runs before dispatch, for every
    track regardless of transcode vs pass-through or how it was
    registered — so this is pre-existing protection, not something new
    this session, but worth confirming it actually reaches the transcode
    path too."""
    flac_bytes, original = _make_test_flac()
    upstream = ExpiringFlacUpstream(flac_bytes)
    base_url = await upstream.start()
    fresh_url = f"{base_url}?token=fresh"
    expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")

    provider = MagicMock()
    provider.get_streaming_url = AsyncMock(return_value=fresh_url)

    port = _free_port()
    proxy = AudioProxyServer(url_provider=provider, host="127.0.0.1", port=port)
    await proxy.start()
    try:
        proxy_url = proxy.register_track(
            "42",
            f"{base_url}?token=stale",
            "audio/flac",
            proxy_key="42_7",
            transcode_to_sample_rate=TARGET_SAMPLE_RATE,
        )
        # Simulate the gap: armed a while ago, current track still has
        # most of its runtime left — well past the proxy's staleness
        # threshold by the time it's actually requested.
        proxy._tracks["42_7"].url_fetched_at -= 600

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(proxy_url) as resp:
                assert resp.status == 200
                body = await resp.read()
    finally:
        await proxy.stop()
        await upstream.stop()

    # Refreshed *before* ever touching the stale URL — the stale token
    # must never have reached the upstream at all.
    assert upstream.rejected == 0
    decoded = _pcm24_bytes_to_float(body[WAV_HEADER_SIZE:], CHANNELS)
    assert len(decoded) == len(expected)
    np.testing.assert_allclose(decoded, expected, atol=2e-4)

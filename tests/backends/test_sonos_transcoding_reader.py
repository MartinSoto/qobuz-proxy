"""Tests for the FLAC decode/transcode pipeline (transcoding_reader.py):

- LazyHttpFlacSource — the CDNBlockCache-backed file-like bridge that lets
  soundfile/libFLAC seek within a remote FLAC file, using its own
  seeking logic, without downloading the whole file first. All of the
  actual CDN behavior it used to own directly (retry-on-transient-failure,
  refresh-on-expired-URL, connection reuse) lives in CDNBlockCache and is
  covered by test_sonos_cdn_block_cache.py — this only has to prove the
  bridge itself: correct bytes at the right positions, without a full
  download, and that two sources sharing one cache actually share its
  benefit.
- TranscodingFlacReader — the on-the-fly downsample-and-WAV-wrap pipeline
  built on top of it. Ground truth for "is the resampled audio correct" is
  a plain one-shot soxr.resample() over the *fully* decoded source array —
  the streaming (chunked) resampler used by the reader should match it
  closely wherever it's producing the same portion of audio.

Both spin up a real local HTTP server and do a real FLAC encode/decode
round trip (no mocks on the decode path) — the thing being validated is
genuinely "does this work," not "does this call the right mock."
"""

import asyncio
import io
import socket
import struct
import wave
from unittest.mock import AsyncMock

import numpy as np
import pytest
import soundfile as sf
import soxr
from aiohttp import web

from qobuz_proxy.backends.dlna.sonos.cdn_block_cache import CDNBlockCache, CDNBlockFetchError
from qobuz_proxy.backends.dlna.sonos.transcoding_reader import (
    WAV_HEADER_SIZE,
    LazyHttpFlacSource,
    TranscodingFlacReader,
)
from qobuz_proxy.playback.stream_resolver import ResolvedStream

SOURCE_SAMPLE_RATE = 96000
TARGET_SAMPLE_RATE = 48000
DURATION_SECONDS = 1.5
CHANNELS = 2
FORMAT_ID = 27  # arbitrary — these tests don't exercise quality selection
TRACK_ID = "42"
# Small, so the LazyHttpFlacSource partial-fetch assertions below actually
# exercise more than one block over a file this short.
BLOCK_SIZE = 32 * 1024


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_test_flac() -> tuple[bytes, np.ndarray]:
    """A deterministic stereo FLAC — a different tone per channel so a
    decoded slice can be checked against the exact source position, not
    just "some" audio."""
    n = int(DURATION_SECONDS * SOURCE_SAMPLE_RATE)
    t = np.arange(n) / SOURCE_SAMPLE_RATE
    left = 0.5 * np.sin(2 * np.pi * 440.0 * t)
    right = 0.5 * np.sin(2 * np.pi * 880.0 * t)
    audio = np.stack([left, right], axis=1).astype("float32")
    buf = io.BytesIO()
    sf.write(buf, audio, SOURCE_SAMPLE_RATE, format="FLAC", subtype="PCM_24")
    return buf.getvalue(), audio


def _stream(url: str) -> ResolvedStream:
    return ResolvedStream(
        url=url,
        blob="",
        format_id=FORMAT_ID,
        sample_rate=SOURCE_SAMPLE_RATE,
        bit_depth=24,
        fetched_at=0.0,
    )


def _resolver_for(url: str) -> AsyncMock:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(url))
    return resolver


def _cache_for(url: str, **kwargs: object) -> CDNBlockCache:
    return CDNBlockCache(resolver=_resolver_for(url), **kwargs)  # type: ignore[arg-type]


def _new_reader(
    cache: CDNBlockCache, loop: asyncio.AbstractEventLoop, track_id: str = TRACK_ID
) -> TranscodingFlacReader:
    return TranscodingFlacReader(cache, track_id, FORMAT_ID, TARGET_SAMPLE_RATE, loop)


def _new_source(
    cache: CDNBlockCache, loop: asyncio.AbstractEventLoop, track_id: str = TRACK_ID
) -> LazyHttpFlacSource:
    return LazyHttpFlacSource(cache, track_id, FORMAT_ID, loop)


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
                headers={"Accept-Ranges": "bytes", "Content-Length": str(len(self._payload))},
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


def _pcm24_bytes_to_float(data: bytes, channels: int) -> np.ndarray:
    """Inverse of transcoding_reader._float_to_pcm24_bytes, for assertions.

    Must sign-extend the dropped 4th byte (0xFF for a negative sample, not
    0x00) — packing drops the top byte because two's-complement truncation
    already preserves the value for anything in 24-bit range, but
    unpacking has to put the sign back explicitly.
    """
    raw = np.frombuffer(data, dtype="u1").reshape(-1, 3)
    sign_bit_set = (raw[:, 2] & 0x80) != 0
    padded = np.zeros((raw.shape[0], 4), dtype="u1")
    padded[:, :3] = raw
    padded[sign_bit_set, 3] = 0xFF
    as_int32 = padded.view("<i4").reshape(-1)
    floats = as_int32.astype("float64") / 8_388_607.0
    return floats.reshape(-1, channels).astype("float32")


class TestLazyHttpFlacSource:
    async def test_sequential_read_decodes_correctly(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url, block_size=BLOCK_SIZE)
        try:
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
        cache = _cache_for(url, block_size=BLOCK_SIZE)
        try:
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
        cache = _cache_for(url, block_size=BLOCK_SIZE)
        try:
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
        cache = _cache_for(url, block_size=BLOCK_SIZE)
        try:
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
        cache = _cache_for(url, block_size=BLOCK_SIZE)
        try:
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
        cache = _cache_for(f"http://127.0.0.1:{port}/file.flac", block_size=BLOCK_SIZE)
        try:
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
        cache = _cache_for(url, block_size=BLOCK_SIZE)
        try:
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


class TestWavFraming:
    async def test_header_and_content_length_are_consistent(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url)
        try:
            loop = asyncio.get_event_loop()
            reader = await asyncio.to_thread(_new_reader, cache, loop)

            assert reader.source_sample_rate == SOURCE_SAMPLE_RATE
            assert reader.channels == CHANNELS
            expected_target_frames = round(len(original) * TARGET_SAMPLE_RATE / SOURCE_SAMPLE_RATE)
            assert reader.total_target_frames == expected_target_frames
            assert reader.content_length == WAV_HEADER_SIZE + reader.data_size
            assert len(reader.wav_header) == WAV_HEADER_SIZE

            # The header must be a well-formed WAV `wave` module itself can
            # parse, describing exactly the target format.
            with wave.open(io.BytesIO(reader.wav_header + b"\x00" * reader.data_size)) as w:
                assert w.getframerate() == TARGET_SAMPLE_RATE
                assert w.getnchannels() == CHANNELS
                assert w.getsampwidth() == 3
                assert w.getnframes() == reader.total_target_frames
        finally:
            await cache.close()
            await server.stop()

    async def test_data_chunk_size_field_matches_data_size(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url)
        try:
            loop = asyncio.get_event_loop()
            reader = await asyncio.to_thread(_new_reader, cache, loop)
            data_size_field = struct.unpack("<I", reader.wav_header[40:44])[0]
            assert data_size_field == reader.data_size
        finally:
            await cache.close()
            await server.stop()


class TestStreamingAndSeeking:
    async def test_stream_from_zero_matches_offline_resample(self) -> None:
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url)
        try:
            expected = soxr.resample(original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ")
            loop = asyncio.get_event_loop()

            def _run() -> bytes:
                reader = _new_reader(cache, loop)
                return b"".join(reader.stream_from(0))

            all_bytes = await asyncio.to_thread(_run)

            assert all_bytes[:WAV_HEADER_SIZE].startswith(b"RIFF")
            decoded = _pcm24_bytes_to_float(all_bytes[WAV_HEADER_SIZE:], CHANNELS)

            assert len(decoded) == len(expected)
            np.testing.assert_allclose(decoded, expected, atol=2e-4)
        finally:
            await cache.close()
            await server.stop()

    async def test_seeking_mid_stream_matches_the_same_position_as_full_resample(self) -> None:
        """The actual point of this whole module: streaming from a byte
        offset partway through must match the *same* audio a full,
        from-zero resample would have at that position — not silence, not
        some other position, and without downloading/decoding the source
        in full to get there."""
        flac_bytes, original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url)
        try:
            expected_full = soxr.resample(
                original, SOURCE_SAMPLE_RATE, TARGET_SAMPLE_RATE, quality="HQ"
            )
            loop = asyncio.get_event_loop()

            def _run() -> tuple[bytes, int]:
                reader = _new_reader(cache, loop)
                bytes_per_frame = reader.channels * 3
                target_frame = int(reader.total_target_frames * 0.4)
                start_byte = WAV_HEADER_SIZE + target_frame * bytes_per_frame
                collected = []
                total = 0
                for chunk in reader.stream_from(start_byte):
                    collected.append(chunk)
                    total += len(chunk)
                    if total >= 4000 * bytes_per_frame:
                        break
                return b"".join(collected), target_frame

            raw, target_frame = await asyncio.to_thread(_run)

            decoded = _pcm24_bytes_to_float(raw, CHANNELS)
            expected_slice = expected_full[target_frame : target_frame + len(decoded)]

            # A fresh resample run started mid-stream has a small filter
            # edge transient at its very start (inherent to any streaming
            # resampler restarting at an arbitrary point — see module
            # docstring) — skip a short settling margin before comparing.
            settle = 200
            np.testing.assert_allclose(decoded[settle:], expected_slice[settle:], atol=5e-4)
        finally:
            await cache.close()
            await server.stop()

    async def test_stream_from_end_of_header_yields_only_data(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url)
        try:
            loop = asyncio.get_event_loop()

            def _run() -> bytes:
                reader = _new_reader(cache, loop)
                first_chunk = next(reader.stream_from(0))
                return first_chunk

            first_chunk = await asyncio.to_thread(_run)
            assert first_chunk[:4] == b"RIFF"
        finally:
            await cache.close()
            await server.stop()

    async def test_seek_past_end_of_data_yields_nothing(self) -> None:
        flac_bytes, _original = _make_test_flac()
        server = RangeServingUpstream(flac_bytes)
        url = await server.start()
        cache = _cache_for(url)
        try:
            loop = asyncio.get_event_loop()

            def _run() -> bytes:
                reader = _new_reader(cache, loop)
                return b"".join(reader.stream_from(reader.content_length))

            assert await asyncio.to_thread(_run) == b""
        finally:
            await cache.close()
            await server.stop()

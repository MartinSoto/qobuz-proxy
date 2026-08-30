"""Tests for TranscodingFlacReader — the on-the-fly downsample-and-WAV-wrap
pipeline built on top of LazyHttpFlacSource (see test_lazy_flac_source.py
for the seek-without-full-download validation this builds on) and, one
layer further down, CDNBlockCache (see test_dlna_cdn_block_cache.py for
its own fetch/cache/retry coverage).

Ground truth for "is the resampled audio correct" is a plain one-shot
soxr.resample() over the *fully* decoded source array — the streaming
(chunked) resampler used by the reader should match it closely wherever
it's producing the same portion of audio.
"""

import asyncio
import io
import socket
import struct
import wave
from unittest.mock import AsyncMock

import numpy as np
import soundfile as sf
import soxr
from aiohttp import web

from qobuz_proxy.backends.dlna.cdn_block_cache import CDNBlockCache
from qobuz_proxy.backends.dlna.transcoding_reader import (
    WAV_HEADER_SIZE,
    TranscodingFlacReader,
)
from qobuz_proxy.playback.stream_resolver import ResolvedStream

SOURCE_SAMPLE_RATE = 96000
TARGET_SAMPLE_RATE = 48000
DURATION_SECONDS = 1.5
CHANNELS = 2
FORMAT_ID = 27  # arbitrary — these tests don't exercise quality selection
TRACK_ID = "42"


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


def _stream(url: str) -> ResolvedStream:
    return ResolvedStream(
        url=url,
        blob="",
        format_id=FORMAT_ID,
        sample_rate=SOURCE_SAMPLE_RATE,
        bit_depth=24,
        fetched_at=0.0,
    )


def _cache_for(url: str) -> CDNBlockCache:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(url))
    return CDNBlockCache(resolver=resolver)


def _new_reader(cache: CDNBlockCache, loop: asyncio.AbstractEventLoop) -> TranscodingFlacReader:
    return TranscodingFlacReader(cache, TRACK_ID, FORMAT_ID, TARGET_SAMPLE_RATE, loop)


class RangeServingUpstream:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._runner: web.AppRunner | None = None

    async def _handle(self, request: web.Request) -> web.Response:
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

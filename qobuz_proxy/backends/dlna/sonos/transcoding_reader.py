"""
On-the-fly FLAC -> downsampled PCM/WAV transcoding, seekable.

The "virtual file" Sonos actually sees. Two things make this tractable:

- The *output* container is raw PCM in a WAV wrapper, not FLAC. WAV's
  byte-offset <-> sample-index mapping is exact, fixed-size arithmetic —
  unlike FLAC's variable-length frames, which is what makes a renderer's
  own seek-position math reliable here. Sonos parses *our* WAV header, so
  whatever byte offset it computes for a given time will always land
  exactly on a real sample boundary.
- The *input* side never downloads or decodes the whole source track.
  soundfile/libFLAC seeks within it directly (LazyHttpFlacSource, below),
  driven by nothing more than a few CDNBlockCache block reads.

Seek mapping (the crux of the whole thing): playback time is the one
quantity that survives resampling unchanged — sample *counts* differ
between the source and target rates, but the time position a given sample
represents does not. So:

    target_byte  -> target_sample  -> time_seconds  -> source_sample

and resampling resumes from source_sample, producing output starting
exactly at target_byte.

Resampling is done with soxr's streaming API (state carried across chunks
within one run) so a fresh run started at an arbitrary seek point doesn't
have to touch anything before it — normal, established practice for
seek-then-resample (any seek anywhere restarts filter state at the seek
point; the resulting edge transient is well below audible, per soxr's own
filter design).
"""

from __future__ import annotations

import asyncio
import logging
import os
import struct
from typing import TYPE_CHECKING, Coroutine, Generator, TypeVar

if TYPE_CHECKING:
    # Only for type annotations — numpy/soundfile/soxr stay genuinely
    # optional at runtime (see the class docstring); every real use below
    # imports them lazily, inside the function that needs them.
    import numpy as np

    from .cdn_block_cache import CDNBlockCache

logger = logging.getLogger(__name__)

WAV_HEADER_SIZE = 44
BYTES_PER_SAMPLE_24BIT = 3
DEFAULT_SOURCE_CHUNK_FRAMES = 8192
DEFAULT_RESAMPLE_QUALITY = "HQ"

_T = TypeVar("_T")

# Bridge-call timeout: how long a single read()/seek()-triggered fetch may
# take before LazyHttpFlacSource gives up waiting on the event loop — not a
# network timeout (CDNBlockCache has its own for the actual HTTP calls).
DEFAULT_BRIDGE_TIMEOUT_SECONDS = 30.0


def _build_wav_header(
    *, sample_rate: int, channels: int, bits_per_sample: int, data_size: int
) -> bytes:
    """A canonical, fixed 44-byte PCM WAV header — no extension chunks, so
    its size is knowable (and constant) before any audio data exists."""
    bytes_per_sample = bits_per_sample // 8
    byte_rate = sample_rate * channels * bytes_per_sample
    block_align = channels * bytes_per_sample
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + data_size,
        b"WAVE",
        b"fmt ",
        16,  # fmt chunk size (PCM)
        1,  # audio format: PCM
        channels,
        sample_rate,
        byte_rate,
        block_align,
        bits_per_sample,
        b"data",
        data_size,
    )


def _float_to_pcm24_bytes(samples: "np.ndarray") -> bytes:
    """Pack a float32 [-1, 1] numpy array (frames, channels) as little-
    endian signed 24-bit PCM. numpy has no int24 dtype, so this widens to
    int32 first and drops the (always-zero, for our range) top byte of
    each little-endian sample."""
    import numpy as np

    clipped = np.clip(samples, -1.0, 1.0)
    as_int32 = (clipped * 8_388_607.0).astype("<i4")
    as_bytes = as_int32.view("u1").reshape(-1, 4)
    packed: bytes = as_bytes[:, :3].tobytes()
    return packed


class LazyHttpFlacSource:
    """A read/seek/tell file-like object backed by CDNBlockCache.

    Lets soundfile/libFLAC seek within a remote FLAC file — using its own
    seeking logic (a SEEKTABLE lookup, or a binary search over frame
    headers when there's none) — without us downloading the whole file
    first, and without us knowing anything about FLAC's own structure at
    all. Nothing here understands FLAC — all seeking intelligence stays
    inside libFLAC, driven through this object's read()/seek() exactly as
    it would drive a real local file handle.

    ``soundfile.SoundFile`` accepts any object implementing ``read()``,
    ``seek()`` and ``tell()`` in place of a real file. libsndfile drives
    that object exactly like a local file: it calls ``seek()``/``read()``
    wherever *it* decides it needs bytes from — including, mid-seek,
    several small probing reads while it narrows down a frame boundary.
    Each of those calls becomes one or more CDNBlockCache.read_block()
    calls here.

    All of the actual CDN fetching — HTTP, retry-on-transient-failure,
    refresh-on-expired-URL, and (the part that matters most for the common
    case of sequential decode) reusing one lingering connection across
    consecutive blocks — lives one layer down, in CDNBlockCache (see
    cdn_block_cache.py). This class is just a thin sync-to-async bridge:
    libsndfile's I/O callbacks are synchronous, so every call into this
    class is meant to run inside a worker thread (``asyncio.to_thread``),
    never directly on the event loop — but CDNBlockCache itself is native
    asyncio, so every read here is bridged back onto the event loop via
    ``asyncio.run_coroutine_threadsafe``.
    """

    def __init__(
        self,
        cache: "CDNBlockCache",
        track_id: str,
        format_id: int,
        loop: asyncio.AbstractEventLoop,
        timeout: float = DEFAULT_BRIDGE_TIMEOUT_SECONDS,
    ) -> None:
        """
        Args:
            cache: Shared CDNBlockCache every block is read through.
            track_id: Qobuz track ID.
            format_id: Qobuz format tier — together with track_id, this is
                the cache's key space (see CDNBlockCache).
            loop: The event loop `cache` lives on. Must be captured by the
                caller *before* dispatching to a worker thread (there is no
                running loop to discover from inside one) — see
                TranscodingFlacReader's own callers.
            timeout: How long a single bridged read may block waiting on
                the event loop.
        """
        self._cache = cache
        self._track_id = track_id
        self._format_id = format_id
        self._loop = loop
        self._timeout = timeout
        self._pos = 0
        self._total_size = self._run(cache.get_track_size(track_id, format_id))

    # -- Python file-like protocol -------------------------------------

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            n = self._total_size - self._pos
        end = min(self._pos + n, self._total_size)
        if end <= self._pos:
            return b""
        data = self._read_range(self._pos, end)
        self._pos = end
        return data

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            new_pos = offset
        elif whence == os.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == os.SEEK_END:
            new_pos = self._total_size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        self._pos = max(0, min(new_pos, self._total_size))
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        # Nothing owned here to release — CDNBlockCache owns any open
        # connection, independent of any one decode's lifetime.
        pass

    @property
    def closed(self) -> bool:
        return False

    # -- Internals --------------------------------------------------------

    def _read_range(self, start: int, end: int) -> bytes:
        """Return bytes [start, end), via one or more cache block reads."""
        return self._run(self._read_range_async(start, end))

    async def _read_range_async(self, start: int, end: int) -> bytes:
        block_size = self._cache.block_size
        first_block = start // block_size
        last_block = (end - 1) // block_size
        # Fetched sequentially (not gathered) — a decode's reads are
        # themselves sequential, and reading blocks in order is exactly
        # what lets CDNBlockCache serve block N+1 off the connection it
        # kept open from block N instead of opening a new one.
        parts = [
            await self._cache.read_block(self._track_id, self._format_id, block_index)
            for block_index in range(first_block, last_block + 1)
        ]
        block_start = first_block * block_size
        return b"".join(parts)[start - block_start : end - block_start]

    def _run(self, coro: "Coroutine[object, object, _T]") -> _T:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=self._timeout)


class TranscodingFlacReader:
    """Serves a source FLAC URL as a downsampled, byte-exact-seekable PCM/
    WAV stream — without downloading or decoding the source in full.

    Usage:
        reader = TranscodingFlacReader(
            cache, track_id, format_id, target_sample_rate=48000, loop=loop
        )
        for chunk in reader.stream_from(start_byte):
            ...write chunk to the HTTP response...

    All I/O here is synchronous (soundfile/libFLAC's callbacks are
    synchronous) — every call into this class is meant to run inside a
    worker thread (asyncio.to_thread), never directly on the event loop.
    """

    def __init__(
        self,
        cache: "CDNBlockCache",
        track_id: str,
        format_id: int,
        target_sample_rate: int,
        loop: asyncio.AbstractEventLoop,
        *,
        source_chunk_frames: int = DEFAULT_SOURCE_CHUNK_FRAMES,
        resample_quality: str = DEFAULT_RESAMPLE_QUALITY,
    ) -> None:
        import soundfile as sf

        self._source_chunk_frames = source_chunk_frames
        self._resample_quality = resample_quality

        self._lazy_source = LazyHttpFlacSource(cache, track_id, format_id, loop)
        self._handle = sf.SoundFile(self._lazy_source)

        # soundfile ships no type stubs, so these read as Any straight off
        # self._handle — pin them down to their real types right here
        # rather than letting Any leak into every downstream computation.
        self.channels: int = self._handle.channels
        self.source_sample_rate: int = self._handle.samplerate
        self.source_frames: int = self._handle.frames
        self.target_sample_rate = target_sample_rate

        self.total_target_frames = round(
            self.source_frames * target_sample_rate / self.source_sample_rate
        )
        self.data_size = self.total_target_frames * self.channels * BYTES_PER_SAMPLE_24BIT
        self.content_length = WAV_HEADER_SIZE + self.data_size
        self.wav_header = _build_wav_header(
            sample_rate=target_sample_rate,
            channels=self.channels,
            bits_per_sample=BYTES_PER_SAMPLE_24BIT * 8,
            data_size=self.data_size,
        )

        logger.info(
            f"TranscodingFlacReader: {self.source_sample_rate}Hz -> "
            f"{target_sample_rate}Hz, {self.channels}ch, "
            f"{self.source_frames} -> {self.total_target_frames} frames "
            f"({self.content_length} bytes as 24-bit PCM/WAV)"
        )

    def stream_from(self, start_byte: int) -> Generator[bytes, None, None]:
        """Yield the virtual WAV's bytes from ``start_byte`` to EOF.

        Covers the header portion (if start_byte falls inside it) and then
        the resampled PCM data, decoding/resampling only as far as the
        caller keeps consuming — stop iterating to stop the work.
        """
        pos = start_byte

        if pos < WAV_HEADER_SIZE:
            yield self.wav_header[pos:]
            pos = WAV_HEADER_SIZE

        data_offset = pos - WAV_HEADER_SIZE
        if data_offset >= self.data_size:
            return

        bytes_per_frame = self.channels * BYTES_PER_SAMPLE_24BIT
        target_frame = data_offset // bytes_per_frame
        # A start_byte not aligned to a whole frame (shouldn't normally
        # happen — DLNA.ORG_OP-driven Range requests target the file we
        # described) is rounded down to the containing frame; any partial
        # leading bytes are simply not re-emitted, which is the same thing
        # a real file read at a misaligned offset would do.
        yield from self._stream_target_frames_from(target_frame)

    def _stream_target_frames_from(self, target_frame: int) -> Generator[bytes, None, None]:
        import soxr

        source_frame = self._target_frame_to_source_frame(target_frame)
        self._handle.seek(source_frame)

        resampler = soxr.ResampleStream(
            self.source_sample_rate,
            self.target_sample_rate,
            self.channels,
            dtype="float32",
            quality=self._resample_quality,
        )

        remaining_source = self.source_frames - source_frame
        while remaining_source > 0:
            n = min(self._source_chunk_frames, remaining_source)
            chunk = self._handle.read(n, dtype="float32", always_2d=True)
            remaining_source -= len(chunk)
            is_last = remaining_source <= 0
            resampled = resampler.resample_chunk(chunk, last=is_last)
            if len(resampled):
                yield _float_to_pcm24_bytes(resampled)

    def _target_frame_to_source_frame(self, target_frame: int) -> int:
        """The seek mapping this whole module exists for: time is what's
        invariant across resampling, not sample count."""
        time_seconds = target_frame / self.target_sample_rate
        source_frame = round(time_seconds * self.source_sample_rate)
        return max(0, min(source_frame, self.source_frames))


__all__ = ["LazyHttpFlacSource", "TranscodingFlacReader", "WAV_HEADER_SIZE"]

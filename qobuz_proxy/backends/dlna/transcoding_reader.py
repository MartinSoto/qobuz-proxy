"""
On-the-fly FLAC -> downsampled PCM/WAV transcoding, seekable.

The "virtual file" Sonos actually sees. Two things make this tractable
(see lazy_flac_source.py and the design discussion that led here):

- The *output* container is raw PCM in a WAV wrapper, not FLAC. WAV's
  byte-offset <-> sample-index mapping is exact, fixed-size arithmetic —
  unlike FLAC's variable-length frames, which is what makes a renderer's
  own seek-position math reliable here. Sonos parses *our* WAV header, so
  whatever byte offset it computes for a given time will always land
  exactly on a real sample boundary.
- The *input* side never downloads or decodes the whole source track.
  soundfile/libFLAC seeks within it directly (LazyHttpFlacSource), driven
  by nothing more than a few HTTP Range requests.

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
import struct
from typing import TYPE_CHECKING, Generator

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

        from .lazy_flac_source import LazyHttpFlacSource

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


__all__ = ["TranscodingFlacReader", "WAV_HEADER_SIZE"]

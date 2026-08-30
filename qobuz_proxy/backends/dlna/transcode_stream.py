"""
HTTP serving for AudioProxyServer's on-the-fly downsampling path.

Everything specific to *serving* a transcoded (downsampled WAV) track over
HTTP lives here — the actual downsampling engine is TranscodingFlacReader
(see transcoding_reader.py); this module is the glue between that engine
and aiohttp: answering HEAD probes with the virtual WAV's Content-Length,
and streaming GET/Range requests as byte-exact-seekable PCM/WAV. Kept out
of proxy_server.py so that file only has to know this handler exists and
dispatch a track to it — everything about downsampling, including owning
the CDNBlockCache instance every transcoded track's bytes are read
through, stays here.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from aiohttp import web

from .cdn_block_cache import CDNBlockCache
from .transcoding_reader import BYTES_PER_SAMPLE_24BIT, WAV_HEADER_SIZE, TranscodingFlacReader

if TYPE_CHECKING:
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver

    from .proxy_server import RegisteredTrack

logger = logging.getLogger(__name__)


def _parse_range_start(range_header: str) -> int:
    """Extract the start byte from a Range header ("bytes=X-...").

    Small enough to duplicate rather than share with proxy_server.py's own
    copy — importing it from there would make this module and
    proxy_server.py import each other.
    """
    try:
        return int(range_header.split("=", 1)[1].split("-", 1)[0] or 0)
    except (IndexError, ValueError):
        return 0


class TranscodeStreamHandler:
    """Serves the on-the-fly downsampling path for AudioProxyServer.

    Usage:
        handler = TranscodeStreamHandler(resolver=stream_resolver)
        ...
        response = await handler.stream(request, track)  # a GET/Range request
        response = await handler.handle_head_probe(track)  # a HEAD probe
        ...
        await handler.close()  # release the cache's one lingering connection
    """

    def __init__(self, resolver: "QobuzStreamResolver") -> None:
        # One CDNBlockCache per handler (== one per AudioProxyServer),
        # shared by every transcoded track this proxy serves. See
        # cdn_block_cache.py.
        self._cache = CDNBlockCache(resolver=resolver)

    async def close(self) -> None:
        """Release the cache's one lingering connection, if any."""
        await self._cache.close()

    async def handle_head_probe(self, track: "RegisteredTrack") -> web.Response:
        """Answer a HEAD probe for a downsampled track with the *virtual*
        (post-transcode) WAV's Content-Length, computed from the source's
        STREAMINFO alone — cheap, no decoding."""
        headers = {"Content-Type": "audio/wav", "Accept-Ranges": "bytes"}
        assert track.transcode_to_sample_rate is not None
        try:
            reader = await self._open_reader(track)
            headers["Content-Length"] = str(reader.content_length)
            logger.debug(
                f"Transcoded HEAD probe for track {track.track_id}: "
                f"Content-Length={reader.content_length}"
            )
        except Exception as e:
            logger.debug(f"Transcoded HEAD probe failed for track {track.track_id}: {e}")
        return web.Response(status=200, headers=headers)

    async def stream(
        self,
        request: web.Request,
        track: "RegisteredTrack",
    ) -> web.StreamResponse:
        """Serve a track downsampled to track.transcode_to_sample_rate as
        PCM/WAV — see TranscodingFlacReader. A fresh reader (and
        LazyHttpFlacSource) is opened per request rather than cached across
        requests — simpler, and normal playback of one track is one
        long-lived GET plus at most a few seeks, not a flood of tiny reads.
        The actual CDN bytes behind it, though, do go through self._cache
        (shared across every request this handler serves) — URL
        resolution, retry-on-expiry, and block reuse all happen there.
        """
        assert track.transcode_to_sample_rate is not None

        try:
            reader = await self._open_reader(track)
        except Exception as e:
            logger.error(f"Failed to open transcoding source for track {track.track_id}: {e}")
            return web.Response(status=502, text="Failed to open source stream")

        range_header = request.headers.get("Range")
        start_byte = _parse_range_start(range_header) if range_header else 0
        start_byte = max(0, min(start_byte, reader.content_length))

        # We're simulating a plain static file on disk — the same thing a
        # dumb NAS would serve. The renderer parsed our WAV header once (it
        # always fetches from byte 0 first) and finds its own alignment
        # from there; a static file server never needs to "help" with
        # that, it only ever needs to hand back the literal bytes that
        # exist at the requested offset. So: whatever byte the renderer
        # asks for, that's exactly what we declare *and* exactly what the
        # response starts with — never a rounded/corrected position.
        #
        # The one thing we can't avoid: our decoder only ever produces
        # whole PCM frames, so reconstructing the *true* bytes at an
        # arbitrary offset means decoding from the containing frame and
        # discarding the leading bytes that fall before the requested
        # byte before writing anything out — exactly what reading an
        # arbitrary byte offset from a real file on disk would hand back,
        # nothing more.
        bytes_per_frame = reader.channels * BYTES_PER_SAMPLE_24BIT
        if start_byte < WAV_HEADER_SIZE:
            aligned_start_byte = start_byte
        else:
            data_offset = start_byte - WAV_HEADER_SIZE
            aligned_start_byte = (
                WAV_HEADER_SIZE + (data_offset // bytes_per_frame) * bytes_per_frame
            )
        leading_trim = start_byte - aligned_start_byte

        remaining = reader.content_length - start_byte

        headers = {"Content-Type": "audio/wav", "Accept-Ranges": "bytes"}
        if range_header:
            headers["Content-Range"] = (
                f"bytes {start_byte}-{max(start_byte, reader.content_length - 1)}"
                f"/{reader.content_length}"
            )
        response = web.StreamResponse(status=206 if range_header else 200, headers=headers)
        response.content_length = remaining

        bytes_written = 0
        stream_start = time.monotonic()
        outcome = "completed"
        try:
            await response.prepare(request)

            # Drive the reader's (synchronous, blocking) generator one step
            # at a time from a worker thread — each to_thread(next, ...)
            # call runs entirely inside that call, so stopping early (the
            # client disconnects) never leaves anything running in the
            # background (no orphaned thread, no stuck queue), and
            # gen.close() below never races a generator still genuinely
            # executing.
            gen = reader.stream_from(aligned_start_byte)
            try:
                while True:
                    chunk = await asyncio.to_thread(next, gen, None)
                    if chunk is None:
                        break
                    if leading_trim:
                        if len(chunk) <= leading_trim:
                            leading_trim -= len(chunk)
                            continue
                        chunk = chunk[leading_trim:]
                        leading_trim = 0
                    await response.write(chunk)
                    bytes_written += len(chunk)
            finally:
                gen.close()
        except (ConnectionResetError, asyncio.CancelledError):
            outcome = "disconnected"
            logger.debug(f"Client disconnected mid-transcode for track {track.track_id}")
        except Exception as e:
            outcome = "error"
            logger.error(f"Transcoding stream error for track {track.track_id}: {e}")

        logger.debug(
            f"Transcode response for track {track.track_id} done: {outcome}, "
            f"{bytes_written} bytes in {time.monotonic() - stream_start:.1f}s"
        )
        return response

    async def _open_reader(self, track: "RegisteredTrack") -> TranscodingFlacReader:
        assert track.transcode_to_sample_rate is not None
        loop = asyncio.get_event_loop()
        return await asyncio.to_thread(
            TranscodingFlacReader,
            self._cache,
            track.track_id,
            track.format_id,
            track.transcode_to_sample_rate,
            loop,
        )


__all__ = ["TranscodeStreamHandler"]

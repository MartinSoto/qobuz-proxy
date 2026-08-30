"""
HTTP serving for SonosAudioProxyServer's plain (non-transcoded) CDN
passthrough path.

Where TranscodeStreamHandler (see transcode_stream.py) serves a
downsampled PCM/WAV rendering of a track, this class serves the source
FLAC/MP3 bytes completely unchanged — but, like the transcode path, reads
them through the shared CDNBlockCache rather than opening its own
independent upstream connection per request the way AudioProxyServer's
generic _proxy_stream does. That means a passthrough request gets the
same single-flight coalescing and lingering-connection reuse as a
transcode one, and the two paths share one cache/connection budget
instead of each keeping its own.

Byte-range handling mirrors TranscodeStreamHandler's own simplification:
a Range header is read only for its start byte, and every response runs
to EOF from there — DLNA renderers seeking always send an open-ended
range ("bytes=N-"), never a closed one, so this loses nothing in
practice while staying simple.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from .cdn_block_cache import CDNBlockCache

logger = logging.getLogger(__name__)


def _parse_range_start(range_header: str) -> int:
    """Extract the start byte from a Range header ("bytes=X-...").

    Small enough to duplicate rather than share with proxy_server.py's/
    transcode_stream.py's own copies — see transcode_stream.py's for why.
    """
    try:
        return int(range_header.split("=", 1)[1].split("-", 1)[0] or 0)
    except (IndexError, ValueError):
        return 0


class PassthroughStreamHandler:
    """Serves the plain CDN-passthrough path for SonosAudioProxyServer.

    Usage:
        handler = PassthroughStreamHandler(cache=shared_cdn_block_cache)
        ...
        response = await handler.stream(request, track_id, format_id, content_type)
        response = await handler.handle_head_probe(track_id, format_id, content_type)
    """

    def __init__(self, cache: "CDNBlockCache") -> None:
        self._cache = cache

    async def handle_head_probe(
        self, track_id: str, format_id: int, content_type: str
    ) -> web.Response:
        """Answer a HEAD probe with the track's real Content-Length,
        discovered via the cache's own (cheap, HEAD-only) size lookup —
        never a body transfer."""
        headers = {"Content-Type": content_type, "Accept-Ranges": "bytes"}
        try:
            size = await self._cache.get_track_size(track_id, format_id)
            headers["Content-Length"] = str(size)
        except Exception as e:
            # A probe answer without Content-Length is still useful;
            # renderers mostly check availability and type here.
            logger.debug(f"HEAD probe failed for track {track_id}: {e}")
        return web.Response(status=200, headers=headers)

    async def stream(
        self,
        request: web.Request,
        track_id: str,
        format_id: int,
        content_type: str,
    ) -> web.StreamResponse:
        """Serve the track's own bytes, completely unchanged, read through
        CDNBlockCache. All retry/expired-URL recovery already lives in the
        cache (see its module docstring) — this only has to stitch the
        blocks it gets back into the client response and stop cleanly on
        disconnect."""
        try:
            total_size = await self._cache.get_track_size(track_id, format_id)
        except Exception as e:
            logger.error(f"Failed to determine size for track {track_id}: {e}")
            return web.Response(status=502, text="Failed to resolve streaming URL")

        range_header = request.headers.get("Range")
        start = 0
        if range_header:
            start = max(0, min(_parse_range_start(range_header), total_size))

        headers = {"Content-Type": content_type, "Accept-Ranges": "bytes"}
        if range_header:
            headers["Content-Range"] = f"bytes {start}-{max(start, total_size - 1)}/{total_size}"
        response = web.StreamResponse(status=206 if range_header else 200, headers=headers)
        response.content_length = total_size - start

        bytes_sent = 0
        try:
            await response.prepare(request)
            pos = start
            chunk_size = self._cache.block_size
            while pos < total_size:
                chunk = await self._cache.read_range(
                    track_id, format_id, pos, min(pos + chunk_size, total_size)
                )
                if not chunk:
                    break
                await response.write(chunk)
                pos += len(chunk)
                bytes_sent += len(chunk)
            await response.write_eof()
        except (ConnectionResetError, ConnectionError):
            logger.debug(f"Client disconnected after {bytes_sent} bytes for track {track_id}")
        except Exception as e:
            logger.error(f"Passthrough stream error for track {track_id}: {e}")
        return response


__all__ = ["PassthroughStreamHandler"]

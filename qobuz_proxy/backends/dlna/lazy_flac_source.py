"""
Lazy, CDNBlockCache-backed file-like source for FLAC decoding.

The point of this module: let a decoder (soundfile/libFLAC) seek within a
remote FLAC file — using its *own* seeking logic (a SEEKTABLE lookup, or a
binary search over frame headers when there's none) — without us
downloading the whole file first, and without us knowing anything about
FLAC's own structure at all.

``soundfile.SoundFile`` accepts any object implementing ``read()``,
``seek()`` and ``tell()`` in place of a real file. libsndfile drives that
object exactly like a local file: it calls ``seek()``/``read()`` wherever
*it* decides it needs bytes from — including, mid-seek, several small
probing reads while it narrows down a frame boundary. Each of those calls
becomes one or more CDNBlockCache.read_block() calls here.

All of the actual CDN fetching — HTTP, retry-on-transient-failure, refresh-
on-expired-URL, and (the part that matters most for the common case of
sequential decode) reusing one lingering connection across consecutive
blocks — lives one layer down, in CDNBlockCache (see cdn_block_cache.py).
This class is now just a thin sync-to-async bridge: libsndfile's I/O
callbacks are synchronous, so every call into this class is meant to run
inside a worker thread (``asyncio.to_thread``), never directly on the
event loop — but CDNBlockCache itself is native asyncio, so every read
here is bridged back onto the event loop via
``asyncio.run_coroutine_threadsafe`` (same bridge shape as
proxy_server.py's now-removed ``_make_sync_url_refresher`` used to be).
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Coroutine, TypeVar

if TYPE_CHECKING:
    from .cdn_block_cache import CDNBlockCache

_T = TypeVar("_T")

# Bridge-call timeout: how long a single read()/seek()-triggered fetch may
# take before this gives up waiting on the event loop — not a network
# timeout (CDNBlockCache has its own for the actual HTTP calls).
DEFAULT_BRIDGE_TIMEOUT_SECONDS = 30.0


class LazyHttpFlacSource:
    """A read/seek/tell file-like object backed by CDNBlockCache.

    Nothing here understands FLAC — all seeking intelligence stays inside
    libFLAC (via soundfile), driven through this object's read()/seek()
    exactly as it would drive a real local file handle.
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
                proxy_server.py's transcode call sites.
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


__all__ = ["LazyHttpFlacSource"]

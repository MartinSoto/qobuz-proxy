"""
Global, block-granular LRU cache for bytes read from the Qobuz CDN.

Where LazyHttpFlacSource (see transcoding_reader.py) is a per-request,
per-track read-ahead buffer handed to one decode, this cache is the
opposite scope: one instance, shared across every track/format/renderer in
the process, keyed on (track_id, format_id, block_index) with a fixed
BLOCK_SIZE grid — block 0 is always [0, BLOCK_SIZE), block 1 is always
[BLOCK_SIZE, 2*BLOCK_SIZE), and so on, regardless of who asked for it or
why. That fixed grid is what makes the cache actually useful across
independent callers: two requests for "the same part of the same track"
always land on the same block key and share one fetch, instead of each
caller's own arbitrary byte range missing the other's cache entirely.

Every fetch goes through the shared QobuzStreamResolver (see
playback/stream_resolver.py) — never a stored URL — so a cache entry is
never the reason a request goes out against a Qobuz-expired signed URL;
resolver.resolve() itself only re-hits the API once its own TTL is up.

Two things keep this from being "just" an LRU dict of byte ranges:

- Single-flight coalescing: concurrent read_block() calls for the same
  key share one upstream fetch and all resolve (or fail) together, rather
  than each firing its own request.
- One lingering connection: fetching a block leaves that one HTTP
  response open (not closed) for a short idle window. A *later* request
  for exactly the block that would come next off that same response reads
  it directly off the still-open stream — no new TLS handshake, no new
  Range GET — instead of the far more common "index 0, then 1, then 2..."
  sequential-decode pattern paying for a fresh connection every block.
  Only one such connection is ever kept: opening (or reusing) one always
  retires whatever was being kept before it.

Synchronous-vs-async note: this is pure asyncio, unlike LazyHttpFlacSource
(which is deliberately synchronous because libsndfile's I/O callbacks are).
LazyHttpFlacSource is the bridge that lets a worker thread driving
libsndfile use this cache — see its module docstring — and
AudioProxyServer owns one instance of this cache per proxy server,
constructing it alongside the resolver and handing it down to every
transcoded track it serves (see proxy_server.py).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, Optional, Tuple, TypeVar

from aiohttp import ClientError, ClientResponse, ClientSession, ClientTimeout

if TYPE_CHECKING:
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver

logger = logging.getLogger(__name__)

DEFAULT_BLOCK_SIZE = 256 * 1024  # 256 KiB
DEFAULT_MAX_CACHE_SIZE = 2 * 1024 * 1024  # 2 MiB — 8 blocks at the default size
DEFAULT_CONNECTION_IDLE_SECONDS = 10.0
# Each entry is tiny (a (track_id, format_id) key -> an int), so this bounds
# entry *count* rather than bytes — unlike _blocks, nothing else ever shrinks
# this dict, so without a cap it grows for the process's entire lifetime.
DEFAULT_MAX_TRACK_SIZE_ENTRIES = 64

# Mirrors proxy_server.py's REQUEST_TIMEOUT_SECONDS/READ_TIMEOUT_SECONDS: a
# connect/read timeout, but no total timeout on the GET — a kept-open
# connection is expected to sit idle between reads.
REQUEST_TIMEOUT_SECONDS = 30

# A signed URL rejected as expired/invalid — see proxy_server.py's
# passthrough path, same set.
EXPIRED_URL_STATUS_CODES = (401, 403, 410)

# Transient upstream failures (connection reset, DNS hiccup, a stalled
# socket, a 502/503) get a brief bounded retry with linear backoff.
MAX_CONNECTION_RETRIES = 3
CONNECTION_RETRY_DELAY_SECONDS = 0.5

BlockKey = Tuple[str, int, int]  # (track_id, format_id, block_index)
TrackKey = Tuple[str, int]  # (track_id, format_id)

_T = TypeVar("_T")


class CDNBlockFetchError(Exception):
    """Raised when a block could not be fetched from the Qobuz CDN after
    exhausting retries/refreshes — a genuine, not-transient failure."""


class _ExpiredURLError(Exception):
    """Internal signal: an upstream request came back with a status that
    means "this signed URL is dead", distinct from a transient failure."""

    def __init__(self, status: int) -> None:
        super().__init__(f"upstream rejected URL as expired (status {status})")
        self.status = status


@dataclass
class _OpenConnection:
    """The one CDN connection this cache keeps alive between reads.

    `lock` guards every access to `response`/`session` so a reuse-read
    (read_block landing on `next_block`) and a discard (idle timeout, or
    this connection getting retired by a new one) can never touch the
    socket at the same time. See module docstring for the reuse story.
    """

    key: TrackKey
    next_block: int
    session: ClientSession
    response: ClientResponse
    lock: asyncio.Lock
    idle_handle: Optional[asyncio.TimerHandle] = None


class CDNBlockCache:
    """Fixed-grid LRU cache of Qobuz CDN bytes, shared process-wide.

    Usage:
        cache = CDNBlockCache(resolver=stream_resolver)
        data = await cache.read_range(track_id, format_id, start, end)
        ...
        await cache.close()  # release the one lingering connection, if any
    """

    def __init__(
        self,
        resolver: "QobuzStreamResolver",
        block_size: int = DEFAULT_BLOCK_SIZE,
        max_cache_size: int = DEFAULT_MAX_CACHE_SIZE,
        connection_idle_seconds: float = DEFAULT_CONNECTION_IDLE_SECONDS,
        request_timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
        max_track_size_entries: int = DEFAULT_MAX_TRACK_SIZE_ENTRIES,
    ) -> None:
        self._resolver = resolver
        self._block_size = block_size
        self._max_cache_size = max_cache_size
        self._connection_idle_seconds = connection_idle_seconds
        self._max_track_size_entries = max_track_size_entries
        self._get_timeout = ClientTimeout(
            total=None, connect=request_timeout_seconds, sock_read=request_timeout_seconds
        )
        self._head_timeout = ClientTimeout(total=request_timeout_seconds)

        # LRU store: insertion/access order == recency, oldest-first eviction.
        self._blocks: "Dict[BlockKey, bytes]" = {}
        self._total_bytes = 0

        # Single-flight coalescing, separately for block fetches and the
        # (track_id, format_id) -> total size discovery they depend on.
        self._inflight: "Dict[BlockKey, asyncio.Future[bytes]]" = {}
        # Bounded LRU (by entry count, not bytes — see DEFAULT_MAX_TRACK_SIZE_ENTRIES)
        # of the same shape/insertion-order-is-recency scheme as _blocks.
        self._track_sizes: Dict[TrackKey, int] = {}
        self._track_size_inflight: "Dict[TrackKey, asyncio.Future[int]]" = {}

        # The one lingering connection (see module docstring). None when
        # nothing is currently being kept open.
        self._open_conn: Optional[_OpenConnection] = None

        # Diagnostics only — not used for correctness. Real HTTP requests
        # issued (HEAD + GET), so tests/logs can confirm reuse is working.
        self.request_count = 0

    # -- Public API ---------------------------------------------------------

    @property
    def cached_bytes(self) -> int:
        return self._total_bytes

    @property
    def cached_block_count(self) -> int:
        return len(self._blocks)

    @property
    def block_size(self) -> int:
        return self._block_size

    async def get_track_size(self, track_id: str, format_id: int) -> int:
        """Total byte size of a track at a given format tier, discovered
        via HEAD and cached — the same lookup read_block() does internally
        to size the final block, exposed for callers (e.g.
        LazyHttpFlacSource) that need it up front."""
        return await self._get_track_size(track_id, format_id)

    async def read_block(self, track_id: str, format_id: int, block_index: int) -> bytes:
        """Return the bytes for one block of a track at a given format tier.

        Blocks always start at block_index * block_size; the final block of
        a track is shorter than block_size, and a block_index entirely past
        the end of the track returns b"". Concurrent calls for the same
        (track_id, format_id, block_index) share one upstream fetch and all
        resolve together.
        """
        if block_index < 0:
            raise ValueError(f"block_index must be >= 0, got {block_index}")
        key: BlockKey = (track_id, format_id, block_index)

        cached = self._blocks.get(key)
        if cached is not None:
            self._touch(key)
            return cached

        future = self._inflight.get(key)
        if future is None:
            future = asyncio.ensure_future(self._fetch_and_cache(key))
            self._inflight[key] = future
        return await future

    async def read_range(self, track_id: str, format_id: int, start: int, end: int) -> bytes:
        """Return bytes [start, end) of a track at a given format tier, via
        one or more block reads — the convenience read_block() doesn't
        offer, since it's keyed on a block index rather than a byte range.
        Callers with an arbitrary byte range in mind (e.g.
        LazyHttpFlacSource) shouldn't have to know the block grid exists
        at all to use this cache.

        Blocks are read in order, not gathered — a decode's own reads are
        themselves sequential, and reading in order is exactly what lets a
        later block reuse the connection left open by the one before it
        (see module docstring).
        """
        if start >= end:
            return b""
        first_block = start // self._block_size
        last_block = (end - 1) // self._block_size
        parts = [
            await self.read_block(track_id, format_id, block_index)
            for block_index in range(first_block, last_block + 1)
        ]
        block_start = first_block * self._block_size
        return b"".join(parts)[start - block_start : end - block_start]

    async def close(self) -> None:
        """Release the one lingering connection, if any. Safe to call even
        if nothing is currently open."""
        conn = self._open_conn
        if conn is not None:
            await self._discard_open_connection(conn)

    # -- Fetch orchestration --------------------------------------------------

    async def _fetch_and_cache(self, key: BlockKey) -> bytes:
        try:
            data = await self._fetch_block(key)
            self._insert_block(key, data)
            return data
        finally:
            self._inflight.pop(key, None)

    async def _fetch_block(self, key: BlockKey) -> bytes:
        track_id, format_id, block_index = key
        total_size = await self._get_track_size(track_id, format_id)

        start = block_index * self._block_size
        if start >= total_size:
            return b""
        end = min(start + self._block_size, total_size)
        length = end - start

        reused = await self._read_from_open_connection(track_id, format_id, block_index, length)
        if reused is not None:
            return reused

        session, response, data = await self._open_and_read_block(
            track_id, format_id, start, length
        )
        conn = _OpenConnection(
            key=(track_id, format_id),
            next_block=block_index + 1,
            session=session,
            response=response,
            lock=asyncio.Lock(),
        )
        await self._set_open_connection(conn)
        return data

    # -- LRU bookkeeping ------------------------------------------------------

    def _touch(self, key: BlockKey) -> None:
        """Mark `key` as the most recently used entry.

        Plain dicts keep insertion order, and _insert_block/eviction below
        treat that order as recency (oldest-first eviction via
        next(iter(...))) — so popping `key` and reassigning it moves it to
        the end (protected from eviction) without a separate ordering
        structure. Called on every cache hit in read_block(), which is what
        makes this LRU rather than FIFO.
        """
        data = self._blocks.pop(key)
        self._blocks[key] = data

    def _insert_block(self, key: BlockKey, data: bytes) -> None:
        if not data:
            return  # nothing to cache for a block past the end of the track
        if len(data) > self._max_cache_size:
            # Pathological config (block_size > max_cache_size) — serve it
            # without caching rather than evicting everything for nothing.
            logger.warning(
                f"CDNBlockCache: block {key} ({len(data)} bytes) exceeds max_cache_size "
                f"({self._max_cache_size}); not caching it"
            )
            return
        self._blocks[key] = data
        self._total_bytes += len(data)
        while self._total_bytes > self._max_cache_size:
            evicted_key, evicted_data = next(iter(self._blocks.items()))
            del self._blocks[evicted_key]
            self._total_bytes -= len(evicted_data)

    # -- Track size discovery --------------------------------------------------

    async def _get_track_size(self, track_id: str, format_id: int) -> int:
        key: TrackKey = (track_id, format_id)
        size = self._track_sizes.get(key)
        if size is not None:
            self._touch_track_size(key)
            return size

        future = self._track_size_inflight.get(key)
        if future is None:
            future = asyncio.ensure_future(self._discover_track_size(key))
            self._track_size_inflight[key] = future
        return await future

    async def _discover_track_size(self, key: TrackKey) -> int:
        try:
            size = await self._fetch_track_size(*key)
            self._insert_track_size(key, size)
            return size
        finally:
            self._track_size_inflight.pop(key, None)

    def _touch_track_size(self, key: TrackKey) -> None:
        """Mark `key` as the most recently used entry — same pop-then-
        reinsert idiom as _touch, for the same reason: plain dicts keep
        insertion order, so this moves `key` to the end (protected from
        eviction) without needing a separate ordering structure."""
        size = self._track_sizes.pop(key)
        self._track_sizes[key] = size

    def _insert_track_size(self, key: TrackKey, size: int) -> None:
        self._track_sizes[key] = size
        while len(self._track_sizes) > self._max_track_size_entries:
            oldest_key, _ = next(iter(self._track_sizes.items()))
            del self._track_sizes[oldest_key]

    async def _fetch_track_size(self, track_id: str, format_id: int) -> int:
        async def _attempt(url: str) -> int:
            async with ClientSession(timeout=self._head_timeout) as session:
                async with session.head(url, allow_redirects=True) as response:
                    self.request_count += 1
                    if response.status in EXPIRED_URL_STATUS_CODES:
                        raise _ExpiredURLError(response.status)
                    if response.status not in (200, 206):
                        raise CDNBlockFetchError(
                            f"HEAD failed ({response.status}) for {track_id}@{format_id}"
                        )
                    content_length = response.headers.get("Content-Length")
                    if content_length is None or not content_length.isdigit():
                        raise CDNBlockFetchError(
                            f"Upstream did not report Content-Length for {track_id}@{format_id}"
                        )
                    return int(content_length)

        return await self._do_with_retry(track_id, format_id, _attempt)

    # -- Reusing the one lingering connection --------------------------------

    async def _read_from_open_connection(
        self, track_id: str, format_id: int, block_index: int, length: int
    ) -> Optional[bytes]:
        conn = self._open_conn
        if conn is None or conn.key != (track_id, format_id) or conn.next_block != block_index:
            return None

        async with conn.lock:
            # Re-check: another coroutine may have retired/replaced this
            # connection while we were waiting for the lock.
            if self._open_conn is not conn or conn.next_block != block_index:
                return None
            self._cancel_idle_timer(conn)
            try:
                data = await conn.response.content.readexactly(length)
            except (ClientError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
                logger.debug(
                    f"CDNBlockCache: reuse read failed for track {track_id}@{format_id} "
                    f"block {block_index} ({type(e).__name__}: {e}); falling back to a "
                    "fresh connection"
                )
                conn.response.close()
                await conn.session.close()
                if self._open_conn is conn:
                    self._open_conn = None
                return None
            conn.next_block = block_index + 1
            self._arm_idle_timer(conn)
            return data

    async def _set_open_connection(self, new_conn: _OpenConnection) -> None:
        """Install `new_conn` as the one lingering connection, retiring
        whatever was being kept before it (only one is ever kept)."""
        old = self._open_conn
        self._open_conn = new_conn
        self._arm_idle_timer(new_conn)
        if old is not None and old is not new_conn:
            await self._discard_open_connection(old)

    async def _discard_open_connection(self, conn: _OpenConnection) -> None:
        self._cancel_idle_timer(conn)
        async with conn.lock:
            if self._open_conn is conn:
                self._open_conn = None
            conn.response.close()
            await conn.session.close()

    def _arm_idle_timer(self, conn: _OpenConnection) -> None:
        self._cancel_idle_timer(conn)
        loop = asyncio.get_event_loop()
        conn.idle_handle = loop.call_later(
            self._connection_idle_seconds, self._on_idle_timeout, conn
        )

    def _cancel_idle_timer(self, conn: _OpenConnection) -> None:
        if conn.idle_handle is not None:
            conn.idle_handle.cancel()
            conn.idle_handle = None

    def _on_idle_timeout(self, conn: _OpenConnection) -> None:
        asyncio.ensure_future(self._discard_open_connection(conn))

    # -- Fresh fetch + shared retry/URL-refresh policy -------------------------

    async def _open_and_read_block(
        self, track_id: str, format_id: int, start: int, length: int
    ) -> "Tuple[ClientSession, ClientResponse, bytes]":
        """Open a new connection with an open-ended Range starting at
        `start`, and read exactly `length` bytes from it. The caller takes
        ownership of the returned session/response (kept open for possible
        reuse) on success; on any failure, whatever this opened is already
        cleaned up before the exception propagates."""

        async def _attempt(url: str) -> "Tuple[ClientSession, ClientResponse, bytes]":
            session = ClientSession(timeout=self._get_timeout)
            try:
                response = await session.get(url, headers={"Range": f"bytes={start}-"})
            except Exception:
                await session.close()
                raise
            self.request_count += 1
            try:
                if response.status in EXPIRED_URL_STATUS_CODES:
                    raise _ExpiredURLError(response.status)
                if response.status not in (200, 206):
                    raise CDNBlockFetchError(
                        f"Upstream GET failed ({response.status}) for {track_id}@{format_id}"
                    )
                data = await response.content.readexactly(length)
            except Exception:
                response.close()
                await session.close()
                raise
            return session, response, data

        return await self._do_with_retry(track_id, format_id, _attempt)

    async def _do_with_retry(
        self, track_id: str, format_id: int, attempt_fn: Callable[[str], Awaitable[_T]]
    ) -> _T:
        """Run attempt_fn(url) against a resolver-supplied URL, with two
        layers of recovery:

        - _ExpiredURLError: force a fresh resolve() and retry immediately,
          once. Still expired after that is a diagnosed, permanent failure.
        - ClientError/OSError/TimeoutError/IncompleteReadError: a bounded
          number of retries with linear backoff, against a fresh resolve()
          each time (cheap: resolver has its own TTL cache).
        """
        last_error: Optional[BaseException] = None
        expired_retry_used = False
        force = False
        attempt = 0
        while True:
            stream = await self._resolver.resolve(track_id, format_id, force=force)
            force = False
            if stream is None:
                raise CDNBlockFetchError(f"No streaming URL available for {track_id}@{format_id}")
            try:
                return await attempt_fn(stream.url)
            except _ExpiredURLError as e:
                if expired_retry_used:
                    raise CDNBlockFetchError(
                        f"Upstream still rejecting URL as expired for "
                        f"{track_id}@{format_id} after refresh"
                    ) from e
                expired_retry_used = True
                force = True
                logger.info(
                    f"CDNBlockCache: upstream rejected URL as expired for "
                    f"{track_id}@{format_id} (status {e.status}) — refreshing and retrying"
                )
                continue
            except (ClientError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
                last_error = e
                attempt += 1
                if attempt > MAX_CONNECTION_RETRIES:
                    raise CDNBlockFetchError(
                        f"Qobuz CDN request failed after {MAX_CONNECTION_RETRIES} retries "
                        f"for {track_id}@{format_id} ({type(e).__name__}: {e})"
                    ) from last_error
                delay = CONNECTION_RETRY_DELAY_SECONDS * attempt
                logger.warning(
                    f"CDNBlockCache: upstream request failed for {track_id}@{format_id} "
                    f"({type(e).__name__}: {e}); retrying ({attempt}/{MAX_CONNECTION_RETRIES}) "
                    f"in {delay:.1f}s"
                )
                await asyncio.sleep(delay)


__all__ = [
    "CDNBlockCache",
    "CDNBlockFetchError",
    "DEFAULT_BLOCK_SIZE",
    "DEFAULT_MAX_CACHE_SIZE",
    "DEFAULT_MAX_TRACK_SIZE_ENTRIES",
]

"""Tests for CDNBlockCache — the global, block-granular LRU cache in front
of the Qobuz CDN (see backends/dlna/cdn_block_cache.py).

Follows the same fake-CDN-as-a-real-aiohttp-server pattern as
test_proxy_server.py, rather than mocking aiohttp internals, so the
connection-reuse assertions below (GET count, Range headers actually sent)
reflect real HTTP behavior instead of a guess at how aiohttp is used
internally.
"""

import asyncio
import socket

import pytest
from aiohttp import web
from unittest.mock import AsyncMock

from qobuz_proxy.backends.dlna.sonos.cdn_block_cache import CDNBlockCache, CDNBlockFetchError
from qobuz_proxy.playback.stream_resolver import ResolvedStream

FORMAT_ID = 6  # arbitrary — these tests don't exercise quality selection


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _stream(url: str) -> ResolvedStream:
    return ResolvedStream(
        url=url, blob="", format_id=FORMAT_ID, sample_rate=44100, bit_depth=16, fetched_at=0.0
    )


class FakeCDN:
    """A minimal real HTTP server standing in for the Qobuz CDN: answers
    HEAD with Content-Length, and GET with an open-ended Range the same way
    _open_and_read_block sends it ("bytes=X-", no end)."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.head_count = 0
        self.get_ranges: list = []  # start byte of each GET (None = no Range)
        self._runner = None
        self._abort_first_get = False
        self._aborted = False
        self._require_token: str | None = None
        self._rejected_tokens: list = []

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        # add_get() auto-registers HEAD on the same path, routed to the
        # same handler (see proxy_server.py's _handle_audio docstring for
        # the same implicit-HEAD behavior on the serving side) — so both
        # methods have to be dispatched from here rather than as separate
        # routes.
        if request.method == "HEAD":
            self.head_count += 1
            return web.Response(
                status=200,
                headers={"Content-Length": str(len(self.payload)), "Accept-Ranges": "bytes"},
            )

        if self._require_token is not None:
            token = request.query.get("token", "")
            if token != self._require_token:
                self._rejected_tokens.append(token)
                return web.Response(status=403, text="URL signature expired")

        range_header = request.headers.get("Range", "")
        start = int(range_header.removeprefix("bytes=").split("-")[0]) if range_header else 0
        self.get_ranges.append(start)
        body = self.payload[start:]

        if self._abort_first_get and not self._aborted:
            self._aborted = True
            resp = web.StreamResponse(
                status=206,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{len(self.payload) - 1}/{len(self.payload)}",
                },
            )
            await resp.prepare(request)
            # Deliver fewer bytes than a single block needs, so the
            # client's readexactly() for block 0 is guaranteed to still be
            # short when the connection dies.
            await resp.write(body[:10])
            assert request.transport is not None
            request.transport.close()
            return resp

        headers = {"Accept-Ranges": "bytes"}
        status = 200
        if range_header:
            status = 206
            headers["Content-Range"] = f"bytes {start}-{len(self.payload) - 1}/{len(self.payload)}"
        return web.Response(status=status, body=body, headers=headers)

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/file", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        port = _free_port()
        site = web.TCPSite(self._runner, "127.0.0.1", port)
        await site.start()
        return f"http://127.0.0.1:{port}/file"

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


@pytest.fixture
async def cdn():
    server = FakeCDN(bytes(range(256)))  # 256 distinct bytes, easy to slice/assert on
    url = await server.start()
    yield server, url
    await server.stop()


def _resolver_for(url: str) -> AsyncMock:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value=_stream(url))
    return resolver


class TestBlockBoundaries:
    async def test_reads_block_zero_at_offset_zero(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        data = await cache.read_block("42", FORMAT_ID, 0)

        assert data == server.payload[0:64]
        await cache.close()

    async def test_reads_block_one_at_block_size_offset(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        data = await cache.read_block("42", FORMAT_ID, 1)

        assert data == server.payload[64:128]
        await cache.close()

    async def test_final_block_is_shorter_than_block_size(self, cdn):
        server, url = cdn  # 256 bytes total, block_size=100 -> blocks of 100,100,56
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=100)

        data = await cache.read_block("42", FORMAT_ID, 2)

        assert data == server.payload[200:256]
        assert len(data) == 56
        await cache.close()

    async def test_block_entirely_past_end_of_track_is_empty(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        data = await cache.read_block("42", FORMAT_ID, 10)

        assert data == b""
        await cache.close()

    async def test_negative_block_index_rejected(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        with pytest.raises(ValueError):
            await cache.read_block("42", FORMAT_ID, -1)
        await cache.close()


class TestTrackSizeDiscovery:
    async def test_head_issued_once_and_shared_across_blocks(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("42", FORMAT_ID, 1)
        await cache.read_block("42", FORMAT_ID, 2)

        assert server.head_count == 1
        await cache.close()

    async def test_head_is_per_track_and_format(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("42", 27, 0)  # different format_id -> separate discovery
        await cache.read_block("43", FORMAT_ID, 0)  # different track -> separate discovery

        assert server.head_count == 3
        await cache.close()

    async def test_concurrent_size_discovery_is_single_flight(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        results = await asyncio.gather(
            cache.read_block("42", FORMAT_ID, 0),
            cache.read_block("42", FORMAT_ID, 1),
            cache.read_block("42", FORMAT_ID, 2),
            cache.read_block("42", FORMAT_ID, 3),
        )

        assert server.head_count == 1
        assert results[0] == server.payload[0:64]
        assert results[3] == server.payload[192:256]
        await cache.close()


class TestTrackSizeEviction:
    """_track_sizes has no relationship to _blocks' byte-based eviction —
    it's its own bounded LRU, by entry count (see
    DEFAULT_MAX_TRACK_SIZE_ENTRIES), so it doesn't grow forever over the
    life of a long-running cache.

    Uses get_track_size() directly rather than read_block(): a block read
    for an already-cached block index short-circuits before ever reaching
    _get_track_size, which would make these tests exercise the wrong thing.
    """

    async def test_evicts_least_recently_used_track_when_full(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64, max_track_size_entries=2)

        await cache.get_track_size("a", FORMAT_ID)
        await cache.get_track_size("b", FORMAT_ID)
        assert server.head_count == 2

        await cache.get_track_size("c", FORMAT_ID)  # evicts "a" (least recently used)
        assert server.head_count == 3

        server.head_count = 0
        await cache.get_track_size("a", FORMAT_ID)  # size must be rediscovered
        assert server.head_count == 1
        await cache.close()

    async def test_accessing_a_track_protects_it_from_eviction(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64, max_track_size_entries=2)

        await cache.get_track_size("a", FORMAT_ID)
        await cache.get_track_size("b", FORMAT_ID)
        await cache.get_track_size("a", FORMAT_ID)  # touch "a" -> "b" is now oldest

        await cache.get_track_size("c", FORMAT_ID)  # should evict "b", not "a"

        server.head_count = 0
        await cache.get_track_size("a", FORMAT_ID)
        assert server.head_count == 0, "track 'a' should still have its size cached"
        await cache.get_track_size("b", FORMAT_ID)
        assert server.head_count == 1, "track 'b' should have been evicted"
        await cache.close()


class TestSingleFlightCoalescing:
    async def test_concurrent_reads_of_the_same_block_share_one_fetch(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        results = await asyncio.gather(*[cache.read_block("42", FORMAT_ID, 0) for _ in range(5)])

        assert all(r == server.payload[0:64] for r in results)
        assert len(server.get_ranges) == 1
        await cache.close()

    async def test_a_failed_fetch_does_not_wedge_future_reads(self, cdn):
        server, url = cdn
        resolver = AsyncMock()
        resolver.resolve = AsyncMock(return_value=None)  # nothing available at all
        cache = CDNBlockCache(resolver=resolver, block_size=64)

        with pytest.raises(CDNBlockFetchError):
            await cache.read_block("42", FORMAT_ID, 0)

        # A retry with a working resolver must not be stuck behind the
        # failed attempt's (already-cleared) in-flight entry.
        resolver.resolve = AsyncMock(return_value=_stream(url))
        data = await cache.read_block("42", FORMAT_ID, 0)
        assert data == server.payload[0:64]
        await cache.close()


class TestLRUEviction:
    async def test_evicts_least_recently_used_block_when_full(self, cdn):
        server, url = cdn
        # block_size=64, max_cache_size=128 -> room for exactly 2 blocks
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64, max_cache_size=128)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("42", FORMAT_ID, 1)
        assert cache.cached_block_count == 2

        await cache.read_block("42", FORMAT_ID, 2)  # evicts block 0 (least recently used)
        assert cache.cached_block_count == 2
        assert cache.cached_bytes == 128

        server.get_ranges.clear()
        await cache.read_block("42", FORMAT_ID, 0)  # must be re-fetched, not served from cache
        assert server.get_ranges  # a GET actually happened
        await cache.close()

    async def test_accessing_a_block_protects_it_from_eviction(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64, max_cache_size=128)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("42", FORMAT_ID, 1)
        await cache.read_block("42", FORMAT_ID, 0)  # touch block 0 -> now block 1 is oldest

        await cache.read_block("42", FORMAT_ID, 2)  # should evict block 1, not block 0

        server.get_ranges.clear()
        await cache.read_block("42", FORMAT_ID, 0)
        assert not server.get_ranges, "block 0 should still have been cached"
        await cache.close()


class TestConnectionReuse:
    async def test_sequential_blocks_reuse_the_open_connection(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("42", FORMAT_ID, 1)
        await cache.read_block("42", FORMAT_ID, 2)

        # One open-ended GET (Range starting at 0) served all three blocks.
        assert server.get_ranges == [0]
        await cache.close()

    async def test_non_sequential_block_does_not_reuse(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("42", FORMAT_ID, 2)  # not the block right after 0

        assert server.get_ranges == [0, 128]
        await cache.close()

    async def test_only_one_connection_is_kept_across_tracks(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        await cache.read_block("42", FORMAT_ID, 0)
        await cache.read_block("43", FORMAT_ID, 0)  # different track -> new connection
        server.get_ranges.clear()
        # Whatever was kept for "42" is gone now — block 1 of "42" is no
        # longer contiguous with anything open, so it needs its own GET.
        await cache.read_block("42", FORMAT_ID, 1)

        assert server.get_ranges == [64]
        await cache.close()

    async def test_idle_connection_is_retired_after_the_configured_window(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(
            resolver=_resolver_for(url), block_size=64, connection_idle_seconds=0.05
        )

        await cache.read_block("42", FORMAT_ID, 0)
        await asyncio.sleep(0.2)  # well past the idle window

        server.get_ranges.clear()
        await cache.read_block("42", FORMAT_ID, 1)

        # The lingering connection was closed by the idle timer, so block 1
        # had to be fetched fresh rather than read off it.
        assert server.get_ranges == [64]
        await cache.close()

    async def test_close_releases_the_open_connection(self, cdn):
        server, url = cdn
        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        await cache.read_block("42", FORMAT_ID, 0)
        assert cache._open_conn is not None

        await cache.close()

        assert cache._open_conn is None


class TestExpiredUrlRefresh:
    async def test_refreshes_url_and_retries_once_on_expired_status(self, cdn):
        server, url = cdn
        server._require_token = "fresh"

        async def _resolve(track_id, format_id, force=False):
            return _stream(f"{url}?token={'fresh' if force else 'stale'}")

        resolver = AsyncMock()
        resolver.resolve = AsyncMock(side_effect=_resolve)
        cache = CDNBlockCache(resolver=resolver, block_size=64)

        data = await cache.read_block("42", FORMAT_ID, 0)

        assert data == server.payload[0:64]
        assert server._rejected_tokens == ["stale"]
        resolver.resolve.assert_awaited_with("42", FORMAT_ID, force=True)
        await cache.close()

    async def test_gives_up_after_one_failed_refresh(self, cdn):
        server, url = cdn
        server._require_token = "fresh"

        async def _resolve(track_id, format_id, force=False):
            # Refresh never actually produces a working token.
            return _stream(f"{url}?token=stale")

        resolver = AsyncMock()
        resolver.resolve = AsyncMock(side_effect=_resolve)
        cache = CDNBlockCache(resolver=resolver, block_size=64)

        with pytest.raises(CDNBlockFetchError):
            await cache.read_block("42", FORMAT_ID, 0)
        await cache.close()


class TestTransientFailureRetry:
    async def test_retries_a_dropped_connection(self, cdn):
        server, url = cdn
        server._abort_first_get = True

        cache = CDNBlockCache(resolver=_resolver_for(url), block_size=64)

        data = await cache.read_block("42", FORMAT_ID, 0)

        assert data == server.payload[0:64]
        assert len(server.get_ranges) == 2  # the aborted attempt, then a fresh retry
        await cache.close()

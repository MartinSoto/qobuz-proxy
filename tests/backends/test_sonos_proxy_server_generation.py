"""Generation-aware stream URLs: SonosAudioProxyServer tags every URL with the
queue generation (session tag, major version, queue item id) and answers 410
for a request from an earlier generation. See the module docstring of
sonos/proxy_server.py and docs/sonos-queue-handover-design.md, 5.2 and 9.2.
"""

from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlparse

import aiohttp
import pytest

from qobuz_proxy.backends.dlna.capabilities import DLNACapabilities, QOBUZ_QUALITY_CD
from qobuz_proxy.backends.dlna.sonos.proxy_server import SonosAudioProxyServer
from qobuz_proxy.playback.queue import QobuzQueue, QueueVersion, session_tag
from qobuz_proxy.playback.stream_resolver import ResolvedStream

from tests.backends.test_sonos_proxy_server_passthrough import (
    FakeCdnUpstream,
    _free_port,
    _passthrough_url,
    _resolver,
)

CAPS = DLNACapabilities(supports_flac=True, max_sample_rate=48000, max_bit_depth=24)
SESSION = "session-a"


async def _queue(session: str | None = SESSION, major: int = 5, minor: int = 1) -> QobuzQueue:
    queue = QobuzQueue()
    if session:
        queue.set_session(session)
    await queue.set_version(QueueVersion(major=major, minor=minor))
    return queue


def _proxy(queue: QobuzQueue | None, resolver: AsyncMock | None = None) -> SonosAudioProxyServer:
    return SonosAudioProxyServer(
        resolver=resolver or _cd_resolver(), queue=queue, host="127.0.0.1", port=_free_port()
    )


def _cd_resolver() -> AsyncMock:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(
        return_value=ResolvedStream(
            url="https://cdn/x",
            blob="b",
            format_id=QOBUZ_QUALITY_CD,
            sample_rate=44100,
            bit_depth=16,
            fetched_at=0.0,
        )
    )
    return resolver


def _params(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}


class TestUrlsCarryTheGeneration:
    async def test_session_version_and_item_id_are_in_the_url(self) -> None:
        proxy = _proxy(await _queue(major=5))

        resolved = await proxy.resolve_track("42", CAPS, queue_item_id=9)

        assert resolved is not None
        params = _params(resolved.proxy_url)
        assert params["s"] == session_tag(SESSION)
        assert params["v"] == "5"
        assert params["i"] == "9"

    async def test_a_new_load_changes_the_url(self) -> None:
        queue = await _queue(major=5)
        proxy = _proxy(queue)
        first = await proxy.resolve_track("42", CAPS, queue_item_id=9)

        await queue.set_version(QueueVersion(major=6, minor=1))
        second = await proxy.resolve_track("42", CAPS, queue_item_id=9)

        assert first is not None and second is not None
        assert first.proxy_url != second.proxy_url

    async def test_a_minor_bump_does_not_change_the_url(self) -> None:
        queue = await _queue(major=5, minor=1)
        proxy = _proxy(queue)
        first = await proxy.resolve_track("42", CAPS, queue_item_id=9)

        await queue.set_version(QueueVersion(major=5, minor=4))
        second = await proxy.resolve_track("42", CAPS, queue_item_id=9)

        assert first is not None and second is not None
        assert first.proxy_url == second.proxy_url

    async def test_without_a_known_session_only_the_item_id_is_added(self) -> None:
        proxy = _proxy(await _queue(session=None))

        resolved = await proxy.resolve_track("42", CAPS, queue_item_id=9)

        assert resolved is not None
        params = _params(resolved.proxy_url)
        assert "s" not in params and "v" not in params
        assert params["i"] == "9"

    async def test_without_a_queue_the_url_has_no_generation(self) -> None:
        proxy = _proxy(None)

        resolved = await proxy.resolve_track("42", CAPS)

        assert resolved is not None
        params = _params(resolved.proxy_url)
        assert not {"s", "v", "i"} & set(params)

    async def test_the_existing_parameters_are_unchanged(self) -> None:
        proxy = _proxy(await _queue())

        resolved = await proxy.resolve_track("42", CAPS, proxy_key="42_9", queue_item_id=9)

        assert resolved is not None
        params = _params(resolved.proxy_url)
        assert params["format"] == "flac"
        assert params["depth"] == "16"
        assert params["rate"] == "44100"
        assert params["item"] == "42_9"


class TestStaleDecision:
    """_is_stale_generation, case by case."""

    @pytest.fixture
    async def proxy(self) -> SonosAudioProxyServer:
        return _proxy(await _queue(major=5))

    def _tag(self) -> str:
        return session_tag(SESSION)

    async def test_the_current_generation_is_fresh(self, proxy: SonosAudioProxyServer) -> None:
        assert not proxy._is_stale_generation({"s": self._tag(), "v": "5"})

    async def test_an_earlier_load_is_stale(self, proxy: SonosAudioProxyServer) -> None:
        assert proxy._is_stale_generation({"s": self._tag(), "v": "4"})

    async def test_another_session_is_stale(self, proxy: SonosAudioProxyServer) -> None:
        assert proxy._is_stale_generation({"s": session_tag("session-b"), "v": "5"})

    async def test_a_url_without_a_generation_is_served(self, proxy: SonosAudioProxyServer) -> None:
        """Entries from before this scheme, or built before the session was known."""
        assert not proxy._is_stale_generation({})
        assert not proxy._is_stale_generation({"s": self._tag()})
        assert not proxy._is_stale_generation({"v": "4"})

    async def test_a_url_built_before_any_version_was_known_is_served(
        self, proxy: SonosAudioProxyServer
    ) -> None:
        assert not proxy._is_stale_generation({"s": self._tag(), "v": "0"})

    async def test_a_malformed_version_is_not_treated_as_stale(
        self, proxy: SonosAudioProxyServer
    ) -> None:
        assert not proxy._is_stale_generation({"s": self._tag(), "v": "abc"})

    async def test_nothing_is_stale_while_the_current_generation_is_unknown(self) -> None:
        no_version = _proxy(await _queue(major=0))
        no_session = _proxy(await _queue(session=None, major=5))
        no_queue = _proxy(None)

        for proxy in (no_version, no_session, no_queue):
            assert not proxy._is_stale_generation({"s": "deadbeef", "v": "3"})


class TestStaleRequestsGetGone:
    """End to end: a real proxy, a real client, a fake CDN."""

    async def _run(self, queue: QobuzQueue, query: str, method: str = "GET") -> int:
        payload = b"\x01" * 2000
        upstream = FakeCdnUpstream(payload)
        upstream_url = await upstream.start()
        proxy = SonosAudioProxyServer(
            resolver=_resolver(upstream_url), queue=queue, host="127.0.0.1", port=_free_port()
        )
        await proxy.start()
        try:
            url = _passthrough_url(proxy, "42") + query
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(method, url) as resp:
                    return resp.status
        finally:
            await proxy.stop()
            await upstream.stop()

    async def test_the_current_generation_is_served(self) -> None:
        queue = await _queue(major=5)

        status = await self._run(queue, f"&s={session_tag(SESSION)}&v=5&i=1")

        assert status == 200

    async def test_an_earlier_load_gets_410_for_get_and_head(self) -> None:
        queue = await _queue(major=6)
        stale = f"&s={session_tag(SESSION)}&v=5&i=1"

        assert await self._run(queue, stale, "GET") == 410
        assert await self._run(queue, stale, "HEAD") == 410

    async def test_a_minor_bump_does_not_invalidate_a_published_entry(self) -> None:
        queue = await _queue(major=5, minor=1)
        entry = f"&s={session_tag(SESSION)}&v=5&i=1"
        assert await self._run(queue, entry) == 200

        await queue.set_version(QueueVersion(major=5, minor=8))

        assert await self._run(queue, entry) == 200

    async def test_a_url_without_a_generation_is_still_served(self) -> None:
        queue = await _queue(major=5)

        assert await self._run(queue, "") == 200


class TestRewrittenQueue:
    """The same queue re-sent under a new major version keeps its entries
    playable; a different one makes them stale."""

    async def test_an_entry_of_an_unchanged_queue_is_still_served(self) -> None:
        queue = await _queue(major=5)
        await queue.apply_snapshot([(0, "42"), (1, "43")])
        proxy = _proxy(queue)
        url = (await proxy.resolve_track("42", CAPS, queue_item_id=0)).proxy_url  # type: ignore[union-attr]

        await queue.set_version(QueueVersion(major=6, minor=1))
        await queue.apply_snapshot([(0, "42"), (1, "43")])

        assert not proxy._is_stale_generation(_params(url))

    async def test_an_entry_of_a_replaced_queue_is_refused(self) -> None:
        queue = await _queue(major=5)
        await queue.apply_snapshot([(0, "42"), (1, "43")])
        proxy = _proxy(queue)
        url = (await proxy.resolve_track("42", CAPS, queue_item_id=0)).proxy_url  # type: ignore[union-attr]

        await queue.set_version(QueueVersion(major=6, minor=1))
        await queue.apply_snapshot([(0, "42"), (1, "99")])

        assert proxy._is_stale_generation(_params(url))

"""Tests for the GENA event subscription client (sonos_events.py)."""

import socket
from unittest.mock import AsyncMock

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from qobuz_proxy.backends.dlna.discovery import DiscoveredDevice
from qobuz_proxy.backends.dlna.sonos_events import (
    ZONE_GROUP_TOPOLOGY_EVENT_PATH,
    SonosEventSubscriber,
    _parse_timeout_header,
)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _sonos_device(ip: str, port: int) -> DiscoveredDevice:
    return DiscoveredDevice(friendly_name="Kitchen", ip=ip, port=port, manufacturer="Sonos, Inc.")


class _FakeGenaDevice:
    """A minimal fake Sonos device answering SUBSCRIBE/UNSUBSCRIBE."""

    def __init__(self, timeout_header: str = "Second-1800") -> None:
        self.subscribe_headers: list[dict] = []
        self.unsubscribe_headers: list[dict] = []
        self._timeout_header = timeout_header
        self._next_sid = 1

    async def handle_subscribe(self, request: web.Request) -> web.Response:
        self.subscribe_headers.append(dict(request.headers))
        if "CALLBACK" in request.headers:
            sid = f"uuid:test-sid-{self._next_sid}"
            self._next_sid += 1
        else:
            sid = request.headers.get("SID", "uuid:unknown")
        return web.Response(status=200, headers={"SID": sid, "TIMEOUT": self._timeout_header})

    async def handle_unsubscribe(self, request: web.Request) -> web.Response:
        self.unsubscribe_headers.append(dict(request.headers))
        return web.Response(status=200)

    async def start(self) -> tuple[web.AppRunner, int]:
        app = web.Application()
        app.router.add_route("SUBSCRIBE", ZONE_GROUP_TOPOLOGY_EVENT_PATH, self.handle_subscribe)
        app.router.add_route("UNSUBSCRIBE", ZONE_GROUP_TOPOLOGY_EVENT_PATH, self.handle_unsubscribe)
        port = _free_port()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", port)
        await site.start()
        return runner, port


class TestParseTimeoutHeader:
    def test_parses_seconds(self) -> None:
        assert _parse_timeout_header("Second-1800") == 1800

    def test_infinite_falls_back_to_a_concrete_value(self) -> None:
        assert _parse_timeout_header("Second-infinite") > 0

    def test_garbage_falls_back_to_default(self) -> None:
        from qobuz_proxy.backends.dlna.sonos_events import DEFAULT_TIMEOUT_SECONDS

        assert _parse_timeout_header("nonsense") == DEFAULT_TIMEOUT_SECONDS


class TestSubscribe:
    async def test_subscribes_and_stores_sid(self) -> None:
        device = _FakeGenaDevice()
        runner, port = await device.start()
        try:
            subscriber = SonosEventSubscriber()
            sub = await subscriber.subscribe(
                [_sonos_device("127.0.0.1", port)], "http://127.0.0.1:9999/sonos-events"
            )

            assert sub is not None
            assert sub.sid == "uuid:test-sid-1"
            assert sub.timeout_seconds == 1800
            assert subscriber.subscription is sub

            headers = device.subscribe_headers[0]
            assert headers["NT"] == "upnp:event"
            assert headers["CALLBACK"] == "<http://127.0.0.1:9999/sonos-events>"
        finally:
            await runner.cleanup()

    async def test_tries_next_device_when_first_is_unreachable(self) -> None:
        device = _FakeGenaDevice()
        runner, port = await device.start()
        try:
            dead_port = _free_port()  # nothing listening
            subscriber = SonosEventSubscriber()
            sub = await subscriber.subscribe(
                [
                    _sonos_device("127.0.0.1", dead_port),
                    _sonos_device("127.0.0.1", port),
                ],
                "http://127.0.0.1:9999/sonos-events",
            )

            assert sub is not None
            assert sub.port == port
        finally:
            await runner.cleanup()

    async def test_no_sonos_devices_returns_none(self) -> None:
        subscriber = SonosEventSubscriber()
        non_sonos = DiscoveredDevice(
            friendly_name="Denon", ip="127.0.0.1", port=1234, manufacturer="Denon"
        )

        sub = await subscriber.subscribe([non_sonos], "http://127.0.0.1:9999/sonos-events")

        assert sub is None


class TestRenew:
    async def test_renew_updates_subscription(self) -> None:
        device = _FakeGenaDevice()
        runner, port = await device.start()
        try:
            subscriber = SonosEventSubscriber()
            await subscriber.subscribe(
                [_sonos_device("127.0.0.1", port)], "http://127.0.0.1:9999/sonos-events"
            )
            old_sid = subscriber.subscription.sid

            result = await subscriber.renew()

            assert result is True
            assert subscriber.subscription is not None
            assert subscriber.subscription.sid == old_sid  # server echoed the same SID back
            renewal_headers = device.subscribe_headers[-1]
            assert "CALLBACK" not in renewal_headers  # renewal omits CALLBACK/NT
            assert renewal_headers["SID"] == old_sid
        finally:
            await runner.cleanup()

    async def test_renew_without_subscription_returns_false(self) -> None:
        subscriber = SonosEventSubscriber()

        assert await subscriber.renew() is False

    async def test_renew_failure_clears_subscription(self) -> None:
        device = _FakeGenaDevice()
        runner, port = await device.start()
        try:
            subscriber = SonosEventSubscriber()
            await subscriber.subscribe(
                [_sonos_device("127.0.0.1", port)], "http://127.0.0.1:9999/sonos-events"
            )
            await runner.cleanup()  # device goes away

            result = await subscriber.renew()

            assert result is False
            assert subscriber.subscription is None
        finally:
            pass


class TestUnsubscribe:
    async def test_sends_unsubscribe_and_clears_state(self) -> None:
        device = _FakeGenaDevice()
        runner, port = await device.start()
        try:
            subscriber = SonosEventSubscriber()
            await subscriber.subscribe(
                [_sonos_device("127.0.0.1", port)], "http://127.0.0.1:9999/sonos-events"
            )

            await subscriber.unsubscribe()

            assert subscriber.subscription is None
            assert len(device.unsubscribe_headers) == 1
        finally:
            await runner.cleanup()

    async def test_noop_without_a_subscription(self) -> None:
        subscriber = SonosEventSubscriber()

        await subscriber.unsubscribe()  # must not raise


class TestNotifyHandler:
    def _make_app_with_subscription(self, on_notify, sid: str = "uuid:test-sid-1"):
        subscriber = SonosEventSubscriber()
        subscriber.on_notify = on_notify
        app = web.Application()
        subscriber.register_route(app)
        return subscriber, app

    async def test_matching_sid_invokes_callback_and_returns_200(self) -> None:
        on_notify = AsyncMock()
        subscriber, app = self._make_app_with_subscription(on_notify)
        subscriber.subscription = _make_subscription("uuid:test-sid-1")

        async with TestClient(TestServer(app)) as client:
            resp = await client.request(
                "NOTIFY", "/sonos-events", headers={"SID": "uuid:test-sid-1"}, data="<xml/>"
            )
            assert resp.status == 200

        on_notify.assert_awaited_once_with("<xml/>")

    async def test_wrong_sid_returns_412_and_skips_callback(self) -> None:
        on_notify = AsyncMock()
        subscriber, app = self._make_app_with_subscription(on_notify)
        subscriber.subscription = _make_subscription("uuid:test-sid-1")

        async with TestClient(TestServer(app)) as client:
            resp = await client.request(
                "NOTIFY", "/sonos-events", headers={"SID": "uuid:some-other-sid"}, data="<xml/>"
            )
            assert resp.status == 412

        on_notify.assert_not_called()

    async def test_no_active_subscription_returns_412(self) -> None:
        on_notify = AsyncMock()
        subscriber, app = self._make_app_with_subscription(on_notify)

        async with TestClient(TestServer(app)) as client:
            resp = await client.request(
                "NOTIFY", "/sonos-events", headers={"SID": "uuid:test-sid-1"}, data="<xml/>"
            )
            assert resp.status == 412

        on_notify.assert_not_called()

    async def test_unclaimed_subscriber_returns_412(self) -> None:
        # No on_notify set at all — e.g. between a manager's stop() and the
        # next one attaching, or when the feature was never enabled.
        subscriber = SonosEventSubscriber()
        subscriber.subscription = _make_subscription("uuid:test-sid-1")
        app = web.Application()
        subscriber.register_route(app)

        async with TestClient(TestServer(app)) as client:
            resp = await client.request(
                "NOTIFY", "/sonos-events", headers={"SID": "uuid:test-sid-1"}, data="<xml/>"
            )
            assert resp.status == 412


def _make_subscription(sid: str):
    from qobuz_proxy.backends.dlna.sonos_events import GenaSubscription

    return GenaSubscription(
        sid=sid, ip="127.0.0.1", port=1400, timeout_seconds=1800, subscribed_at=0.0
    )

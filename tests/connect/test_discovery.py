"""Tests for DiscoveryService.update_name (live mDNS rename)."""

from unittest.mock import MagicMock

from zeroconf import ServiceInfo

from qobuz_proxy.config import Config
from qobuz_proxy.connect.discovery import DiscoveryService


def _make_service() -> DiscoveryService:
    config = Config()
    config.device.name = "Kitchen"
    return DiscoveryService(config=config, app_id="test-app-id")


class TestUpdateName:
    async def test_updates_config_name_even_before_mdns_registered(self) -> None:
        service = _make_service()

        await service.update_name("Kitchen, Living Room")

        assert service.config.device.name == "Kitchen, Living Room"

    async def test_pushes_updated_service_info_when_registered(self) -> None:
        service = _make_service()
        service._zeroconf = MagicMock()
        service._service_info = ServiceInfo(
            "_qobuz-connect._tcp.local.",
            "Kitchen._qobuz-connect._tcp.local.",
            addresses=[bytes([10, 0, 1, 30])],
            port=8689,
            properties={"path": "/streamcore", "Name": "Kitchen", "device_uuid": "abc"},
            # register_service() sets this in place, defaulting it to .name,
            # on the *original* object — simulate that having already run.
            server="Kitchen._qobuz-connect._tcp.local.",
        )

        await service.update_name("Kitchen, Living Room")

        service._zeroconf.update_service.assert_called_once()
        pushed_info = service._zeroconf.update_service.call_args[0][0]
        assert pushed_info.properties[b"Name"] == b"Kitchen, Living Room"
        assert pushed_info.properties[b"device_uuid"] == b"abc"  # other properties preserved
        # Instance identity is deliberately left unchanged
        assert pushed_info.name == "Kitchen._qobuz-connect._tcp.local."
        assert pushed_info.type == "_qobuz-connect._tcp.local."
        assert pushed_info.port == 8689
        assert service._service_info is pushed_info  # tracked for future updates/unregister

    async def test_carries_server_forward_to_the_new_service_info(self) -> None:
        # Regression: update_service() rejects server=None outright (unlike
        # register_service(), which defaults it in place) — a naive rebuild
        # of ServiceInfo without carrying .server forward fails with
        # "ServiceInfo must have a server".
        service = _make_service()
        service._zeroconf = MagicMock()
        service._service_info = ServiceInfo(
            "_qobuz-connect._tcp.local.",
            "Kitchen._qobuz-connect._tcp.local.",
            addresses=[bytes([10, 0, 1, 30])],
            port=8689,
            properties={"Name": "Kitchen"},
            server="Kitchen._qobuz-connect._tcp.local.",
        )

        await service.update_name("Kitchen, Living Room")

        pushed_info = service._zeroconf.update_service.call_args[0][0]
        assert pushed_info.server == "Kitchen._qobuz-connect._tcp.local."

    async def test_falls_back_to_name_if_server_was_never_set(self) -> None:
        service = _make_service()
        service._zeroconf = MagicMock()
        service._service_info = ServiceInfo(
            "_qobuz-connect._tcp.local.",
            "Kitchen._qobuz-connect._tcp.local.",
            addresses=[bytes([10, 0, 1, 30])],
            port=8689,
            properties={"Name": "Kitchen"},
            # server intentionally omitted — defensive fallback path
        )

        await service.update_name("Kitchen, Living Room")

        pushed_info = service._zeroconf.update_service.call_args[0][0]
        assert pushed_info.server == "Kitchen._qobuz-connect._tcp.local."

    async def test_noop_when_not_yet_registered(self) -> None:
        service = _make_service()
        assert service._zeroconf is None

        await service.update_name("Kitchen, Living Room")  # must not raise

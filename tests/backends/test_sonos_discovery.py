"""Tests for sonos.discovery.discover_and_enrich."""

from unittest.mock import AsyncMock, patch

from qobuz_proxy.backends.dlna.discovery import DiscoveredDevice
from qobuz_proxy.backends.dlna.sonos.discovery import discover_and_enrich
from qobuz_proxy.backends.dlna.sonos.topology import SonosZoneMember


def _device(**kwargs) -> DiscoveredDevice:  # type: ignore[no-untyped-def]
    defaults = dict(
        location="http://10.0.1.30:1400/xml/device_description.xml",
        friendly_name="10.0.1.30 - Sonos One",
        manufacturer="Sonos, Inc.",
        model_name="One",
        udn="uuid:RINCON_KITCHEN",
        ip="10.0.1.30",
        port=1400,
    )
    defaults.update(kwargs)
    return DiscoveredDevice(**defaults)


class TestDiscoverAndEnrich:
    async def test_returns_plain_scan_results_when_no_sonos_topology(self) -> None:
        devices = [_device()]
        with (
            patch(
                "qobuz_proxy.backends.dlna.sonos.discovery.discover_dlna_devices",
                AsyncMock(return_value=devices),
            ),
            patch(
                "qobuz_proxy.backends.dlna.sonos.discovery.fetch_sonos_topology",
                AsyncMock(return_value=None),
            ),
        ):
            result = await discover_and_enrich(timeout=1.0)

        assert result == devices

    async def test_enriches_when_sonos_topology_is_available(self) -> None:
        devices = [_device()]
        topology = {
            "RINCON_KITCHEN": SonosZoneMember(
                uuid="RINCON_KITCHEN",
                zone_name="Kitchen",
                invisible=False,
                is_stereo_pair=False,
            )
        }
        with (
            patch(
                "qobuz_proxy.backends.dlna.sonos.discovery.discover_dlna_devices",
                AsyncMock(return_value=devices),
            ),
            patch(
                "qobuz_proxy.backends.dlna.sonos.discovery.fetch_sonos_topology",
                AsyncMock(return_value=topology),
            ),
        ):
            result = await discover_and_enrich(timeout=1.0)

        assert len(result) == 1
        assert result[0].friendly_name == "Kitchen"

"""Tests for the group-coordinator quality lookup used by --discover-sonos."""

import socket

from aiohttp import web

from qobuz_proxy.cli import _fetch_coordinator_quality

DEVICE_DESCRIPTION = """<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <friendlyName>{name}</friendlyName>
    <manufacturer>{manufacturer}</manufacturer>
    <modelName>{model}</modelName>
    <UDN>uuid:RINCON_TEST</UDN>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>
        <controlURL>/AVTransport/Control</controlURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:ConnectionManager:1</serviceType>
        <controlURL>/ConnectionManager/Control</controlURL>
      </service>
    </serviceList>
  </device>
</root>"""

GET_PROTOCOL_INFO_RESPONSE = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>
<u:GetProtocolInfoResponse xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1">
<Source></Source>
<Sink>http-get:*:audio/flac:DLNA.ORG_PN=FLAC_192;DLNA.ORG_OP=01,http-get:*:audio/mpeg:*</Sink>
</u:GetProtocolInfoResponse>
</s:Body></s:Envelope>"""


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _start_device_server(
    manufacturer: str, model: str = "Test Player"
) -> tuple[web.AppRunner, int]:
    async def description(request: web.Request) -> web.Response:
        return web.Response(
            text=DEVICE_DESCRIPTION.format(
                name="Test Room", manufacturer=manufacturer, model=model
            ),
            content_type="text/xml",
        )

    async def protocol_info(request: web.Request) -> web.Response:
        return web.Response(text=GET_PROTOCOL_INFO_RESPONSE)

    app = web.Application()
    app.router.add_get("/xml/device_description.xml", description)
    app.router.add_post("/ConnectionManager/Control", protocol_info)

    port = _free_port()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner, port


class TestFetchCoordinatorQuality:
    async def test_sonos_device_is_capped_by_override(self) -> None:
        # The Sink advertises FLAC_192 (Hi-Res 24/192), but --discover-sonos
        # has no config/flag context of its own — it reflects the
        # out-of-the-box default (hires_downsampling off), so every Sonos
        # is conservatively capped to CD regardless of model. See
        # apply_device_overrides's hires_downsampling parameter — this
        # diagnostic command never passes it.
        runner, port = await _start_device_server(manufacturer="Sonos, Inc.")
        try:
            result = await _fetch_coordinator_quality("127.0.0.1", port)

            assert result is not None
            assert result.advertised == 27  # Hi-Res 192k, as advertised
            assert result.effective == 6  # CD, after the Sonos override
            assert result.confirmed is True
        finally:
            await runner.cleanup()

    async def test_legacy_sonos_model_is_capped_to_cd_quality(self) -> None:
        # Play:1/Play:3 are the known 16-bit-only blacklist — everything
        # else defaults to 24-bit (see SONOS_16BIT_ONLY_MODELS).
        runner, port = await _start_device_server(manufacturer="Sonos, Inc.", model="Play:1")
        try:
            result = await _fetch_coordinator_quality("127.0.0.1", port)

            assert result is not None
            assert result.advertised == 27
            assert result.effective == 6  # CD, after the legacy-model override
        finally:
            await runner.cleanup()

    async def test_non_sonos_device_is_not_capped(self) -> None:
        runner, port = await _start_device_server(manufacturer="Denon", model="AVR-X1700H")
        try:
            result = await _fetch_coordinator_quality("127.0.0.1", port)

            assert result is not None
            assert result.advertised == 27
            assert result.effective == 27
        finally:
            await runner.cleanup()

    async def test_unreachable_device_returns_none(self) -> None:
        result = await _fetch_coordinator_quality("127.0.0.1", _free_port())

        assert result is None

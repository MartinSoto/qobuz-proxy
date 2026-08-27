"""Tests for the generic DLNAClient."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from qobuz_proxy.backends.dlna.client import DLNAClient, DLNAClientError


def _make_client() -> DLNAClient:
    client = DLNAClient("10.0.0.5")
    client.device_info = MagicMock(av_transport_url="http://10.0.0.5:1400/AVTransport/Control")
    return client


class TestConnect:
    async def test_closes_session_when_description_fetch_fails(self):
        client = DLNAClient("10.0.0.5")
        session = AsyncMock()
        client._build_session = MagicMock(return_value=session)  # type: ignore[method-assign]
        client._fetch_device_description = AsyncMock(  # type: ignore[method-assign]
            side_effect=DLNAClientError("unreachable")
        )

        with pytest.raises(DLNAClientError):
            await client.connect()

        session.close.assert_awaited_once()
        assert client._session is None

    async def test_closes_session_when_device_lacks_av_transport(self):
        client = DLNAClient("10.0.0.5")
        session = AsyncMock()
        client._build_session = MagicMock(return_value=session)  # type: ignore[method-assign]
        client._fetch_device_description = AsyncMock(  # type: ignore[method-assign]
            return_value=MagicMock(av_transport_url="")
        )

        with pytest.raises(DLNAClientError):
            await client.connect()

        session.close.assert_awaited_once()
        assert client._session is None


class TestTimeStringParsing:
    def test_parses_hms(self):
        client = DLNAClient("10.0.0.5")
        assert client._time_string_to_ms("0:03:25") == 205000
        assert client._time_string_to_ms("01:00:00.500") == 3600500

    def test_not_implemented_returns_none(self):
        """Regression for BUG-10: NOT_IMPLEMENTED must read as unknown, not 0,
        or it stomps the last known position on every poll."""
        client = DLNAClient("10.0.0.5")
        assert client._time_string_to_ms("NOT_IMPLEMENTED") is None
        assert client._time_string_to_ms("") is None
        assert client._time_string_to_ms("garbage") is None

    async def test_position_info_unparseable_reltime_is_none(self):
        client = _make_client()
        client._soap_action = AsyncMock(  # type: ignore[method-assign]
            return_value="<RelTime>NOT_IMPLEMENTED</RelTime>"
        )

        assert await client.get_position_info() is None


class TestParseXmlValueExact:
    """_parse_xml_value_exact backs get_track_uri()/get_media_info()
    (TrackURI/CurrentURI) — DLNABackend._device_confirms_stopped() and
    hijack detection rely on telling "tag not found / call failed" (None,
    genuinely unknown) apart from "tag found but empty" ("", the device
    itself confirming nothing is loaded)."""

    def test_missing_tag_is_none(self):
        client = DLNAClient("10.0.0.5")
        assert client._parse_xml_value_exact("<Envelope><Other/></Envelope>", "TrackURI") is None

    def test_empty_element_is_empty_string_not_none(self):
        # ElementTree's own .text for <TrackURI></TrackURI> is None, not
        # "" — ".text or \"\"" is what turns this into a real signal.
        client = DLNAClient("10.0.0.5")
        result = client._parse_xml_value_exact("<Envelope><TrackURI/></Envelope>", "TrackURI")
        assert result == ""
        assert result is not None

    def test_present_value_is_returned(self):
        client = DLNAClient("10.0.0.5")
        xml = "<Envelope><TrackURI>http://proxy/track.flac</TrackURI></Envelope>"
        assert client._parse_xml_value_exact(xml, "TrackURI") == "http://proxy/track.flac"

    def test_unparseable_xml_is_none(self):
        client = DLNAClient("10.0.0.5")
        assert client._parse_xml_value_exact("not xml", "TrackURI") is None

    async def test_get_track_uri_surfaces_empty_string(self):
        client = _make_client()
        client._soap_action = AsyncMock(  # type: ignore[method-assign]
            return_value="<Envelope><TrackURI/></Envelope>"
        )

        result = await client.get_track_uri()

        assert result == ""
        assert result is not None

    async def test_get_track_uri_is_none_when_call_fails(self):
        client = _make_client()
        client._soap_action = AsyncMock(return_value=None)  # type: ignore[method-assign]

        assert await client.get_track_uri() is None

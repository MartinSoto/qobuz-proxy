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

"""Tests for DLNAClient Sonos queue actions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from qobuz_proxy.backends.dlna.client import DLNAClient, DLNAClientError

ADD_URI_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <u:AddURIToQueueResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
      <FirstTrackNumberEnqueued>5</FirstTrackNumberEnqueued>
      <NumTracksAdded>1</NumTracksAdded>
      <NewQueueLength>5</NewQueueLength>
    </u:AddURIToQueueResponse>
  </s:Body>
</s:Envelope>"""


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


GET_VOLUME_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <u:GetVolumeResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1">
      <CurrentVolume>33</CurrentVolume>
    </u:GetVolumeResponse>
  </s:Body>
</s:Envelope>"""

GET_GROUP_VOLUME_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <u:GetGroupVolumeResponse xmlns:u="urn:schemas-upnp-org:service:GroupRenderingControl:1">
      <CurrentVolume>47</CurrentVolume>
    </u:GetGroupVolumeResponse>
  </s:Body>
</s:Envelope>"""

DEVICE_DESCRIPTION_WITH_GROUP_RENDERING_CONTROL = """<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <friendlyName>Kitchen</friendlyName>
    <manufacturer>Sonos, Inc.</manufacturer>
    <modelName>One</modelName>
    <UDN>uuid:RINCON_KITCHEN</UDN>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>
        <controlURL>/MediaRenderer/AVTransport/Control</controlURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:RenderingControl:1</serviceType>
        <controlURL>/MediaRenderer/RenderingControl/Control</controlURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:GroupRenderingControl:1</serviceType>
        <controlURL>/MediaRenderer/GroupRenderingControl/Control</controlURL>
      </service>
    </serviceList>
  </device>
</root>"""


class TestDeviceDescriptionParsing:
    def test_captures_group_rendering_control_url_separately(self):
        client = DLNAClient("10.0.0.5")

        info = client._parse_device_description(
            DEVICE_DESCRIPTION_WITH_GROUP_RENDERING_CONTROL, "http://10.0.0.5:1400"
        )

        assert (
            info.rendering_control_url
            == "http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control"
        )
        assert (
            info.group_rendering_control_url
            == "http://10.0.0.5:1400/MediaRenderer/GroupRenderingControl/Control"
        )

    def test_plain_dlna_device_has_no_group_rendering_control_url(self):
        """A non-Sonos renderer (no GroupRenderingControl service at all) must
        fall back to the plain per-device volume, not silently lose volume
        control."""
        client = DLNAClient("10.0.0.5")
        xml = DEVICE_DESCRIPTION_WITH_GROUP_RENDERING_CONTROL.replace(
            "GroupRenderingControl", "SomeOtherControl"
        )

        info = client._parse_device_description(xml, "http://10.0.0.5:1400")

        assert (
            info.rendering_control_url
            == "http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control"
        )
        assert info.group_rendering_control_url == ""


class TestGroupVolume:
    """A Sonos group's volume must move together — see client.py's
    set_volume()/get_volume() docstrings. GroupRenderingControl scales every
    member of the dynamic group, not just the coordinator we happen to be
    connected to."""

    def _client_with(self, *, group_url: str, standard_url: str) -> DLNAClient:
        client = DLNAClient("10.0.0.5")
        client.device_info = MagicMock(
            rendering_control_url=standard_url,
            group_rendering_control_url=group_url,
        )
        return client

    async def test_set_volume_prefers_group_control_when_available(self):
        client = self._client_with(
            group_url="http://10.0.0.5:1400/MediaRenderer/GroupRenderingControl/Control",
            standard_url="http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control",
        )
        client._soap_action = AsyncMock(return_value="<ok/>")  # type: ignore[method-assign]

        assert await client.set_volume(40) is True

        call_args = client._soap_action.await_args.args
        assert call_args[0] == "http://10.0.0.5:1400/MediaRenderer/GroupRenderingControl/Control"
        assert call_args[2] == "SetGroupVolume"
        assert call_args[3] == {"InstanceID": "0", "DesiredVolume": "40"}

    async def test_set_volume_snapshots_the_group_ratio_first(self):
        """SetGroupVolume scales proportionally to the *last* snapshot, not
        to members' actual current volumes — without a fresh snapshot right
        before each change, it scales against a stale ratio (regression:
        this made the coordinator move far more than other members)."""
        client = self._client_with(
            group_url="http://10.0.0.5:1400/MediaRenderer/GroupRenderingControl/Control",
            standard_url="http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control",
        )
        client._soap_action = AsyncMock(return_value="<ok/>")  # type: ignore[method-assign]

        assert await client.set_volume(40) is True

        actions = [call.args[2] for call in client._soap_action.await_args_list]
        assert actions == ["SnapshotGroupVolume", "SetGroupVolume"]
        snapshot_args = client._soap_action.await_args_list[0].args
        assert (
            snapshot_args[0] == "http://10.0.0.5:1400/MediaRenderer/GroupRenderingControl/Control"
        )
        assert snapshot_args[3] == {"InstanceID": "0"}

    async def test_set_volume_falls_back_to_standard_without_group_control(self):
        client = self._client_with(
            group_url="",
            standard_url="http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control",
        )
        client._soap_action = AsyncMock(return_value="<ok/>")  # type: ignore[method-assign]

        assert await client.set_volume(40) is True

        call_args = client._soap_action.await_args.args
        assert call_args[0] == "http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control"
        assert call_args[2] == "SetVolume"
        assert call_args[3] == {"InstanceID": "0", "Channel": "Master", "DesiredVolume": "40"}

    async def test_get_volume_prefers_group_control_when_available(self):
        client = self._client_with(
            group_url="http://10.0.0.5:1400/MediaRenderer/GroupRenderingControl/Control",
            standard_url="http://10.0.0.5:1400/MediaRenderer/RenderingControl/Control",
        )
        client._soap_action = AsyncMock(return_value=GET_GROUP_VOLUME_RESPONSE)  # type: ignore[method-assign]

        assert await client.get_volume() == 47

        call_args = client._soap_action.await_args.args
        assert call_args[2] == "GetGroupVolume"

    async def test_get_volume_falls_back_to_standard_without_group_control(self):
        client = self._client_with(group_url="", standard_url="http://10.0.0.5:1400/x")
        client._soap_action = AsyncMock(return_value=GET_VOLUME_RESPONSE)  # type: ignore[method-assign]

        assert await client.get_volume() == 33

        call_args = client._soap_action.await_args.args
        assert call_args[2] == "GetVolume"

    async def test_set_volume_fails_cleanly_with_no_rendering_control_at_all(self):
        client = self._client_with(group_url="", standard_url="")

        assert await client.set_volume(40) is False

    async def test_get_volume_returns_none_with_no_rendering_control_at_all(self):
        client = self._client_with(group_url="", standard_url="")

        assert await client.get_volume() is None


class TestAddUriToQueue:
    async def test_returns_enqueued_track_number(self):
        client = _make_client()
        client._soap_action = AsyncMock(return_value=ADD_URI_RESPONSE)  # type: ignore[method-assign]

        result = await client.add_uri_to_queue("http://proxy/audio/222_9.flac")

        assert result == 5

    async def test_returns_none_on_failure(self):
        client = _make_client()
        client._soap_action = AsyncMock(return_value=None)  # type: ignore[method-assign]

        assert await client.add_uri_to_queue("http://proxy/audio/222_9.flac") is None


class TestRemoveTrackFromQueue:
    async def test_sends_object_id_for_track_number(self):
        client = _make_client()
        client._soap_action = AsyncMock(return_value="<ok/>")  # type: ignore[method-assign]

        assert await client.remove_track_from_queue(5)

        args = client._soap_action.await_args.args
        assert args[2] == "RemoveTrackFromQueue"
        assert args[3]["ObjectID"] == "Q:0/5"

    async def test_returns_false_on_failure(self):
        client = _make_client()
        client._soap_action = AsyncMock(return_value=None)  # type: ignore[method-assign]

        assert await client.remove_track_from_queue(5) is False

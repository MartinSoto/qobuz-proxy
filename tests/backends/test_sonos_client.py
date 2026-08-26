"""Tests for SonosClient's AVTransport-queue actions and group volume."""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends.dlna.client import DLNADeviceInfo
from qobuz_proxy.backends.dlna.sonos.client import SonosClient

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


def _make_client() -> SonosClient:
    client = SonosClient("10.0.0.5")
    client.device_info = MagicMock(av_transport_url="http://10.0.0.5:1400/AVTransport/Control")
    return client


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


GET_GROUP_VOLUME_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <u:GetGroupVolumeResponse xmlns:u="urn:schemas-upnp-org:service:GroupRenderingControl:1">
      <CurrentVolume>37</CurrentVolume>
    </u:GetGroupVolumeResponse>
  </s:Body>
</s:Envelope>"""


def _make_client_with(  # type: ignore[no-untyped-def]
    group_rendering_control_url: str = "", rendering_control_url: str = ""
) -> SonosClient:
    client = SonosClient("10.0.0.5")
    client.device_info = DLNADeviceInfo(
        rendering_control_url=rendering_control_url,
        group_rendering_control_url=group_rendering_control_url,
    )
    return client


class TestGroupVolume:
    """Volume acts on the whole dynamic group (GroupRenderingControl) when
    the device exposes it — matching what the Sonos app's own volume
    slider does for a group, instead of just the one physical speaker
    this client happens to be connected to."""

    async def test_get_volume_uses_group_control_when_available(self) -> None:
        client = _make_client_with(
            group_rendering_control_url="http://10.0.0.5:1400/GroupRenderingControl/Control",
            rendering_control_url="http://10.0.0.5:1400/RenderingControl/Control",
        )
        client._soap_action = AsyncMock(return_value=GET_GROUP_VOLUME_RESPONSE)  # type: ignore[method-assign]

        result = await client.get_volume()

        assert result == 37
        args = client._soap_action.await_args.args
        assert args[0] == "http://10.0.0.5:1400/GroupRenderingControl/Control"
        assert args[2] == "GetGroupVolume"

    async def test_get_volume_falls_back_to_rendering_control(self) -> None:
        client = _make_client_with(
            rendering_control_url="http://10.0.0.5:1400/RenderingControl/Control"
        )
        client._soap_action = AsyncMock(  # type: ignore[method-assign]
            return_value='<CurrentVolume val="42"/>'
        )
        client._parse_xml_value = MagicMock(return_value="42")  # type: ignore[method-assign]

        result = await client.get_volume()

        assert result == 42
        args = client._soap_action.await_args.args
        assert args[2] == "GetVolume"

    async def test_set_volume_snapshots_then_sets_group_volume(self) -> None:
        client = _make_client_with(
            group_rendering_control_url="http://10.0.0.5:1400/GroupRenderingControl/Control",
        )
        client._soap_action = AsyncMock(return_value="<ok/>")  # type: ignore[method-assign]

        assert await client._do_set_volume(55) is True

        calls = client._soap_action.await_args_list
        assert len(calls) == 2
        assert calls[0].args[2] == "SnapshotGroupVolume"
        assert calls[1].args[2] == "SetGroupVolume"
        assert calls[1].args[3]["DesiredVolume"] == "55"

    async def test_set_volume_falls_back_to_rendering_control(self) -> None:
        client = _make_client_with(
            rendering_control_url="http://10.0.0.5:1400/RenderingControl/Control"
        )
        client._soap_action = AsyncMock(return_value="<ok/>")  # type: ignore[method-assign]

        assert await client._do_set_volume(55) is True

        args = client._soap_action.await_args.args
        assert args[2] == "SetVolume"

"""SonosClient's whole-queue actions (queue handover): batch add, range remove,
block move, play mode, seek to a position, and reading the queue back.

Argument layouts and error codes are the ones measured on a real Sonos One
(docs/sonos-queue-handover-design.md, 9.2).
"""

import socket
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from aiohttp import web

from qobuz_proxy.backends.dlna.client import DLNADeviceInfo, SoapResult
from qobuz_proxy.backends.dlna.sonos.client import (
    ERROR_STALE_UPDATE_ID,
    MAX_URIS_PER_ADD,
    SonosClient,
)

ADD_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>
<u:AddMultipleURIsToQueueResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
<FirstTrackNumberEnqueued>4</FirstTrackNumberEnqueued><NumTracksAdded>3</NumTracksAdded>
<NewQueueLength>6</NewQueueLength><NewUpdateID>590</NewUpdateID>
</u:AddMultipleURIsToQueueResponse></s:Body></s:Envelope>"""

REMOVE_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>
<u:RemoveTrackRangeFromQueueResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
<NewUpdateID>17</NewUpdateID></u:RemoveTrackRangeFromQueueResponse></s:Body></s:Envelope>"""

EMPTY_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>
<u:ReorderTracksInQueueResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>
</s:Body></s:Envelope>"""

# The DIDL travels XML-escaped inside <Result>, exactly like the real device.
BROWSE_RESPONSE = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>
<u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
<Result>&lt;DIDL-Lite xmlns:dc=&quot;http://purl.org/dc/elements/1.1/&quot; xmlns:upnp=&quot;urn:schemas-upnp-org:metadata-1-0/upnp/&quot; xmlns=&quot;urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/&quot;&gt;
&lt;item id=&quot;Q:0/1&quot; parentID=&quot;Q:0&quot; restricted=&quot;true&quot;&gt;&lt;res protocolInfo=&quot;http-get:*:audio/flac:*&quot;&gt;http://proxy:7120/audio/42?format=flac&amp;amp;depth=16&amp;amp;rate=44100&amp;amp;i=0&lt;/res&gt;&lt;dc:title&gt;First &amp;amp; best&lt;/dc:title&gt;&lt;/item&gt;
&lt;item id=&quot;Q:0/2&quot; parentID=&quot;Q:0&quot; restricted=&quot;true&quot;&gt;&lt;res protocolInfo=&quot;http-get:*:audio/wav:*&quot;&gt;http://proxy:7120/audio/43?format=wav&amp;amp;i=1&lt;/res&gt;&lt;dc:title&gt;Second&lt;/dc:title&gt;&lt;/item&gt;
&lt;/DIDL-Lite&gt;</Result>
<NumberReturned>2</NumberReturned><TotalMatches>7</TotalMatches><UpdateID>588</UpdateID>
</u:BrowseResponse></s:Body></s:Envelope>"""


def _client() -> SonosClient:
    client = SonosClient("10.0.0.5")
    client.device_info = MagicMock(
        av_transport_url="http://10.0.0.5:1400/AVTransport/Control",
        content_directory_url="http://10.0.0.5:1400/MediaServer/ContentDirectory/Control",
    )
    return client


def _answer(client: SonosClient, result: SoapResult) -> AsyncMock:
    client._soap_action_detailed = AsyncMock(return_value=result)  # type: ignore[method-assign]
    return client._soap_action_detailed  # type: ignore[return-value]


class TestAddUrisToQueue:
    async def test_sends_one_call_with_joined_uris_and_metadata(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body=ADD_RESPONSE))

        result = await client.add_uris_to_queue(
            ["http://p/a", "http://p/b", "http://p/c"], ["<d1/>", "<d2/>", "<d3/>"], position=4
        )

        args = call.await_args.args
        assert args[2] == "AddMultipleURIsToQueue"
        sent = args[3]
        assert sent["EnqueuedURIs"] == "http://p/a http://p/b http://p/c"
        assert sent["EnqueuedURIsMetaData"] == "<d1/> <d2/> <d3/>"
        assert sent["NumberOfURIs"] == "3"
        assert sent["DesiredFirstTrackNumberEnqueued"] == "4"
        assert sent["EnqueueAsNext"] == "0"
        assert sent["UpdateID"] == "0"
        assert result.ok and (result.first_position, result.added) == (4, 3)
        assert (result.queue_length, result.update_id) == (6, 590)

    async def test_is_sent_once_and_a_upnp_error_is_final(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body=ADD_RESPONSE))

        await client.add_uris_to_queue(["u"], ["d"])

        assert call.await_args.kwargs == {"max_retries": 1, "retry_upnp_errors": False}

    async def test_the_batch_limit_is_sixteen(self) -> None:
        client = _client()
        _answer(client, SoapResult(success=True, body=ADD_RESPONSE))
        uris = [f"u{i}" for i in range(MAX_URIS_PER_ADD)]

        assert (await client.add_uris_to_queue(uris, ["d"] * 16)).ok
        with pytest.raises(ValueError):
            await client.add_uris_to_queue(uris + ["u16"], ["d"] * 17)

    async def test_mismatched_metadata_and_empty_input_are_refused(self) -> None:
        client = _client()
        _answer(client, SoapResult(success=True, body=ADD_RESPONSE))

        with pytest.raises(ValueError):
            await client.add_uris_to_queue(["a", "b"], ["d"])
        with pytest.raises(ValueError):
            await client.add_uris_to_queue([], [])

    async def test_a_device_error_is_reported_with_its_code(self) -> None:
        client = _client()
        _answer(client, SoapResult(success=False, error_code=ERROR_STALE_UPDATE_ID))

        result = await client.add_uris_to_queue(["u"], ["d"], update_id=5)

        assert not result.ok and result.stale_update_id

    async def test_without_a_device_nothing_is_sent(self) -> None:
        client = SonosClient("10.0.0.5")

        assert not (await client.add_uris_to_queue(["u"], ["d"])).ok


class TestRemoveAndMove:
    async def test_remove_range_arguments_and_update_id(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body=REMOVE_RESPONSE))

        result = await client.remove_queue_range(5, 2, update_id=16)

        args = call.await_args.args
        assert args[2] == "RemoveTrackRangeFromQueue"
        assert args[3] == {
            "InstanceID": "0",
            "UpdateID": "16",
            "StartingIndex": "5",
            "NumberOfTracks": "2",
        }
        assert result.ok and result.update_id == 17

    async def test_move_range_arguments(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body=EMPTY_RESPONSE))

        result = await client.move_queue_range(1, 2, 5)

        args = call.await_args.args
        assert args[2] == "ReorderTracksInQueue"
        assert args[3]["StartingIndex"] == "1"
        assert args[3]["NumberOfTracks"] == "2"
        assert args[3]["InsertBefore"] == "5"
        assert result.ok and result.update_id is None  # the reply carries none

    async def test_invalid_range_error_is_reported(self) -> None:
        client = _client()
        _answer(client, SoapResult(success=False, error_code=402))

        result = await client.remove_queue_range(5, 10)

        assert not result.ok and result.error_code == 402


class TestPlayModeAndSeek:
    async def test_set_play_mode(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body="<ok/>"))

        assert await client.set_play_mode("REPEAT_ALL")

        assert call.await_args.args[2] == "SetPlayMode"
        assert call.await_args.args[3]["NewPlayMode"] == "REPEAT_ALL"

    async def test_seek_to_queue_position_is_a_track_nr_seek(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body="<ok/>"))

        assert await client.seek_to_queue_position(7)

        assert call.await_args.args[2] == "Seek"
        assert call.await_args.args[3]["Unit"] == "TRACK_NR"
        assert call.await_args.args[3]["Target"] == "7"

    async def test_failures_read_as_false(self) -> None:
        client = _client()
        _answer(client, SoapResult(success=False, error_code=800))

        assert not await client.set_play_mode("NORMAL")
        assert not await client.seek_to_queue_position(1)


class TestBrowseQueue:
    async def test_parses_entries_total_and_update_id(self) -> None:
        client = _client()
        call = _answer(client, SoapResult(success=True, body=BROWSE_RESPONSE))

        page = await client.browse_queue(0, 100)

        assert call.await_args.args[2] == "Browse"
        assert call.await_args.args[3]["ObjectID"] == "Q:0"
        assert page is not None
        assert (page.total, page.update_id) == (7, 588)
        assert [e.position for e in page.entries] == [1, 2]
        assert page.entries[0].title == "First & best"
        assert page.entries[0].url == (
            "http://proxy:7120/audio/42?format=flac&depth=16&rate=44100&i=0"
        )
        assert page.entries[1].url == "http://proxy:7120/audio/43?format=wav&i=1"

    async def test_a_failed_or_unparseable_read_is_none(self) -> None:
        client = _client()
        _answer(client, SoapResult(success=False))
        assert await client.browse_queue() is None

        _answer(client, SoapResult(success=True, body="not xml"))
        assert await client.browse_queue() is None

    async def test_without_a_content_directory_url_is_none(self) -> None:
        client = SonosClient("10.0.0.5")
        client.device_info = DLNADeviceInfo()

        assert await client.browse_queue() is None


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestNoBlindRetryOverRealHttp:
    """The retry behavior itself: a device that answers a UPnP error."""

    async def _run(self, **soap_kwargs: object) -> int:
        requests = 0

        async def handler(request: web.Request) -> web.Response:
            nonlocal requests
            requests += 1
            return web.Response(
                status=500,
                text=(
                    '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>'
                    "<s:Fault><detail><UPnPError><errorCode>1028</errorCode></UPnPError>"
                    "</detail></s:Fault></s:Body></s:Envelope>"
                ),
            )

        app = web.Application()
        app.router.add_post("/ctl", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        port = _free_port()
        await web.TCPSite(runner, "127.0.0.1", port).start()
        client = SonosClient("127.0.0.1")
        client._session = aiohttp.ClientSession()
        try:
            result = await client._soap_action_detailed(
                f"http://127.0.0.1:{port}/ctl",
                "urn:schemas-upnp-org:service:AVTransport:1",
                "RemoveTrackRangeFromQueue",
                {"InstanceID": "0"},
                **soap_kwargs,  # type: ignore[arg-type]
            )
            assert not result.success and result.error_code == 1028
            return requests
        finally:
            await client._session.close()
            await runner.cleanup()

    async def test_a_upnp_error_is_not_retried_when_asked_not_to(self) -> None:
        assert await self._run(max_retries=3, retry_upnp_errors=False) == 1

"""ProxyEntryBuilder: real URLs and DIDL for queue entries, from batch metadata
and the proxy's publish-time container choice (no per-track stream request)."""

from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlparse
from xml.etree import ElementTree as ET

from qobuz_proxy.backends.dlna.capabilities import DLNACapabilities
from qobuz_proxy.backends.dlna.sonos.audio_url import EntryKey, parse_entry_url
from qobuz_proxy.backends.dlna.sonos.entry_builder import ProxyEntryBuilder
from qobuz_proxy.backends.dlna.sonos.proxy_server import SonosAudioProxyServer
from qobuz_proxy.backends.dlna.sonos.queue_sync import SonosQueueSync
from qobuz_proxy.playback.queue import QobuzQueue

from tests.backends.fake_sonos_queue import FakeSonosQueue

CAPS = DLNACapabilities(supports_flac=True, max_sample_rate=48000, max_bit_depth=24)

CATALOG = {
    "100": {
        "title": "Plain CD",
        "artist": "Artist A",
        "album": "Album A",
        "album_art_url": "http://art/a.jpg",
        "duration_ms": 200_000,
        "max_sample_rate_hz": 44100,
        "max_bit_depth": 16,
    },
    "200": {
        "title": "Hi-Res & More",
        "artist": "Artist B",
        "album": "Album B",
        "album_art_url": "http://art/b.jpg",
        "duration_ms": 300_000,
        "max_sample_rate_hz": 96000,
        "max_bit_depth": 24,
    },
}


def K(track: int, item: int) -> EntryKey:
    return EntryKey(str(track), item)


def make(
    queue: QobuzQueue | None = None, gain: bool = False
) -> tuple[ProxyEntryBuilder, AsyncMock]:
    api = AsyncMock()
    api.get_tracks_metadata = AsyncMock(
        side_effect=lambda ids: {i: CATALOG[i] for i in ids if i in CATALOG}
    )
    api.get_tracks_audio_info = AsyncMock(return_value={})
    resolver = AsyncMock()
    resolver.api_client = api
    proxy = SonosAudioProxyServer(
        resolver=resolver,
        queue=queue,
        hires_downsampling=True,
        hires_downsampling_gain=gain,
        host="127.0.0.1",
        port=7120,
    )
    return ProxyEntryBuilder(proxy, api, lambda: CAPS), api.get_tracks_metadata


class TestBuild:
    async def test_a_cd_track_is_a_flac_passthrough_entry(self) -> None:
        builder, _ = make()

        [entry] = await builder.build([K(100, 7)])

        q = parse_qs(urlparse(entry.url).query)
        assert (q["format"], q["depth"], q["rate"]) == (["flac"], ["16"], ["44100"])
        assert q["i"] == ["7"] and q["item"] == ["100_7"]
        assert parse_entry_url(entry.url).key == K(100, 7)  # type: ignore[union-attr]

    async def test_a_hires_track_is_a_wav_transcode_entry(self) -> None:
        builder, _ = make()

        [entry] = await builder.build([K(200, 3)])

        q = parse_qs(urlparse(entry.url).query)
        assert (q["format"], q["depth"], q["rate"]) == (["wav"], ["24"], ["48000"])

    async def test_the_didl_describes_the_track_and_declares_the_right_mime_type(self) -> None:
        builder, _ = make()

        cd, hires = await builder.build([K(100, 1), K(200, 2)])

        for entry, mime, title in (
            (cd, "audio/flac", "Plain CD"),
            (hires, "audio/wav", "Hi-Res & More"),
        ):
            root = ET.fromstring(entry.didl)
            ns = {
                "d": "urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/",
                "dc": "http://purl.org/dc/elements/1.1/",
                "u": "urn:schemas-upnp-org:metadata-1-0/upnp/",
            }
            item = root.find("d:item", ns)
            assert item is not None
            assert item.findtext("dc:title", namespaces=ns) == title
            res = item.find("d:res", ns)
            assert res is not None and f":{mime}:" in res.attrib["protocolInfo"]
            assert res.text == entry.url
        assert "Artist A" in cd.didl and "http://art/a.jpg" in cd.didl and "0:03:20" in cd.didl

    async def test_the_batch_is_one_metadata_request_and_is_cached(self) -> None:
        builder, fetch = make()

        await builder.build([K(100, 1), K(200, 2), K(100, 3)])
        await builder.build([K(100, 4), K(200, 5)])

        assert fetch.await_count == 1
        assert sorted(fetch.await_args_list[0].args[0]) == ["100", "200"]

    async def test_a_track_qobuz_does_not_know_still_gets_an_entry(self) -> None:
        builder, _ = make()

        [entry] = await builder.build([K(999, 1)])

        assert parse_entry_url(entry.url).key == K(999, 1)  # type: ignore[union-attr]
        assert "Track 999" in entry.didl
        assert "format=wav" in entry.url  # unknown format: a transcode can take anything

    async def test_the_generation_is_part_of_the_url(self) -> None:
        queue = QobuzQueue()
        queue.set_session("session")
        queue._version.major = 6
        builder, _ = make(queue)

        [entry] = await builder.build([K(100, 1)])

        parsed = parse_entry_url(entry.url)
        assert parsed is not None and parsed.major == 6 and parsed.session


class TestWithTheQueueSync:
    async def test_loading_a_queue_with_real_entries(self) -> None:
        builder, _ = make()
        device = FakeSonosQueue()
        sync = SonosQueueSync(device, builder, name="test")
        desired = [K(100 if i % 2 else 200, i) for i in range(1, 21)]

        await sync.load(desired, start=desired[0])

        assert sync.mirror == desired
        assert len(device.entries) == 20

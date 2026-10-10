"""Publish-time container choice (sonos/container.py and
SonosAudioProxyServer.publish_track): the decision made from a track's best
available format, without asking Qobuz for the stream.

The equivalence test is the important one: it runs the same tracks through the
request-time decision (resolve_track, which looks at the stream Qobuz actually
returns) and the publish-time one, and requires identical URLs, so the two
cannot drift apart.
"""

from itertools import product
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlparse

import pytest

from qobuz_proxy.backends.dlna.capabilities import (
    DLNACapabilities,
    QOBUZ_QUALITY_192K,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_MP3,
)
from qobuz_proxy.backends.dlna.sonos.container import choose_container
from qobuz_proxy.backends.dlna.sonos.proxy_server import SonosAudioProxyServer
from qobuz_proxy.playback.stream_resolver import ResolvedStream

SONOS = DLNACapabilities(supports_flac=True, max_sample_rate=48000, max_bit_depth=24)
CD_ONLY = DLNACapabilities(supports_flac=True, max_sample_rate=44100, max_bit_depth=16)
NO_FLAC = DLNACapabilities(supports_flac=False)

# what each Qobuz tier delivers at most: tier -> (rate, depth)
TIER_LIMITS = {
    QOBUZ_QUALITY_MP3: (44100, 16),
    QOBUZ_QUALITY_CD: (44100, 16),
    QOBUZ_QUALITY_96K: (96000, 24),
    QOBUZ_QUALITY_192K: (192000, 24),
}
SOURCES = [  # (best rate Hz, best bit depth) a track can have
    (44100, 16),
    (48000, 24),
    (88200, 24),
    (96000, 24),
    (176400, 24),
    (192000, 24),
]


class TestChooseContainer:
    def test_a_cd_track_passes_through_on_the_cd_tier(self) -> None:
        c = choose_container(44100, 16, SONOS, hires_downsampling=True)

        assert (c.fmt, c.depth, c.rate, c.content_type) == ("flac", 16, 44100, "audio/flac")
        assert c.format_id == QOBUZ_QUALITY_CD
        assert (c.sample_rate, c.bit_depth) == (44100, 16)

    def test_a_48k_24bit_track_passes_through_on_the_96k_tier(self) -> None:
        c = choose_container(48000, 24, SONOS, hires_downsampling=True)

        assert (c.fmt, c.depth, c.rate) == ("flac", 24, 96000)
        assert c.format_id == QOBUZ_QUALITY_96K
        assert (c.sample_rate, c.bit_depth) == (48000, 24)

    def test_a_hires_track_is_downsampled_to_the_device_rate(self) -> None:
        c = choose_container(96000, 24, SONOS, hires_downsampling=True)

        assert (c.fmt, c.depth, c.rate, c.content_type) == ("wav", 24, 48000, "audio/wav")
        assert (c.sample_rate, c.bit_depth) == (48000, 24)

    def test_a_hires_track_falls_back_to_cd_when_downsampling_is_off(self) -> None:
        c = choose_container(96000, 24, SONOS, hires_downsampling=False)

        assert (c.fmt, c.depth, c.rate) == ("flac", 16, 44100)
        assert c.format_id == QOBUZ_QUALITY_CD

    def test_a_device_without_24_bit_never_gets_wav(self) -> None:
        c = choose_container(96000, 24, CD_ONLY, hires_downsampling=True)

        assert c.fmt == "flac" and c.depth == 16

    def test_force_transcode_keeps_the_tracks_own_rate_when_it_already_fits(self) -> None:
        """Playback gain is applied in the transcoder, so every FLAC track goes through it."""
        c = choose_container(44100, 16, SONOS, hires_downsampling=True, force_transcode=True)

        assert (c.fmt, c.rate, c.sample_rate) == ("wav", 44100, 44100)

    def test_a_device_without_flac_gets_mp3(self) -> None:
        for caps in (NO_FLAC, None):
            c = choose_container(96000, 24, caps, hires_downsampling=True, force_transcode=True)

            assert (c.fmt, c.content_type, c.format_id) == ("mp3", "audio/mpeg", QOBUZ_QUALITY_MP3)

    def test_an_unknown_format_is_never_published_as_flac_passthrough(self) -> None:
        with_downsampling = choose_container(0, 0, SONOS, hires_downsampling=True)
        without = choose_container(0, 0, SONOS, hires_downsampling=False)

        assert with_downsampling.fmt == "wav"  # a transcode can take any source
        assert (without.fmt, without.depth, without.rate) == ("flac", 16, 44100)  # CD always fits

    def test_a_manual_quality_cap_limits_the_source(self) -> None:
        c = choose_container(96000, 24, SONOS, hires_downsampling=True, forced_format_id=6)

        assert (c.fmt, c.depth, c.rate) == ("flac", 16, 44100)  # the CD tier fits, no transcode


class _FakeQobuz:
    """Serves the best format a track has at or below the requested tier."""

    def __init__(self, rate: int, depth: int) -> None:
        self.rate, self.depth = rate, depth

    async def resolve(self, track_id: str, format_id: int, force: bool = False) -> ResolvedStream:
        tier_rate, tier_depth = TIER_LIMITS.get(format_id, (192000, 24))
        rate, depth = min(self.rate, tier_rate), min(self.depth, tier_depth)
        served_tier = (
            QOBUZ_QUALITY_MP3
            if format_id == QOBUZ_QUALITY_MP3
            else QOBUZ_QUALITY_CD
            if depth <= 16 and rate <= 44100
            else QOBUZ_QUALITY_96K
            if rate <= 96000
            else QOBUZ_QUALITY_192K
        )
        return ResolvedStream(
            url="https://cdn/x",
            blob="b",
            format_id=served_tier,
            sample_rate=rate,
            bit_depth=depth,
            fetched_at=0.0,
        )


def _proxy(source: tuple[int, int], *, downsampling: bool, gain: bool) -> SonosAudioProxyServer:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=_FakeQobuz(*source).resolve)
    resolver.api_client.get_tracks_audio_info = AsyncMock(return_value={})
    return SonosAudioProxyServer(
        resolver=resolver,
        hires_downsampling=downsampling or gain,
        hires_downsampling_gain=gain,
        host="127.0.0.1",
        port=0,
    )


def _shape(url: str) -> dict[str, str]:
    q = parse_qs(urlparse(url).query)
    return {k: q[k][0] for k in ("format", "depth", "rate")}


class TestMatchesTheRequestTimeDecision:
    @pytest.mark.parametrize(
        "caps", [SONOS, CD_ONLY, NO_FLAC, None], ids=["sonos", "cd-only", "no-flac", "no-caps"]
    )
    async def test_same_urls_as_resolve_track(self, caps: DLNACapabilities | None) -> None:
        combos = product(SOURCES, (False, True), (False, True), (None, 6, 7, 5))
        for source, downsampling, gain, forced in combos:
            if (caps is None or not caps.supports_flac) and forced not in (None, 5):
                # Deliberate difference: with a manual FLAC-tier override the
                # request-time path still serves FLAC to a device that cannot
                # play it; the publish-time choice always gives such a device MP3.
                continue
            proxy = _proxy(source, downsampling=downsampling, gain=gain)
            at_request = await proxy.resolve_track("42", caps, forced_format_id=forced)
            at_publish = proxy.publish_track(
                "42",
                caps,
                max_sample_rate_hz=source[0],
                max_bit_depth=source[1],
                forced_format_id=forced,
            )
            where = f"source={source} downsampling={downsampling} gain={gain} forced={forced}"

            assert at_request is not None, where
            assert _shape(at_publish.proxy_url) == _shape(at_request.proxy_url), where
            assert at_publish.content_type == at_request.content_type, where
            assert (at_publish.sample_rate, at_publish.bit_depth) == (
                at_request.sample_rate,
                at_request.bit_depth,
            ), where
            assert at_publish.format_id == at_request.format_id, where


class TestPublishTrack:
    def test_makes_no_request_to_qobuz(self) -> None:
        proxy = _proxy((96000, 24), downsampling=True, gain=False)

        proxy.publish_track("42", SONOS, max_sample_rate_hz=96000, max_bit_depth=24)

        proxy._resolver.resolve.assert_not_called()  # type: ignore[attr-defined]

    def test_blob_is_unknown_until_the_device_asks(self) -> None:
        proxy = _proxy((44100, 16), downsampling=True, gain=False)

        resolved = proxy.publish_track("42", SONOS, max_sample_rate_hz=44100, max_bit_depth=16)

        assert resolved.blob == ""

    def test_carries_the_generation_and_item_id(self) -> None:
        from qobuz_proxy.playback.queue import QobuzQueue, session_tag

        queue = QobuzQueue()
        queue.set_session("s")
        queue._version.major = 7  # a loaded queue
        proxy = SonosAudioProxyServer(
            resolver=AsyncMock(), queue=queue, hires_downsampling=True, host="127.0.0.1", port=0
        )

        resolved = proxy.publish_track(
            "42",
            SONOS,
            max_sample_rate_hz=96000,
            max_bit_depth=24,
            proxy_key="42_9",
            queue_item_id=9,
        )

        q = parse_qs(urlparse(resolved.proxy_url).query)
        assert q["s"] == [session_tag("s")] and q["v"] == ["7"] and q["i"] == ["9"]
        assert q["item"] == ["42_9"]

    def test_gain_is_only_applied_to_a_transcoded_entry(self) -> None:
        proxy = _proxy((96000, 24), downsampling=True, gain=False)

        wav = proxy.publish_track(
            "42", SONOS, max_sample_rate_hz=96000, max_bit_depth=24, gain_db=-3.5
        )
        flac = proxy.publish_track(
            "43", SONOS, max_sample_rate_hz=44100, max_bit_depth=16, gain_db=-3.5
        )

        assert "gain_db=-3.500" in wav.proxy_url
        assert "gain_db" not in flac.proxy_url

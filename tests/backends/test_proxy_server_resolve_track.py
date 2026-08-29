"""Unit tests for AudioProxyServer.resolve_track's decision tree: native
passthrough vs. local downsampling vs. a safe CD-tier fallback, driven by
device capabilities and what Qobuz actually has for a track — see
resolve_track's own docstring for the full rationale (always asking for
the ceiling tier first, so the decision is based on the track's true
native format rather than a capability-clamped guess).

Deliberately a fast unit test against a mocked QobuzStreamResolver rather
than a real HTTP round trip — test_proxy_server_transcode.py already
covers the end-to-end (real FLAC, real HTTP) case; this covers every
branch of the decision itself, cheaply.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.backends.dlna.capabilities import (
    DLNACapabilities,
    QOBUZ_QUALITY_MP3,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_192K,
)
from qobuz_proxy.backends.dlna.proxy_server import AudioProxyServer
from qobuz_proxy.playback.stream_resolver import ResolvedStream


def _stream(format_id: int, sample_rate: int, bit_depth: int, blob: str = "b") -> ResolvedStream:
    return ResolvedStream(
        url=f"https://cdn/{format_id}",
        blob=blob,
        format_id=format_id,
        sample_rate=sample_rate,
        bit_depth=bit_depth,
        fetched_at=0.0,
    )


def _caps(max_sample_rate: int, max_bit_depth: int, supports_flac: bool = True) -> DLNACapabilities:
    return DLNACapabilities(
        supports_flac=supports_flac, max_sample_rate=max_sample_rate, max_bit_depth=max_bit_depth
    )


def _make_proxy(resolve_side_effect) -> AudioProxyServer:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=resolve_side_effect)
    proxy = AudioProxyServer(resolver=resolver, host="127.0.0.1", port=0)
    return proxy


class TestCeilingTierSelection:
    """Which Qobuz tier gets requested first — see _ceiling_tier_for."""

    def test_mp3_only_device_requests_mp3(self) -> None:
        assert AudioProxyServer._ceiling_tier_for(_caps(44100, 16, supports_flac=False)) == (
            QOBUZ_QUALITY_MP3
        )

    def test_no_capabilities_requests_mp3(self) -> None:
        assert AudioProxyServer._ceiling_tier_for(None) == QOBUZ_QUALITY_MP3

    def test_cd_only_device_requests_cd(self) -> None:
        assert AudioProxyServer._ceiling_tier_for(_caps(48000, 16)) == QOBUZ_QUALITY_CD

    def test_24bit_device_always_requests_the_true_ceiling(self) -> None:
        """Even a device capped well below 192k still gets asked for the
        192k tier — so the actual native format can be discovered, not
        guessed from the device's own cap (see resolve_track's docstring)."""
        assert AudioProxyServer._ceiling_tier_for(_caps(48000, 24)) == QOBUZ_QUALITY_192K
        assert AudioProxyServer._ceiling_tier_for(_caps(192000, 24)) == QOBUZ_QUALITY_192K


class TestResolveTrackDecision:
    async def test_native_format_fits_the_device_passes_through_unmodified(self) -> None:
        """The actual point of always asking for the ceiling: a device
        capped at 48kHz gets a 24/48 master passed straight through — no
        transcoding — because Qobuz's own response reveals that's really
        all the track has, even though the ceiling tier was requested."""
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_192K, 48000, 24)
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=False)

        assert resolved is not None
        assert resolved.content_type == "audio/flac"
        assert resolved.sample_rate == 48000
        assert resolved.bit_depth == 24
        assert resolved.proxy_url.endswith(".flac")

    async def test_exceeding_native_format_transcodes_when_downsampling_enabled(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_192K, 192000, 24)
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=True)

        assert resolved is not None
        assert resolved.content_type == "audio/wav"
        assert resolved.sample_rate == 48000  # downsampled to the device's cap
        assert resolved.bit_depth == 24  # TranscodingFlacReader's fixed output depth
        assert resolved.proxy_url.endswith(".wav")

    async def test_exceeding_native_format_falls_back_to_cd_when_downsampling_disabled(
        self,
    ) -> None:
        calls = []

        async def _resolve(track_id, format_id, force=False):
            calls.append(format_id)
            if format_id == QOBUZ_QUALITY_192K:
                return _stream(QOBUZ_QUALITY_192K, 192000, 24)
            return _stream(QOBUZ_QUALITY_CD, 44100, 16)

        proxy = _make_proxy(_resolve)
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=False)

        assert resolved is not None
        assert resolved.content_type == "audio/flac"
        assert resolved.sample_rate == 44100
        assert resolved.bit_depth == 16
        assert calls == [QOBUZ_QUALITY_192K, QOBUZ_QUALITY_CD]

    async def test_cd_only_device_never_asks_for_hires_at_all(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_CD, 44100, 16)
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=16)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=True)

        assert resolved is not None
        assert resolved.content_type == "audio/flac"
        proxy._resolver.resolve.assert_awaited_once_with("42", QOBUZ_QUALITY_CD)

    async def test_mp3_only_device_gets_mp3_passthrough(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_MP3, 44100, 0)
        )
        caps = _caps(max_sample_rate=44100, max_bit_depth=16, supports_flac=False)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=True)

        assert resolved is not None
        assert resolved.content_type == "audio/mpeg"
        assert resolved.proxy_url.endswith(".mp3")

    async def test_returns_none_when_qobuz_has_nothing_for_the_track(self) -> None:
        proxy = _make_proxy(lambda track_id, format_id, force=False: None)

        resolved = await proxy.resolve_track("42", _caps(48000, 24), hires_downsampling=True)

        assert resolved is None

    async def test_forced_format_id_skips_ceiling_selection_but_still_downsamples_if_needed(
        self,
    ) -> None:
        """A manual/app-driven quality override picks the initial request,
        but the fits/transcode/fallback decision still applies on top —
        matches _resolve_content_type_and_transcode's old behavior of
        never bypassing the downsampling safety net."""
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_96K, 96000, 24)
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track(
            "42", caps, hires_downsampling=True, forced_format_id=QOBUZ_QUALITY_96K
        )

        assert resolved is not None
        assert resolved.content_type == "audio/wav"
        assert resolved.sample_rate == 48000
        proxy._resolver.resolve.assert_awaited_once_with("42", QOBUZ_QUALITY_96K)

    async def test_gapless_proxy_key_produces_a_distinct_url(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_CD, 44100, 16)
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=16)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=False, proxy_key="42_7")

        assert resolved is not None
        assert "42_7" in resolved.proxy_url

    async def test_blob_and_format_id_are_carried_through(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(
                QOBUZ_QUALITY_CD, 44100, 16, blob="the-blob"
            )
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=16)

        resolved = await proxy.resolve_track("42", caps, hires_downsampling=False)

        assert resolved is not None
        assert resolved.blob == "the-blob"
        assert resolved.format_id == QOBUZ_QUALITY_CD

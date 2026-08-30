"""Unit tests for SonosAudioProxyServer's on-the-fly-downsampling override
of _resolve_unfitting_stream — the Sonos-specific half of the decision
tree covered generically (native passthrough / CD-tier fallback) by
test_proxy_server_resolve_track.py.

Deliberately a fast unit test against a mocked QobuzStreamResolver rather
than a real HTTP round trip — test_sonos_proxy_server_transcode.py already
covers the end-to-end (real FLAC, real HTTP) case; this covers every
branch of the decision itself, cheaply.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.backends.dlna.capabilities import (
    DLNACapabilities,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_192K,
)
from qobuz_proxy.backends.dlna.sonos.proxy_server import SonosAudioProxyServer
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


def _make_proxy(resolve_side_effect, hires_downsampling: bool) -> SonosAudioProxyServer:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=resolve_side_effect)
    return SonosAudioProxyServer(
        resolver=resolver, hires_downsampling=hires_downsampling, host="127.0.0.1", port=0
    )


class TestDownsamplingDecision:
    async def test_exceeding_native_format_transcodes_when_downsampling_enabled(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_192K, 192000, 24),
            hires_downsampling=True,
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps)

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

        proxy = _make_proxy(_resolve, hires_downsampling=False)
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps)

        assert resolved is not None
        assert resolved.content_type == "audio/flac"
        assert resolved.sample_rate == 44100
        assert resolved.bit_depth == 16
        assert calls == [QOBUZ_QUALITY_192K, QOBUZ_QUALITY_CD]

    async def test_16bit_device_falls_back_to_cd_even_with_downsampling_enabled(self) -> None:
        """Downsampling only ever rescues a sample-rate mismatch on an
        otherwise 24-bit-capable device — a device that also fails on bit
        depth genuinely can't be served better than CD, same as any other
        DLNA device. _ceiling_tier_for's own gating means a 16-bit device
        never organically asks above CD in the first place, so this uses
        forced_format_id (an app-driven override) to reach the case where
        _resolve_unfitting_stream sees a stream that exceeds bit depth
        too, not just sample rate."""
        calls = []

        async def _resolve(track_id, format_id, force=False):
            calls.append(format_id)
            if format_id == QOBUZ_QUALITY_192K:
                return _stream(QOBUZ_QUALITY_192K, 192000, 24)
            return _stream(QOBUZ_QUALITY_CD, 44100, 16)

        proxy = _make_proxy(_resolve, hires_downsampling=True)
        caps = _caps(max_sample_rate=48000, max_bit_depth=16)  # 16-bit, not 24

        resolved = await proxy.resolve_track("42", caps, forced_format_id=QOBUZ_QUALITY_192K)

        assert resolved is not None
        assert resolved.content_type == "audio/flac"
        assert resolved.sample_rate == 44100
        assert calls == [QOBUZ_QUALITY_192K, QOBUZ_QUALITY_CD]

    async def test_forced_format_id_skips_ceiling_selection_but_still_downsamples_if_needed(
        self,
    ) -> None:
        """A manual/app-driven quality override picks the initial request,
        but the fits/transcode/fallback decision still applies on top —
        never bypasses the downsampling safety net."""
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_96K, 96000, 24),
            hires_downsampling=True,
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps, forced_format_id=QOBUZ_QUALITY_96K)

        assert resolved is not None
        assert resolved.content_type == "audio/wav"
        assert resolved.sample_rate == 48000
        proxy._resolver.resolve.assert_awaited_once_with("42", QOBUZ_QUALITY_96K)

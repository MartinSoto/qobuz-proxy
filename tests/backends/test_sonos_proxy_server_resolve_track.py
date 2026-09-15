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
    QOBUZ_QUALITY_MP3,
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


def _make_proxy(
    resolve_side_effect,
    hires_downsampling: bool,
    hires_downsampling_gain: bool = False,
    queue=None,
) -> SonosAudioProxyServer:
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(side_effect=resolve_side_effect)
    resolver.api_client.get_tracks_audio_info = AsyncMock(return_value={})
    return SonosAudioProxyServer(
        resolver=resolver,
        hires_downsampling=hires_downsampling,
        hires_downsampling_gain=hires_downsampling_gain,
        queue=queue,
        host="127.0.0.1",
        port=0,
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
        assert "format=wav" in resolved.proxy_url
        assert "rate=48000" in resolved.proxy_url

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


class TestHiresDownsamplingGain:
    """hires_downsampling_gain forces every track through the transcode
    pipeline (not just ones exceeding the device's cap) so playback gain
    applies consistently — see gain.py's QueueGainResolver."""

    async def test_a_track_that_already_fits_is_still_transcoded(self) -> None:
        """Without the gain flag this would be a plain passthrough (see
        the base TestDownsamplingDecision case) — with it, gain needs a
        decode pipeline even though nothing needs downsampling."""
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_CD, 44100, 16),
            hires_downsampling=True,
            hires_downsampling_gain=True,
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps)

        assert resolved is not None
        assert resolved.content_type == "audio/wav"
        # min(stream.sample_rate, capabilities.max_sample_rate): the track
        # already fit, so there's nothing to actually downsample — no
        # gratuitous upsampling to the device's ceiling.
        assert resolved.sample_rate == 44100
        assert "rate=44100" in resolved.proxy_url

    async def test_gain_db_is_embedded_in_the_url(self) -> None:
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_192K, 192000, 24),
            hires_downsampling=True,
            hires_downsampling_gain=True,
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps)

        assert resolved is not None
        # No queue/gain data available -> 0 dB, which isn't worth adding
        # to the URL at all (see _resolved_track).
        assert "gain_db=" not in resolved.proxy_url

    async def test_nonzero_gain_appears_in_the_url(self) -> None:
        from qobuz_proxy.auth.api_client import TrackAudioInfo

        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_192K, 192000, 24),
            hires_downsampling=True,
            hires_downsampling_gain=True,
        )
        proxy._gain_resolver._api.get_tracks_audio_info = AsyncMock(
            return_value={"42": TrackAudioInfo(track_id="42", gain_db=-9.35, peak=0.97)}
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps)

        assert resolved is not None
        assert "gain_db=-9.350" in resolved.proxy_url

    async def test_mp3_only_stream_still_passes_through_untouched(self) -> None:
        """No decode path exists for MP3 here at all (TranscodingFlacReader
        decodes FLAC only) — gain must never force it through anyway."""
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_MP3, 44100, 16),
            hires_downsampling=True,
            hires_downsampling_gain=True,
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24, supports_flac=False)

        resolved = await proxy.resolve_track("42", caps)

        assert resolved is not None
        assert resolved.content_type == "audio/mpeg"
        assert "format=wav" not in resolved.proxy_url

    async def test_gain_without_explicit_hires_downsampling_still_transcodes(self) -> None:
        """Defensive: even if a caller sets the gain flag without also
        setting hires_downsampling (config.py is expected to always do
        both together, but this class shouldn't silently degrade to a
        CD-tier fallback if that invariant is ever violated)."""
        proxy = _make_proxy(
            lambda track_id, format_id, force=False: _stream(QOBUZ_QUALITY_CD, 44100, 16),
            hires_downsampling=False,
            hires_downsampling_gain=True,
        )
        caps = _caps(max_sample_rate=48000, max_bit_depth=24)

        resolved = await proxy.resolve_track("42", caps)

        assert resolved is not None
        assert resolved.content_type == "audio/wav"

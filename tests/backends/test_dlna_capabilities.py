"""Tests for DLNA capability parsing — FLAC MIME detection in particular."""

from qobuz_proxy.backends.dlna.capabilities import (
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_MP3,
    DLNACapabilities,
    apply_device_overrides,
    parse_protocol_info_sink,
)


def _entry(content_format: str) -> str:
    return f"http-get:*:{content_format}:*"


class TestFlacMimeDetection:
    """Devices advertise FLAC under different MIME types."""

    def test_audio_flac_detected(self) -> None:
        caps = parse_protocol_info_sink(_entry("audio/flac"))
        assert caps.supports_flac is True
        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_audio_x_flac_detected(self) -> None:
        """gmediarender / GStreamer renderers advertise audio/x-flac."""
        caps = parse_protocol_info_sink(_entry("audio/x-flac"))
        assert caps.supports_flac is True
        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_x_flac_among_many_entries(self) -> None:
        """Mirrors the gmediarender Sink: lots of non-audio types plus x-flac."""
        sink = ",".join(
            [
                _entry("application/ogg"),
                _entry("application/x-3gp"),
                _entry("audio/x-flac"),
                _entry("audio/mpeg"),
            ]
        )
        caps = parse_protocol_info_sink(sink)
        assert caps.supports_flac is True
        assert caps.supports_mp3 is True
        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_no_flac_falls_back_to_mp3(self) -> None:
        caps = parse_protocol_info_sink(_entry("audio/mpeg"))
        assert caps.supports_flac is False
        assert caps.max_quality == QOBUZ_QUALITY_MP3


class TestByMimeFlacEquivalence:
    """A request for audio/flac should match a device that only has x-flac."""

    def test_by_mime_flac_matches_x_flac(self) -> None:
        caps = parse_protocol_info_sink(_entry("audio/x-flac"))
        matches = caps.by_mime("audio/flac")
        assert len(matches) == 1
        assert matches[0].mime == "audio/x-flac"

    def test_best_entry_for_flac_finds_x_flac(self) -> None:
        caps = parse_protocol_info_sink(_entry("audio/x-flac"))
        entry = caps.best_entry_for_media("audio/flac")
        assert entry is not None
        assert entry.mime == "audio/x-flac"


class TestMp3Aliases:
    def test_audio_mp3_alias_detected(self) -> None:
        caps = parse_protocol_info_sink(_entry("audio/mp3"))
        assert caps.supports_mp3 is True


class TestFormatInfoConfirmed:
    """Distinguish explicit rate/depth info from defaulted CD assumptions."""

    def test_bare_flac_entry_is_not_confirmed(self) -> None:
        """gmediarender-style Sink: FLAC with no params means 16/44 is a guess."""
        caps = parse_protocol_info_sink(_entry("audio/x-flac"))
        assert caps.format_info_confirmed is False

    def test_l16_rate_does_not_confirm_flac_capabilities(self) -> None:
        """An explicit L16 rate says nothing about the device's FLAC limits."""
        sink = ",".join(
            [
                "http-get:*:audio/L16;rate=44100;channels=2:*",
                _entry("audio/x-flac"),
            ]
        )
        caps = parse_protocol_info_sink(sink)
        assert caps.format_info_confirmed is False

    def test_dlna_profile_confirms(self) -> None:
        sink = "http-get:*:audio/flac:DLNA.ORG_PN=FLAC_192;DLNA.ORG_OP=01"
        caps = parse_protocol_info_sink(sink)
        assert caps.format_info_confirmed is True
        assert caps.max_quality == 27

    def test_explicit_tokens_confirm(self) -> None:
        sink = "http-get:*:audio/flac:sampleRate=96000;bitsPerSample=24"
        caps = parse_protocol_info_sink(sink)
        assert caps.format_info_confirmed is True
        assert caps.max_quality == 7


class TestSonosDeviceOverrides:
    """Every Sonos device is capped at 48kHz (a platform-wide ceiling, not a
    per-model quirk); bit depth is a blacklist of known 16-bit-only legacy
    models, defaulting new/unlisted models to 24-bit — see
    apply_device_overrides's docstring for why a blacklist and not a
    whitelist."""

    def _caps(self, advertised_sr: int = 192000, advertised_bd: int = 24) -> DLNACapabilities:
        return DLNACapabilities(
            supports_flac=True, max_sample_rate=advertised_sr, max_bit_depth=advertised_bd
        )

    def test_unlisted_sonos_model_defaults_to_24bit_48k(self) -> None:
        """A model not on the legacy blacklist — including any future Sonos
        product this code has never heard of — gets treated as 24-bit
        capable, sample-rate-capped at 48kHz."""
        caps = self._caps()
        apply_device_overrides(caps, "Sonos, Inc.", "One SL")

        assert caps.max_sample_rate == 48000
        assert caps.max_bit_depth == 24
        assert caps.max_quality == QOBUZ_QUALITY_96K

    def test_blacklisted_legacy_model_stays_16bit(self) -> None:
        caps = self._caps()
        apply_device_overrides(caps, "Sonos, Inc.", "Play:1")

        assert caps.max_sample_rate == 48000
        assert caps.max_bit_depth == 16
        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_blacklist_match_is_case_insensitive(self) -> None:
        caps = self._caps()
        apply_device_overrides(caps, "SONOS", "PLAY:3")

        assert caps.max_bit_depth == 16

    def test_override_wins_even_if_advertised_more(self) -> None:
        """A legacy model that (wrongly) advertises Hi-Res 192k must still
        end up capped — the override doesn't just fill in gaps."""
        caps = self._caps(advertised_sr=192000, advertised_bd=24)
        apply_device_overrides(caps, "Sonos, Inc.", "Play:1")

        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_non_sonos_device_is_untouched(self) -> None:
        caps = self._caps(advertised_sr=192000, advertised_bd=24)
        apply_device_overrides(caps, "Denon", "AVR-X1700H")

        assert caps.max_sample_rate == 192000
        assert caps.max_bit_depth == 24

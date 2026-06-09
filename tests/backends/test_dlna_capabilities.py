"""Tests for DLNA capability parsing — FLAC MIME detection in particular."""

from qobuz_proxy.backends.dlna.capabilities import (
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_MP3,
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

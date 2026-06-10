"""Tests for DLNABackend._build_didl metadata generation."""

from qobuz_proxy.backends.types import BackendTrackMetadata
from qobuz_proxy.backends.dlna.backend import DLNABackend


def _make_backend() -> DLNABackend:
    backend = DLNABackend.__new__(DLNABackend)
    backend._capabilities = None
    return backend


def _make_metadata(**kwargs) -> BackendTrackMetadata:  # type: ignore[no-untyped-def]
    defaults = {
        "track_id": "1",
        "title": "Test Track",
        "artist": "Test Artist",
        "album": "Test Album",
        "duration_ms": 180000,
        "artwork_url": "",
    }
    defaults.update(kwargs)
    return BackendTrackMetadata(**defaults)


class TestBuildDidl:
    def test_hires_96k_includes_correct_audio_attributes(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert 'sampleFrequency="96000"' in didl
        assert 'bitsPerSample="24"' in didl

    def test_hires_192k_includes_correct_audio_attributes(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=192000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert 'sampleFrequency="192000"' in didl
        assert 'bitsPerSample="24"' in didl

    def test_cd_quality_includes_correct_audio_attributes(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=44100, bit_depth=16)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert 'sampleFrequency="44100"' in didl
        assert 'bitsPerSample="16"' in didl

    def test_no_audio_attributes_when_format_unknown(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=0, bit_depth=0)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert "sampleFrequency" not in didl
        assert "bitsPerSample" not in didl

    def test_audio_attributes_are_on_res_element(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        # Attributes must appear inside the <res ...> tag, not elsewhere
        res_start = didl.index("<res ")
        res_end = didl.index(">", res_start)
        res_tag = didl[res_start:res_end]
        assert 'sampleFrequency="96000"' in res_tag
        assert 'bitsPerSample="24"' in res_tag

    def test_track_metadata_in_didl(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert "Test Track" in didl
        assert "Test Artist" in didl
        assert "Test Album" in didl

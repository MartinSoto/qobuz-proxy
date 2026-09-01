"""Tests for track display metadata retrieval and caching.

Streaming URL / quality / blob resolution no longer lives here — see
tests/playback/test_stream_resolver.py for that (QobuzStreamResolver).
"""

from unittest.mock import AsyncMock

import pytest

from qobuz_proxy.backends.types import BackendTrackMetadata
from qobuz_proxy.playback.metadata import (
    AudioQuality,
    MetadataCache,
    MetadataService,
    TrackMetadata,
)


class TestAudioQuality:
    """Tests for AudioQuality class."""

    def test_quality_constants(self) -> None:
        """Test quality format constants."""
        assert AudioQuality.MP3_320 == 5
        assert AudioQuality.FLAC_CD == 6
        assert AudioQuality.FLAC_HIRES_96 == 7
        assert AudioQuality.FLAC_HIRES_192 == 27

    def test_get_name_known_quality(self) -> None:
        """Test getting name for known quality IDs."""
        assert AudioQuality.get_name(5) == "MP3 320kbps"
        assert AudioQuality.get_name(6) == "FLAC CD (16-bit/44.1kHz)"
        assert AudioQuality.get_name(7) == "FLAC Hi-Res (24-bit/96kHz)"
        assert AudioQuality.get_name(27) == "FLAC Hi-Res (24-bit/192kHz)"

    def test_get_name_unknown_quality(self) -> None:
        """Test getting name for unknown quality ID."""
        assert AudioQuality.get_name(99) == "Unknown (99)"


class TestTrackMetadata:
    """Tests for TrackMetadata class."""

    def test_default_values(self) -> None:
        """Test default field values."""
        metadata = TrackMetadata()
        assert metadata.track_id == ""
        assert metadata.title == ""
        assert metadata.artist == ""
        assert metadata.album == ""
        assert metadata.duration_ms == 0
        assert metadata.artwork_url == ""

    def test_to_dict(self) -> None:
        """Test conversion to dictionary."""
        metadata = TrackMetadata(
            track_id="12345",
            title="Test Track",
            artist="Test Artist",
            album="Test Album",
            duration_ms=180000,
            artwork_url="https://example.com/art.jpg",
        )

        result = metadata.to_dict()

        assert result["track_id"] == "12345"
        assert result["title"] == "Test Track"
        assert result["artist"] == "Test Artist"
        assert result["album"] == "Test Album"
        assert result["duration_ms"] == 180000
        assert result["artwork_url"] == "https://example.com/art.jpg"

    def test_duration_s_property(self) -> None:
        """Test duration_s property conversion."""
        metadata = TrackMetadata(duration_ms=180000)
        assert metadata.duration_s == 180.0

        metadata = TrackMetadata(duration_ms=0)
        assert metadata.duration_s == 0.0


class TestMetadataCache:
    """Tests for MetadataCache class."""

    def test_get_empty_cache(self) -> None:
        """Test getting from empty cache returns None."""
        cache = MetadataCache()
        assert cache.get("12345") is None

    def test_set_and_get(self) -> None:
        """Test setting and getting cache entry."""
        cache = MetadataCache()
        metadata = TrackMetadata(track_id="12345", title="Test")

        cache.set("12345", metadata)

        result = cache.get("12345")
        assert result is not None
        assert result.track_id == "12345"
        assert result.title == "Test"

    def test_clear(self) -> None:
        """Test clearing cache."""
        cache = MetadataCache()
        cache.set("12345", TrackMetadata(track_id="12345"))
        cache.set("67890", TrackMetadata(track_id="67890"))

        cache.clear()

        assert cache.get("12345") is None
        assert cache.get("67890") is None

    def test_lru_eviction(self) -> None:
        """Test LRU eviction when cache is full."""
        cache = MetadataCache()
        cache._max_size = 3  # Small cache for testing

        # Add 3 entries
        cache.set("1", TrackMetadata(track_id="1"))
        cache.set("2", TrackMetadata(track_id="2"))
        cache.set("3", TrackMetadata(track_id="3"))

        # All should be present
        assert cache.get("1") is not None
        assert cache.get("2") is not None
        assert cache.get("3") is not None

        # Add 4th entry, should evict oldest ("1")
        cache.set("4", TrackMetadata(track_id="4"))

        assert cache.get("1") is None  # Evicted
        assert cache.get("2") is not None
        assert cache.get("3") is not None
        assert cache.get("4") is not None

    def test_update_existing_no_eviction(self) -> None:
        """Test updating existing entry doesn't trigger eviction."""
        cache = MetadataCache()
        cache._max_size = 2

        cache.set("1", TrackMetadata(track_id="1", title="Original"))
        cache.set("2", TrackMetadata(track_id="2"))

        # Update "1" shouldn't evict "2"
        cache.set("1", TrackMetadata(track_id="1", title="Updated"))

        assert cache.get("1") is not None
        assert cache.get("1").title == "Updated"
        assert cache.get("2") is not None


class MockAPIClient:
    """Mock API client for testing MetadataService."""

    def __init__(self) -> None:
        self.get_track_metadata = AsyncMock()


@pytest.fixture
def mock_api() -> MockAPIClient:
    """Create a mock API client."""
    return MockAPIClient()


@pytest.fixture
def metadata_service(mock_api: MockAPIClient) -> MetadataService:
    """Create a MetadataService with mock API client."""
    return MetadataService(mock_api)  # type: ignore[arg-type]


class TestMetadataService:
    """Tests for MetadataService class."""

    async def test_get_metadata_success(
        self, metadata_service: MetadataService, mock_api: MockAPIClient
    ) -> None:
        """Test successful metadata retrieval."""
        mock_api.get_track_metadata.return_value = {
            "title": "Test Track",
            "artist": "Test Artist",
            "album": "Test Album",
            "duration_ms": 180000,
            "album_art_url": "https://example.com/art.jpg",
        }

        result = await metadata_service.get_metadata("12345")

        assert result is not None
        assert result.track_id == "12345"
        assert result.title == "Test Track"
        assert result.artist == "Test Artist"
        assert result.album == "Test Album"
        assert result.duration_ms == 180000
        assert result.artwork_url == "https://example.com/art.jpg"

    async def test_get_metadata_not_found(
        self, metadata_service: MetadataService, mock_api: MockAPIClient
    ) -> None:
        """Test metadata retrieval for nonexistent track."""
        mock_api.get_track_metadata.return_value = None

        result = await metadata_service.get_metadata("99999999")

        assert result is None

    async def test_get_metadata_cache_hit(
        self, metadata_service: MetadataService, mock_api: MockAPIClient
    ) -> None:
        """Test metadata is cached and reused."""
        mock_api.get_track_metadata.return_value = {
            "title": "Test Track",
            "artist": "Test Artist",
            "album": "Test Album",
            "duration_ms": 180000,
            "album_art_url": "",
        }

        # First call
        result1 = await metadata_service.get_metadata("12345")
        # Second call
        result2 = await metadata_service.get_metadata("12345")

        assert result1 is not None
        assert result2 is not None
        assert result1 is result2  # Same object
        # API called only once
        assert mock_api.get_track_metadata.call_count == 1

    def test_log_now_playing_info(
        self, metadata_service: MetadataService, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test now playing log output using backend metadata + the
        actual_quality a backend resolved (see PlayResult.format_id)."""
        import logging

        metadata = BackendTrackMetadata(
            track_id="12345",
            title="Test Track",
            artist="Test Artist",
            album="Test Album",
        )

        with caplog.at_level(logging.INFO):
            metadata_service.log_now_playing_info(metadata, actual_quality=27)

        assert "Now playing: Test Artist - Test Track" in caplog.text
        assert "[Test Album]" in caplog.text
        assert "FLAC Hi-Res (24-bit/192kHz)" in caplog.text

    def test_log_now_playing_info_without_quality(
        self, metadata_service: MetadataService, caplog: pytest.LogCaptureFixture
    ) -> None:
        """actual_quality=None (unavailable) must still log something sane,
        not raise."""
        import logging

        metadata = BackendTrackMetadata(track_id="12345", title="T", artist="A", album="Al")

        with caplog.at_level(logging.INFO):
            metadata_service.log_now_playing_info(metadata, actual_quality=None)

        assert "Now playing: A - T" in caplog.text

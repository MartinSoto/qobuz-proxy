"""The getFileUrl `blob` must survive into the metadata cache.

reportStreamingEnd needs the opaque `blob` Qobuz returns from track/getFileUrl;
if we drop it on the floor, play-reporting (and Last.fm scrobbling) can't work.
"""

from unittest.mock import AsyncMock

from qobuz_proxy.playback.metadata import MetadataService


def _service_with_url_result(result: dict) -> MetadataService:
    api = AsyncMock()
    api.get_track_metadata = AsyncMock(
        return_value={
            "title": "T",
            "artist": "A",
            "album": "Al",
            "duration_ms": 1000,
            "album_art_url": "",
        }
    )
    api.get_track_url = AsyncMock(return_value=result)
    return MetadataService(api_client=api, max_quality=27)


class TestBlobCapture:
    async def test_blob_stored_and_readable(self) -> None:
        service = _service_with_url_result(
            {
                "url": "https://cdn/stream.flac",
                "format_id": 27,
                "bit_depth": 24,
                "sampling_rate": 96.0,
                "blob": "opaque-blob-xyz",
            }
        )

        meta = await service.get_metadata("555", fetch_url=True)

        assert meta is not None
        assert meta.blob == "opaque-blob-xyz"
        assert service.get_track_blob("555") == "opaque-blob-xyz"

    async def test_missing_blob_defaults_empty(self) -> None:
        service = _service_with_url_result(
            {"url": "https://cdn/s.flac", "format_id": 6, "bit_depth": 16, "sampling_rate": 44.1}
        )

        meta = await service.get_metadata("777", fetch_url=True)

        assert meta is not None
        assert meta.blob == ""
        assert service.get_track_blob("777") == ""

    async def test_get_track_blob_uncached_returns_none(self) -> None:
        service = _service_with_url_result({"url": "x"})
        assert service.get_track_blob("nope") is None

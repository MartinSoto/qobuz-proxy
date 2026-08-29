"""Tests for QobuzStreamResolver — the single place that calls
track/getFileUrl and caches the result per (track_id, format_id).

Covers what used to be split across MetadataService (URL TTL/expiry,
quality fallback) and TrackMetadata.blob (getFileUrl's opaque token,
needed for reportStreamingEnd) before both moved here.
"""

import time
from unittest.mock import AsyncMock

from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver, ResolvedStream


class MockAPIClient:
    def __init__(self) -> None:
        self.get_track_url = AsyncMock()


def _result(**overrides) -> dict:
    base = {
        "url": "https://streaming.example.com/track.flac",
        "format_id": 27,
        "bit_depth": 24,
        "sampling_rate": 96.0,  # kHz, as Qobuz's API returns it
        "blob": "opaque-blob-xyz",
    }
    base.update(overrides)
    return base


class TestResolve:
    async def test_returns_stream_with_fields_converted(self) -> None:
        api = MockAPIClient()
        api.get_track_url.return_value = _result()
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        stream = await resolver.resolve("12345", 27)

        assert stream is not None
        assert stream.url == "https://streaming.example.com/track.flac"
        assert stream.format_id == 27
        assert stream.bit_depth == 24
        assert stream.sample_rate == 96000  # kHz -> Hz
        assert stream.blob == "opaque-blob-xyz"

    async def test_missing_blob_defaults_empty(self) -> None:
        api = MockAPIClient()
        api.get_track_url.return_value = _result(blob=None)
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        stream = await resolver.resolve("12345", 6)

        assert stream is not None
        assert stream.blob == ""

    async def test_actual_format_id_can_differ_from_requested(self) -> None:
        """The mechanism the whole "ask for the ceiling, see what's really
        there" design hinges on: Qobuz's response format_id/sample_rate
        reflect what it actually served, not an echo of the request."""
        api = MockAPIClient()
        api.get_track_url.return_value = _result(format_id=7, sampling_rate=96.0)
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        stream = await resolver.resolve("12345", 27)  # asked for 192k

        assert stream.format_id == 7  # got 96k back — that's the track's real ceiling
        assert stream.sample_rate == 96000

    async def test_returns_none_when_api_has_nothing(self) -> None:
        api = MockAPIClient()
        api.get_track_url.return_value = None
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        assert await resolver.resolve("12345", 27) is None

    async def test_api_called_once_per_track_id_and_format_id(self) -> None:
        """A cache hit within TTL must not re-hit the Qobuz API."""
        api = MockAPIClient()
        api.get_track_url.return_value = _result()
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        first = await resolver.resolve("12345", 27)
        second = await resolver.resolve("12345", 27)

        assert first is second
        assert api.get_track_url.call_count == 1

    async def test_different_format_id_is_a_separate_cache_entry(self) -> None:
        """Resolving the same track at two tiers (e.g. hi-res ceiling, then
        a CD-tier fallback) must not clobber or reuse each other's cache
        entry — both need to be independently fresh."""
        api = MockAPIClient()
        api.get_track_url.side_effect = [
            _result(format_id=27, sampling_rate=192.0),
            _result(format_id=6, sampling_rate=44.1, bit_depth=16),
        ]
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        hires = await resolver.resolve("12345", 27)
        cd = await resolver.resolve("12345", 6)

        assert hires.sample_rate == 192000
        assert cd.sample_rate == 44100
        assert api.get_track_url.call_count == 2

    async def test_stale_cache_entry_is_refetched(self) -> None:
        api = MockAPIClient()
        api.get_track_url.side_effect = [
            _result(url="https://cdn/first.flac"),
            _result(url="https://cdn/second.flac"),
        ]
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        first = await resolver.resolve("12345", 27)
        assert first.url == "https://cdn/first.flac"

        # Simulate the entry aging past the TTL.
        resolver._cache[("12345", 27)].fetched_at = time.time() - 10_000

        second = await resolver.resolve("12345", 27)
        assert second.url == "https://cdn/second.flac"
        assert api.get_track_url.call_count == 2

    async def test_force_always_refetches_even_when_fresh(self) -> None:
        api = MockAPIClient()
        api.get_track_url.side_effect = [
            _result(url="https://cdn/first.flac"),
            _result(url="https://cdn/second.flac"),
        ]
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        await resolver.resolve("12345", 27)
        second = await resolver.resolve("12345", 27, force=True)

        assert second.url == "https://cdn/second.flac"
        assert api.get_track_url.call_count == 2


class TestInvalidate:
    async def test_invalidate_drops_every_tier_for_a_track(self) -> None:
        api = MockAPIClient()
        api.get_track_url.side_effect = [
            _result(format_id=27, sampling_rate=192.0),
            _result(format_id=6, sampling_rate=44.1, bit_depth=16),
            _result(format_id=27, sampling_rate=192.0, url="https://cdn/refetched.flac"),
        ]
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        await resolver.resolve("12345", 27)
        await resolver.resolve("12345", 6)
        resolver.invalidate("12345")
        refetched = await resolver.resolve("12345", 27)

        assert refetched.url == "https://cdn/refetched.flac"
        assert api.get_track_url.call_count == 3

    async def test_invalidate_leaves_other_tracks_alone(self) -> None:
        api = MockAPIClient()
        api.get_track_url.return_value = _result()
        resolver = QobuzStreamResolver(api)  # type: ignore[arg-type]

        await resolver.resolve("12345", 27)
        await resolver.resolve("67890", 27)
        resolver.invalidate("12345")

        # "67890" is still cached — resolving it again shouldn't re-hit the API.
        api.get_track_url.reset_mock()
        await resolver.resolve("67890", 27)
        assert api.get_track_url.call_count == 0


class TestResolvedStreamExpiry:
    def test_is_expired_false_within_ttl(self) -> None:
        stream = ResolvedStream(
            url="x",
            blob="",
            format_id=27,
            sample_rate=192000,
            bit_depth=24,
            fetched_at=time.time(),
        )
        assert stream.is_expired() is False

    def test_is_expired_true_past_ttl(self) -> None:
        stream = ResolvedStream(
            url="x",
            blob="",
            format_id=27,
            sample_rate=192000,
            bit_depth=24,
            fetched_at=time.time() - 10_000,
        )
        assert stream.is_expired() is True

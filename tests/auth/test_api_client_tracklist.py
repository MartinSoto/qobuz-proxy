"""track/getList batching: at most 50 ids per request (measured: 51 is
rejected with HTTP 400), and batch display metadata for queue publishing."""

import json
from typing import Any
from unittest.mock import AsyncMock

from qobuz_proxy.auth.api_client import TRACK_GETLIST_MAX_IDS, QobuzAPIClient


def _client_returning_every_requested_id() -> QobuzAPIClient:
    """A client whose _request_signed answers like Qobuz: an item per id sent."""
    client = QobuzAPIClient("app123", "secret456")

    async def fake(*args: Any, **kwargs: Any) -> dict[str, Any]:
        ids = json.loads(kwargs["body"])["tracks_id"]
        return {"tracks": {"items": [{"id": i, "duration": 100} for i in ids]}}

    client._request_signed = AsyncMock(side_effect=fake)  # type: ignore[method-assign]
    return client


def _sent_chunk_sizes(client: QobuzAPIClient) -> list[int]:
    return [
        len(json.loads(call.kwargs["body"])["tracks_id"])
        for call in client._request_signed.await_args_list  # type: ignore[attr-defined]
    ]


class TestChunking:
    async def test_the_limit_is_fifty(self) -> None:
        assert TRACK_GETLIST_MAX_IDS == 50

    async def test_a_queue_longer_than_the_limit_is_fetched_in_chunks(self) -> None:
        client = _client_returning_every_requested_id()
        ids = [str(1000 + i) for i in range(120)]

        result = await client.get_tracks_audio_info(ids)

        assert _sent_chunk_sizes(client) == [50, 50, 20]
        assert set(result) == set(ids)

    async def test_exactly_the_limit_is_one_request(self) -> None:
        client = _client_returning_every_requested_id()

        await client.get_tracks_audio_info([str(i) for i in range(50)])

        assert _sent_chunk_sizes(client) == [50]

    async def test_a_failing_chunk_costs_only_that_chunk(self) -> None:
        client = QobuzAPIClient("app123", "secret456")
        calls = 0

        async def fake(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("HTTP 500")
            ids = json.loads(kwargs["body"])["tracks_id"]
            return {"tracks": {"items": [{"id": i, "duration": 100} for i in ids]}}

        client._request_signed = AsyncMock(side_effect=fake)  # type: ignore[method-assign]
        ids = [str(i) for i in range(1, 121)]

        result = await client.get_tracks_audio_info(ids)

        assert set(result) == set(ids[:50]) | set(ids[100:])


class TestGetTracksMetadata:
    async def test_empty_input_makes_no_request(self) -> None:
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock()  # type: ignore[method-assign]

        assert await client.get_tracks_metadata([]) == {}
        client._request_signed.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_flattens_display_data_and_the_best_format(self) -> None:
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "tracks": {
                    "items": [
                        {
                            "id": 449427998,
                            "title": "What A Difference A Day Made",
                            "duration": 285,
                            "maximum_sampling_rate": 96,
                            "maximum_bit_depth": 24,
                            "performer": {"name": "Ledisi"},
                            "album": {
                                "title": "For Dinah",
                                "image": {"small": "s.jpg", "large": "l.jpg"},
                            },
                        },
                        {
                            "id": 436980185,
                            "title": "Maius",
                            "duration": 341,
                            "maximum_sampling_rate": 44.1,
                            "maximum_bit_depth": 16,
                        },
                    ]
                }
            }
        )

        result = await client.get_tracks_metadata(["449427998", "436980185"])

        hires = result["449427998"]
        assert hires["title"] == "What A Difference A Day Made"
        assert hires["artist"] == "Ledisi"
        assert hires["album"] == "For Dinah"
        assert hires["album_art_url"] == "l.jpg"
        assert hires["duration_ms"] == 285_000
        assert hires["max_sample_rate_hz"] == 96000
        assert hires["max_bit_depth"] == 24
        cd = result["436980185"]
        assert cd["max_sample_rate_hz"] == 44100
        assert cd["max_bit_depth"] == 16
        assert cd["artist"] == "" and cd["album"] == ""

    async def test_fractional_rates_round_to_whole_hertz(self) -> None:
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "tracks": {
                    "items": [{"id": 1, "maximum_sampling_rate": 88.2, "maximum_bit_depth": 24}]
                }
            }
        )

        result = await client.get_tracks_metadata(["1"])

        assert result["1"]["max_sample_rate_hz"] == 88200

    async def test_a_track_without_format_data_reads_as_unknown(self) -> None:
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(  # type: ignore[method-assign]
            return_value={"tracks": {"items": [{"id": 1, "title": "X"}]}}
        )

        result = await client.get_tracks_metadata(["1"])

        assert result["1"]["max_sample_rate_hz"] == 0
        assert result["1"]["max_bit_depth"] == 0

    async def test_get_track_metadata_carries_the_format_too(self) -> None:
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "title": "T",
                "duration": 10,
                "maximum_sampling_rate": 192,
                "maximum_bit_depth": 24,
            }
        )

        meta = await client.get_track_metadata("1")

        assert meta is not None
        assert meta["max_sample_rate_hz"] == 192000
        assert meta["max_bit_depth"] == 24


class TestTracksWithoutLoudnessData:
    """Qobuz sends audio_info with null gain and peak for some tracks (seen on
    real ones): that must not cost the rest of the chunk its gain."""

    def _client(self, items: list[dict[str, Any]]) -> QobuzAPIClient:
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(  # type: ignore[method-assign]
            return_value={"tracks": {"items": items}}
        )
        return client

    async def test_a_null_gain_leaves_only_that_track_without_data(self) -> None:
        client = self._client(
            [
                {
                    "id": 1,
                    "duration": 100,
                    "audio_info": {"replaygain_track_gain": None, "replaygain_track_peak": None},
                },
                {
                    "id": 2,
                    "duration": 200,
                    "audio_info": {"replaygain_track_gain": -2.76, "replaygain_track_peak": 0.98},
                },
            ]
        )

        result = await client.get_tracks_audio_info(["1", "2"])

        assert set(result) == {"2"}  # no data for 1: the caller applies no adjustment
        assert result["2"].gain_db == -2.76 and result["2"].peak == 0.98

    async def test_a_null_peak_alone_is_no_peak_limit(self) -> None:
        client = self._client(
            [
                {
                    "id": 1,
                    "duration": 100,
                    "audio_info": {"replaygain_track_gain": 1.5, "replaygain_track_peak": None},
                }
            ]
        )

        result = await client.get_tracks_audio_info(["1"])

        assert result["1"].gain_db == 1.5 and result["1"].peak == 1.0

"""Tests for QueueGainResolver (backends/dlna/sonos/gain.py) — the
approximated album-gain grouping for Sonos's hires_downsampling_gain.
"""

from unittest.mock import AsyncMock

import pytest

from qobuz_proxy.auth.api_client import TrackAudioInfo
from qobuz_proxy.backends.dlna.sonos.gain import QueueGainResolver
from qobuz_proxy.playback.queue import QobuzQueue


def _info(
    track_id: str, gain_db: float, peak: float = 0.9, album_id: str = "", duration_ms: int = 200_000
):
    return TrackAudioInfo(
        track_id=track_id, gain_db=gain_db, peak=peak, album_id=album_id, duration_ms=duration_ms
    )


def _api_client(infos: dict[str, TrackAudioInfo]):
    api = AsyncMock()
    api.get_tracks_audio_info = AsyncMock(
        side_effect=lambda ids: {t: infos[t] for t in ids if t in infos}
    )
    return api


class TestPlainTrackGain:
    async def test_no_queue_uses_own_gain(self) -> None:
        api = _api_client({"A": _info("A", gain_db=-8.0)})
        resolver = QueueGainResolver(api_client=api, queue=None)

        assert await resolver.resolve("A") == pytest.approx(-8.0)

    async def test_untrustworthy_queue_falls_back_to_track_gain(self) -> None:
        queue = QobuzQueue()  # never fed a full snapshot — current_track_ids() is None
        api = _api_client({"A": _info("A", gain_db=-8.0)})
        resolver = QueueGainResolver(api_client=api, queue=queue)

        assert await resolver.resolve("A") == pytest.approx(-8.0)

    async def test_single_track_queue_uses_track_gain(self) -> None:
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "A")])
        api = _api_client({"A": _info("A", gain_db=-8.0, album_id="ALB")})
        resolver = QueueGainResolver(api_client=api, queue=queue)

        assert await resolver.resolve("A") == pytest.approx(-8.0)

    async def test_no_gain_data_at_all_returns_zero(self) -> None:
        api = _api_client({})
        resolver = QueueGainResolver(api_client=api, queue=None)

        assert await resolver.resolve("missing") == 0.0

    async def test_api_failure_degrades_to_zero_rather_than_raising(self) -> None:
        api = AsyncMock()
        api.get_tracks_audio_info = AsyncMock(side_effect=RuntimeError("network error"))
        resolver = QueueGainResolver(api_client=api, queue=None)

        assert await resolver.resolve("A") == 0.0


class TestApproximatedAlbumGain:
    async def test_same_album_uses_duration_weighted_average(self) -> None:
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])
        # A: -10 dB for 100s, B: -6 dB for 300s -> weighted average -7 dB
        api = _api_client(
            {
                "A": _info("A", gain_db=-10.0, album_id="ALB", duration_ms=100_000),
                "B": _info("B", gain_db=-6.0, album_id="ALB", duration_ms=300_000),
            }
        )
        resolver = QueueGainResolver(api_client=api, queue=queue)

        result = await resolver.resolve("A")

        assert result == pytest.approx(-7.0)

    async def test_not_necessarily_the_complete_album_or_in_order(self) -> None:
        """A queue holding a subset of one album, in any order, still
        groups — confirmed live it doesn't need to be the whole album."""
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "C"), (2, "A")])  # only 2 of a 12-track album
        api = _api_client(
            {
                "A": _info("A", gain_db=-10.0, album_id="ALB", duration_ms=100_000),
                "C": _info("C", gain_db=-10.0, album_id="ALB", duration_ms=100_000),
            }
        )
        resolver = QueueGainResolver(api_client=api, queue=queue)

        assert await resolver.resolve("A") == pytest.approx(-10.0)

    async def test_different_albums_falls_back_to_track_gain(self) -> None:
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])
        api = _api_client(
            {
                "A": _info("A", gain_db=-10.0, album_id="ALB1"),
                "B": _info("B", gain_db=-2.0, album_id="ALB2"),
            }
        )
        resolver = QueueGainResolver(api_client=api, queue=queue)

        assert await resolver.resolve("A") == pytest.approx(-10.0)

    async def test_missing_gain_data_for_one_queued_track_falls_back(self) -> None:
        """Confirmed conservative: don't guess at album membership from a
        partial picture just because most of the queue resolved."""
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])
        api = _api_client({"A": _info("A", gain_db=-10.0, album_id="ALB")})  # B never resolves

        resolver = QueueGainResolver(api_client=api, queue=queue)

        assert await resolver.resolve("A") == pytest.approx(-10.0)

    async def test_no_album_id_at_all_falls_back_to_track_gain(self) -> None:
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])
        api = _api_client(
            {
                "A": _info("A", gain_db=-10.0, album_id=""),
                "B": _info("B", gain_db=-2.0, album_id=""),
            }
        )
        resolver = QueueGainResolver(api_client=api, queue=queue)

        assert await resolver.resolve("A") == pytest.approx(-10.0)


class TestPeakClamp:
    async def test_group_gain_is_clamped_to_this_tracks_own_peak(self) -> None:
        """A short, hot outlier must never clip just because a shared
        album gain was computed from other tracks' quieter mastering."""
        queue = QobuzQueue()
        await queue.replace_queue_tracks([(1, "A"), (2, "B")])
        # Weighted average would be +2 dB, but A's own peak only allows
        # ~0 dB of boost (peak 1.0 -> ceiling 0 dB).
        api = _api_client(
            {
                "A": _info("A", gain_db=2.0, peak=1.0, album_id="ALB", duration_ms=100_000),
                "B": _info("B", gain_db=2.0, peak=0.5, album_id="ALB", duration_ms=100_000),
            }
        )
        resolver = QueueGainResolver(api_client=api, queue=queue)

        result = await resolver.resolve("A")

        assert result == pytest.approx(0.0, abs=1e-6)

    async def test_zero_peak_is_not_clamped(self) -> None:
        api = _api_client({"A": _info("A", gain_db=-5.0, peak=0.0)})
        resolver = QueueGainResolver(api_client=api, queue=None)

        assert await resolver.resolve("A") == pytest.approx(-5.0)

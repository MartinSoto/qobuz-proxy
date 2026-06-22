"""The player must drive PlayReporter across its playback lifecycle.

Wires a real PlayReporter (over a mocked API client) into the player and checks
that play/pause/stop/track-end/track-switch produce the right report calls.
"""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import BackendTrackMetadata, PlaybackState
from qobuz_proxy.backends.base import AudioBackend
from qobuz_proxy.playback.play_reporter import PlayReporter
from qobuz_proxy.playback.player import QobuzPlayer


class _Backend(AudioBackend):
    def __init__(self) -> None:
        super().__init__(name="test")

    async def play(self, url: str, metadata: BackendTrackMetadata) -> None: ...
    async def pause(self) -> None: ...
    async def resume(self) -> None: ...
    async def stop(self) -> None: ...
    async def seek(self, position_ms: int) -> None: ...
    async def get_position(self) -> int:
        return 0

    async def set_volume(self, level: int) -> None: ...
    async def get_volume(self) -> int:
        return 0

    async def get_state(self) -> PlaybackState:
        return self._state

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None: ...


async def _coro(value):  # type: ignore[no-untyped-def]
    return value


def _make_player_with_reporter():
    backend = _Backend()

    metadata = MagicMock()
    metadata.get_streaming_url = MagicMock(side_effect=lambda tid: _coro(f"http://t/{tid}"))
    metadata.get_metadata = MagicMock(side_effect=lambda tid: _coro(None))
    metadata.get_track_actual_quality = MagicMock(return_value=27)
    metadata.get_track_blob = MagicMock(return_value="theblob")
    metadata.get_track_format = MagicMock(return_value=(27, 96000, 24))
    metadata.log_now_playing_info = MagicMock()

    api = AsyncMock()
    api.report_streaming_start = AsyncMock(return_value=True)
    api.report_streaming_end = AsyncMock(return_value=True)
    reporter = PlayReporter(api)

    queue = MagicMock()
    player = QobuzPlayer(
        queue=queue, metadata_service=metadata, backend=backend, play_reporter=reporter
    )
    return player, api


class TestPlayerReporting:
    async def test_play_track_reports_start(self) -> None:
        player, api = _make_player_with_reporter()

        await player.play_track(queue_item_id=1, track_id="555")

        api.report_streaming_start.assert_awaited_once_with(track_id="555", format_id=27)

    async def test_stop_reports_end(self) -> None:
        player, api = _make_player_with_reporter()
        await player.play_track(queue_item_id=1, track_id="555")

        await player.stop_playback()

        api.report_streaming_end.assert_awaited_once()
        assert api.report_streaming_end.await_args.kwargs["track_id"] == "555"
        assert api.report_streaming_end.await_args.kwargs["blob"] == "theblob"

    async def test_pause_reports_end(self) -> None:
        player, api = _make_player_with_reporter()
        await player.play_track(queue_item_id=1, track_id="555")

        await player.pause()

        api.report_streaming_end.assert_awaited_once()

    async def test_switching_track_reports_end_then_start(self) -> None:
        player, api = _make_player_with_reporter()
        await player.play_track(queue_item_id=1, track_id="100")
        await player.play_track(queue_item_id=2, track_id="200")

        # First track ended, second track started.
        assert api.report_streaming_end.await_args.kwargs["track_id"] == "100"
        assert api.report_streaming_start.await_count == 2
        assert api.report_streaming_start.await_args.kwargs["track_id"] == "200"

    async def test_adopting_track_midplay_does_not_report_start(self) -> None:
        """Handoff from the app: it already reported the play (we start at a
        non-zero position), so we must not report a duplicate start."""
        player, api = _make_player_with_reporter()

        # App played this to ~26s, then transfers to us.
        await player.apply_remote_state(
            track_id="900", queue_item_id=1, position_ms=26000, playing_state=2
        )

        api.report_streaming_start.assert_not_awaited()

    async def test_fresh_play_from_zero_reports_start(self) -> None:
        """A track we start ourselves (position ~0) is ours to report."""
        player, api = _make_player_with_reporter()

        await player.apply_remote_state(
            track_id="900", queue_item_id=1, position_ms=0, playing_state=2
        )

        api.report_streaming_start.assert_awaited_once_with(track_id="900", format_id=27)

    async def test_no_reporter_is_safe(self) -> None:
        """A player built without a reporter must not crash on playback."""
        backend = _Backend()
        metadata = MagicMock()
        metadata.get_streaming_url = MagicMock(side_effect=lambda tid: _coro(f"http://t/{tid}"))
        metadata.get_metadata = MagicMock(side_effect=lambda tid: _coro(None))
        metadata.get_track_format = MagicMock(return_value=(0, 0, 0))
        metadata.get_track_actual_quality = MagicMock(return_value=None)
        metadata.log_now_playing_info = MagicMock()
        player = QobuzPlayer(queue=MagicMock(), metadata_service=metadata, backend=backend)

        await player.play_track(queue_item_id=1, track_id="555")
        await player.stop_playback()

        assert player.state == PlaybackState.STOPPED

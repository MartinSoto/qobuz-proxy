"""Tests for StateReporter._build_state_report's defensive guards."""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import BufferStatus, PlaybackState
from qobuz_proxy.playback.state_reporter import StateReporter


def _make_reporter(queue_item_id: int) -> StateReporter:
    current_track = MagicMock()
    current_track.queue_item_id = queue_item_id

    player = MagicMock()
    player.current_track = current_track
    player.state = PlaybackState.PLAYING
    player.duration_ms = 180_000
    player._position_timestamp_ms = 0
    player._position_value_ms = 0
    player.backend.get_buffer_status = AsyncMock(return_value=BufferStatus.OK)

    queue_state = MagicMock()
    queue_state.version.major = 1
    queue_state.version.minor = 0

    queue = MagicMock()
    queue.get_state = AsyncMock(return_value=queue_state)

    return StateReporter(player=player, queue=queue, send_callback=AsyncMock())


class TestQueueItemIdGuard:
    async def test_normal_value_passes_through(self) -> None:
        reporter = _make_reporter(queue_item_id=42)

        report = await reporter._build_state_report()

        assert report.current_queue_item_id == 42

    async def test_out_of_range_value_reports_zero_instead_of_crashing(self) -> None:
        # Regression: an unrecognized upstream sentinel (e.g. the server's
        # all-bits-set "no track" QueueTrackRef leaking through as a huge
        # queue_item_id) must degrade to "no item" here rather than crash
        # the report loop with a protobuf int32 range error every cycle.
        reporter = _make_reporter(queue_item_id=0xFFFFFFFFFFFFFFFF)

        report = await reporter._build_state_report()

        assert report.current_queue_item_id == 0

    async def test_negative_value_reports_zero(self) -> None:
        reporter = _make_reporter(queue_item_id=-1)

        report = await reporter._build_state_report()

        assert report.current_queue_item_id == 0

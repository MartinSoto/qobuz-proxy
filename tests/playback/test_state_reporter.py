"""Tests for StateReporter: report building, and the send/heartbeat-watchdog
relay (see docs' "make row 3 event-driven" design work — report_now() is
the single send path every command-driven and backend-event-driven caller
funnels through; the heartbeat is a watchdog on top of that, not the
primary mechanism)."""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import BufferStatus, PlaybackState
from qobuz_proxy.playback.state_reporter import StateReporter


def _make_reporter(
    queue_item_id: int = 1, state: PlaybackState = PlaybackState.PLAYING
) -> tuple[StateReporter, AsyncMock]:
    current_track = MagicMock()
    current_track.queue_item_id = queue_item_id

    player = MagicMock()
    player.current_track = current_track
    player.state = state
    player.duration_ms = 180_000
    player._position_timestamp_ms = 0
    player._position_value_ms = 0
    player.current_position_ms = 0
    player.backend.get_buffer_status = AsyncMock(return_value=BufferStatus.OK)

    queue_state = MagicMock()
    queue_state.version.major = 1
    queue_state.version.minor = 0

    queue = MagicMock()
    queue.get_state = AsyncMock(return_value=queue_state)

    send_callback = AsyncMock()
    reporter = StateReporter(player=player, queue=queue, send_callback=send_callback)
    return reporter, send_callback


class TestQueueItemIdGuard:
    async def test_normal_value_passes_through(self) -> None:
        reporter, _ = _make_reporter(queue_item_id=42)

        report = await reporter._build_state_report()

        assert report.current_queue_item_id == 42

    async def test_out_of_range_value_reports_zero_instead_of_crashing(self) -> None:
        # Regression: an unrecognized upstream sentinel (e.g. the server's
        # all-bits-set "no track" QueueTrackRef leaking through as a huge
        # queue_item_id) must degrade to "no item" here rather than crash
        # the report loop with a protobuf int32 range error every cycle.
        reporter, _ = _make_reporter(queue_item_id=0xFFFFFFFFFFFFFFFF)

        report = await reporter._build_state_report()

        assert report.current_queue_item_id == 0

    async def test_negative_value_reports_zero(self) -> None:
        reporter, _ = _make_reporter(queue_item_id=-1)

        report = await reporter._build_state_report()

        assert report.current_queue_item_id == 0


class TestHeartbeatWatchdog:
    """_heartbeat_loop is a watchdog on top of report_now(), not the
    primary relay mechanism — see the module docstring."""

    async def test_no_event_for_the_timeout_fires_a_heartbeat_while_playing(
        self,
    ) -> None:
        import asyncio
        from unittest.mock import patch

        reporter, send_callback = _make_reporter(state=PlaybackState.PLAYING)

        with patch("qobuz_proxy.playback.state_reporter.STATE_HEARTBEAT_TIMEOUT_SECONDS", 0.02):
            await reporter.start()
            await asyncio.sleep(0.08)
            await reporter.stop()

        assert send_callback.await_count >= 1

    async def test_heartbeat_suppressed_when_not_playing(self) -> None:
        import asyncio
        from unittest.mock import patch

        reporter, send_callback = _make_reporter(state=PlaybackState.PAUSED)

        with patch("qobuz_proxy.playback.state_reporter.STATE_HEARTBEAT_TIMEOUT_SECONDS", 0.02):
            await reporter.start()
            await asyncio.sleep(0.08)
            await reporter.stop()

        send_callback.assert_not_called()

    async def test_event_triggered_sends_reset_the_watchdog_deadline(self) -> None:
        """A steady stream of event-triggered sends (report_now(), faster
        than the timeout) must keep the watchdog quiet — it must never
        also fire a redundant send on top of them."""
        import asyncio
        from unittest.mock import patch

        reporter, send_callback = _make_reporter(state=PlaybackState.PLAYING)

        with patch("qobuz_proxy.playback.state_reporter.STATE_HEARTBEAT_TIMEOUT_SECONDS", 0.05):
            await reporter.start()
            for _ in range(6):
                await asyncio.sleep(0.02)  # faster than the watchdog timeout
                await reporter.report_now()
            await reporter.stop()

        # Exactly the 6 explicit calls — the watchdog piled nothing on top.
        assert send_callback.await_count == 6

    async def test_failed_send_does_not_reset_the_watchdog_clock(self) -> None:
        reporter, send_callback = _make_reporter()
        send_callback.side_effect = RuntimeError("network down")

        before = reporter._last_report_at
        await reporter.report_now()

        assert reporter._last_report_at == before

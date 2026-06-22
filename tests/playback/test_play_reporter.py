"""Tests for PlayReporter — the play start/end lifecycle on top of the API calls.

It must report exactly one start per play, compute played duration on stop, and
never double-report (so a burst of redundant lifecycle calls stays correct).
"""

from unittest.mock import AsyncMock

from qobuz_proxy.playback.play_reporter import PlayReporter


def _reporter():
    api = AsyncMock()
    api.report_streaming_start = AsyncMock(return_value=True)
    api.report_streaming_end = AsyncMock(return_value=True)
    clock = {"ms": 1_000_000}
    reporter = PlayReporter(api, clock=lambda: clock["ms"])
    return reporter, api, clock


class TestPlayReporter:
    async def test_note_playing_reports_start_once(self) -> None:
        reporter, api, _ = _reporter()
        ctx = dict(track_id="100", format_id=27, blob="b", context_uuid="u")

        await reporter.note_playing(**ctx)
        await reporter.note_playing(**ctx)  # redundant — must not double-report

        api.report_streaming_start.assert_awaited_once_with(track_id="100", format_id=27)

    async def test_note_stopped_reports_end_with_played_duration(self) -> None:
        reporter, api, clock = _reporter()
        await reporter.note_playing(
            track_id="100", format_id=27, blob="theblob", context_uuid="u-1"
        )

        clock["ms"] += 183_000  # played 183 s
        await reporter.note_stopped()

        api.report_streaming_end.assert_awaited_once()
        kwargs = api.report_streaming_end.await_args.kwargs
        assert kwargs["track_id"] == "100"
        assert kwargs["blob"] == "theblob"
        assert kwargs["context_uuid"] == "u-1"
        assert kwargs["started_at_ms"] == 1_000_000
        assert kwargs["played_seconds"] == 183

    async def test_note_stopped_with_no_active_session_is_noop(self) -> None:
        reporter, api, _ = _reporter()
        await reporter.note_stopped()
        api.report_streaming_end.assert_not_awaited()

    async def test_double_stop_reports_end_once(self) -> None:
        reporter, api, _ = _reporter()
        await reporter.note_playing(track_id="100", format_id=27, blob="b", context_uuid="u")
        await reporter.note_stopped()
        await reporter.note_stopped()
        api.report_streaming_end.assert_awaited_once()

    async def test_report_start_false_skips_start_but_still_tracks(self) -> None:
        """Adopted (handoff) plays must not re-report a start the controller owns,
        but the session is still tracked so other transitions behave."""
        reporter, api, _ = _reporter()

        await reporter.note_playing(
            track_id="100", format_id=27, blob="b", context_uuid="u", report_start=False
        )

        api.report_streaming_start.assert_not_awaited()
        # A redundant note_playing for the same track is still a no-op.
        await reporter.note_playing(
            track_id="100", format_id=27, blob="b", context_uuid="u", report_start=True
        )
        api.report_streaming_start.assert_not_awaited()

    async def test_switching_track_ends_previous_then_starts_new(self) -> None:
        reporter, api, clock = _reporter()
        await reporter.note_playing(track_id="100", format_id=27, blob="b1", context_uuid="u1")
        clock["ms"] += 5_000
        await reporter.note_playing(track_id="200", format_id=6, blob="b2", context_uuid="u2")

        # Previous track's end was reported...
        api.report_streaming_end.assert_awaited_once()
        assert api.report_streaming_end.await_args.kwargs["track_id"] == "100"
        assert api.report_streaming_end.await_args.kwargs["played_seconds"] == 5
        # ...and the new track's start.
        assert api.report_streaming_start.await_count == 2
        assert api.report_streaming_start.await_args.kwargs == {"track_id": "200", "format_id": 6}

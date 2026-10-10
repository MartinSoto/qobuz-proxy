"""The device moved on to another queue entry by itself (queue handover): the
player adopts it as the current track and reports it, like a gapless
transition but without having armed anything."""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import PlaybackState, PlayResult
from qobuz_proxy.playback.queue import QueueTrack

from tests.playback.test_player_serialization import _make_player


def _player_playing_item_3():  # type: ignore[no-untyped-def]
    player, backend = _make_player()
    player._state = PlaybackState.PLAYING
    player._is_active_renderer = True
    player._current_track = QueueTrack(queue_item_id=3, track_id="T3", context_uuid=b"ctx")
    reporter = MagicMock()
    reporter.note_playing = AsyncMock()
    reporter.note_stopped = AsyncMock()
    player._play_reporter = reporter
    player._send_state_update = AsyncMock()  # type: ignore[method-assign]
    return player, backend, reporter


class TestHandleItemChanged:
    async def test_adopts_the_new_entry_as_the_current_track(self) -> None:
        player, _, _ = _player_playing_item_3()

        await player._handle_item_changed("T4", 4, None)

        assert player.current_track is not None
        assert (player.current_track.track_id, player.current_track.queue_item_id) == ("T4", 4)

    async def test_keeps_the_loads_context_for_play_reports(self) -> None:
        player, _, _ = _player_playing_item_3()

        await player._handle_item_changed("T4", 4, None)

        assert player.current_track is not None
        assert player.current_track.context_uuid == b"ctx"

    async def test_resets_the_position_and_reports_the_change(self) -> None:
        player, _, reporter = _player_playing_item_3()
        player._position_value_ms = 90_000

        await player._handle_item_changed("T4", 4, None)

        assert player._position_value_ms == 0
        reporter.note_playing.assert_awaited()
        player._send_state_update.assert_awaited()

    async def test_the_resolved_stream_is_recorded_for_the_play_report(self) -> None:
        player, _, _ = _player_playing_item_3()

        await player._handle_item_changed("T4", 4, PlayResult(blob="the-blob", format_id=7))

        track = player.current_track
        assert track is not None and (track.blob, track.actual_quality) == ("the-blob", 7)

    async def test_the_servers_old_next_pointer_is_forgotten(self) -> None:
        player, _, _ = _player_playing_item_3()
        cleared = MagicMock()
        player.set_next_track_callbacks(get_callback=lambda: None, clear_callback=cleared)

        await player._handle_item_changed("T4", 4, None)

        cleared.assert_called_once()

    async def test_the_entry_already_current_is_a_no_op(self) -> None:
        player, _, reporter = _player_playing_item_3()

        await player._handle_item_changed("T3", 3, None)

        reporter.note_playing.assert_not_awaited()
        player._send_state_update.assert_not_awaited()

    async def test_the_same_track_in_another_slot_is_a_new_entry(self) -> None:
        """A track can appear twice in a queue; only the item id tells them apart."""
        player, _, _ = _player_playing_item_3()

        await player._handle_item_changed("T3", 9, None)

        assert player.current_track is not None and player.current_track.queue_item_id == 9


class TestBackendEventReachesThePlayer:
    async def test_the_backend_callback_is_ordered_through_the_command_queue(self) -> None:
        player, backend, _ = _player_playing_item_3()
        await player.start()

        backend._notify_item_changed("T7", 7, None)
        await player._command_queue.join()

        assert player.current_track is not None and player.current_track.queue_item_id == 7


class TestQueueChanged:
    async def test_is_passed_on_to_the_backend(self) -> None:
        player, backend = _make_player()
        backend.queue_changed = AsyncMock()  # type: ignore[method-assign]

        await player.on_queue_changed()

        backend.queue_changed.assert_awaited_once()

    async def test_a_backend_failure_never_reaches_the_caller(self) -> None:
        player, backend = _make_player()
        backend.queue_changed = AsyncMock(side_effect=RuntimeError("device gone"))  # type: ignore[method-assign]

        await player.on_queue_changed()  # must not raise

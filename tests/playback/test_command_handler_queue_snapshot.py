"""Tests for the snapshot-driven queue handling in PlaybackCommandHandler:
type 90 snapshots (with the shuffled order), the debounced snapshot request
after every edit and shuffle toggle, and the session-level settings (repeat,
shuffle, autoplay) that arrive as Controller-role broadcasts.
See docs/queue-and-shuffle-wire-observations.md (O7, O10, O16, O19, O21).
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from qobuz_proxy.playback.command_handler import (
    MSG_TYPE_AUTOPLAY_MODE_SET,
    MSG_TYPE_AUTOPLAY_TRACKS_LOADED,
    MSG_TYPE_LOOP_MODE_SET,
    MSG_TYPE_SHUFFLE_MODE_SET,
    PlaybackCommandHandler,
)
from qobuz_proxy.playback.queue import QobuzQueue
from qobuz_proxy.proto import common as common_pb2
from qobuz_proxy.proto import qconnect_payload_pb2 as pb

from tests.playback.test_player_serialization import _make_player

DEBOUNCE = 0.01


def _handler(
    request: AsyncMock | None = None,
) -> tuple[PlaybackCommandHandler, QobuzQueue, AsyncMock]:
    player, _backend = _make_player()
    queue = QobuzQueue()
    request = request if request is not None else AsyncMock(return_value=True)
    handler = PlaybackCommandHandler(
        player, queue=queue, request_queue_state=request, snapshot_debounce_s=DEBOUNCE
    )
    return handler, queue, request


def _snapshot_msg(
    ids: list[int],
    *,
    shuffle: bool = False,
    indexes: list[int] | None = None,
    version: tuple[int, int] = (1, 1),
) -> pb.QConnectMessage:
    msg = pb.QConnectMessage()
    msg.messageType = 90
    state = msg.srvrCtrlQueueState
    state.queueVersion.major, state.queueVersion.minor = version
    for i in ids:
        ref = state.tracks.add()
        ref.queueItemId = i
        ref.trackId = 9000 + i
    if shuffle:
        state.shuffleMode = True
    state.shuffledTrackIndexes.extend(indexes or [])
    return msg


def _edit_msg(kind: str) -> pb.QConnectMessage:
    msg = pb.QConnectMessage()
    if kind == "added":
        msg.messageType = 93
        ref = msg.srvrCtrlQueueTracksAdded.tracks.add()
        ref.queueItemId, ref.trackId = 20, 9020
    elif kind == "inserted":
        msg.messageType = 92
        ref = msg.srvrCtrlQueueTracksInserted.tracks.add()
        ref.queueItemId, ref.trackId = 21, 9021
    elif kind == "removed":
        msg.messageType = 94
        msg.srvrCtrlQueueTracksRemoved.queueItemIds.append(1)
    elif kind == "reordered":
        msg.messageType = 95
        msg.srvrCtrlQueueTracksReordered.queueItemIds.append(2)
        msg.srvrCtrlQueueTracksReordered.insertAfter = 3
    elif kind == "shuffle":
        msg.messageType = 96
        msg.srvrCtrlShuffleModeSet.shuffleOn = True
        msg.srvrCtrlShuffleModeSet.shufflePivot = 1
    else:
        raise AssertionError(kind)
    return msg


async def _settle() -> None:
    await asyncio.sleep(DEBOUNCE * 6)


class TestSnapshotMessage:
    async def test_applies_the_shuffled_order(self) -> None:
        handler, queue, _ = _handler()

        await handler._handle_queue_state(
            _snapshot_msg([5, 6, 7, 8], shuffle=True, indexes=[0, 2, 3, 1])
        )

        order = await queue.play_order()
        assert order is not None
        assert [e.queue_item_id for e in order] == [5, 7, 8, 6]
        assert await queue.shuffle_enabled() is True

    async def test_unshuffled_snapshot_keeps_the_original_order(self) -> None:
        handler, queue, _ = _handler()

        await handler._handle_queue_state(_snapshot_msg([5, 6, 7]))

        order = await queue.play_order()
        assert order is not None
        assert [e.queue_item_id for e in order] == [5, 6, 7]

    async def test_a_snapshot_does_not_trigger_another_request(self) -> None:
        handler, _, request = _handler()

        await handler._handle_queue_state(_snapshot_msg([5, 6]))
        await _settle()

        request.assert_not_awaited()

    async def test_snapshot_records_the_version(self) -> None:
        handler, queue, _ = _handler()

        await handler._handle_queue_state(_snapshot_msg([5], version=(21, 4)))

        assert str(await queue.get_version()) == "21.4"


class TestEditsRequestASnapshot:
    @pytest.mark.parametrize("kind", ["added", "inserted", "removed", "reordered", "shuffle"])
    async def test_each_edit_asks_for_a_snapshot_once_settled(self, kind: str) -> None:
        handler, queue, request = _handler()
        await handler._handle_queue_state(_snapshot_msg([1, 2, 3]))

        await handler.handle_message(_edit_msg(kind).messageType, _edit_msg(kind))
        assert await queue.play_order() is None  # stale right away
        await _settle()

        request.assert_awaited_once()

    async def test_a_burst_of_edits_shares_one_request(self) -> None:
        handler, _, request = _handler()

        for kind in ("reordered", "reordered", "removed", "added"):
            await handler.handle_message(_edit_msg(kind).messageType, _edit_msg(kind))
        await _settle()

        assert request.await_count == 1

    async def test_an_edit_arriving_after_the_ask_went_out_asks_again(self) -> None:
        """The ask only covers what the server had seen when it was sent."""
        handler, _, request = _handler()
        calls = 0

        async def ask() -> bool:
            nonlocal calls
            calls += 1
            if calls == 1:
                handler._request_snapshot_soon()  # a new edit lands while we ask
            return True

        request.side_effect = ask

        await handler._handle_queue_tracks_added(_edit_msg("added"))
        await _settle()

        assert calls == 2

    async def test_a_failing_request_is_swallowed(self, caplog: pytest.LogCaptureFixture) -> None:
        handler, _, request = _handler(AsyncMock(side_effect=OSError("gone")))

        await handler._handle_queue_tracks_added(_edit_msg("added"))
        await _settle()

        request.assert_awaited_once()
        assert "snapshot request failed" in caplog.text

    async def test_without_a_request_callback_edits_still_work(self) -> None:
        player, _ = _make_player()
        queue = QobuzQueue()
        handler = PlaybackCommandHandler(player, queue=queue)
        await handler._handle_queue_state(_snapshot_msg([1, 2]))

        await handler._handle_queue_tracks_removed(_edit_msg("removed"))
        await _settle()

        assert await queue.current_track_ids() == frozenset({"9002"})

    async def test_clearing_the_queue_needs_no_snapshot(self) -> None:
        handler, queue, request = _handler()
        await handler._handle_queue_state(_snapshot_msg([1, 2]))
        msg = pb.QConnectMessage()
        msg.messageType = 89
        msg.srvrCtrlQueueCleared.queueVersion.major = 3

        await handler.handle_message(89, msg)
        await _settle()

        request.assert_not_awaited()
        assert await queue.play_order() == []


class TestSessionSettings:
    async def test_loop_mode_is_applied_to_the_player(self) -> None:
        handler, _, _ = _handler()
        handler.player.set_loop_mode = AsyncMock()  # type: ignore[method-assign]
        msg = pb.QConnectMessage()
        msg.messageType = MSG_TYPE_LOOP_MODE_SET
        msg.srvrCtrlLoopModeSet.mode = common_pb2.LOOP_MODE_REPEAT_ONE

        await handler.handle_message(MSG_TYPE_LOOP_MODE_SET, msg)

        handler.player.set_loop_mode.assert_awaited_once_with(common_pb2.LOOP_MODE_REPEAT_ONE)

    async def test_loop_mode_arriving_twice_is_harmless(self) -> None:
        handler, _, _ = _handler()
        handler.player.set_loop_mode = AsyncMock()  # type: ignore[method-assign]
        msg = pb.QConnectMessage()
        msg.messageType = MSG_TYPE_LOOP_MODE_SET
        msg.srvrCtrlLoopModeSet.mode = common_pb2.LOOP_MODE_REPEAT_ALL

        await handler.handle_message(MSG_TYPE_LOOP_MODE_SET, msg)
        await handler.handle_message(MSG_TYPE_LOOP_MODE_SET, msg)

        assert handler.player.set_loop_mode.await_count == 2
        assert {c.args for c in handler.player.set_loop_mode.await_args_list} == {
            (common_pb2.LOOP_MODE_REPEAT_ALL,)
        }

    async def test_loop_mode_needs_no_snapshot(self) -> None:
        handler, _, request = _handler()
        handler.player.set_loop_mode = AsyncMock()  # type: ignore[method-assign]
        msg = pb.QConnectMessage()
        msg.messageType = MSG_TYPE_LOOP_MODE_SET
        msg.srvrCtrlLoopModeSet.mode = common_pb2.LOOP_MODE_OFF

        await handler.handle_message(MSG_TYPE_LOOP_MODE_SET, msg)
        await _settle()

        request.assert_not_awaited()

    async def test_autoplay_on_and_off(self) -> None:
        handler, queue, _ = _handler()
        on = pb.QConnectMessage()
        on.messageType = MSG_TYPE_AUTOPLAY_MODE_SET
        on.srvrCtrlAutoplayModeSet.autoplayOn = True
        on.srvrCtrlAutoplayModeSet.autoplayFlag = True
        off = pb.QConnectMessage()
        off.messageType = MSG_TYPE_AUTOPLAY_MODE_SET
        off.srvrCtrlAutoplayModeSet.autoplayFlag = True  # field 3 absent (O21)

        await handler.handle_message(MSG_TYPE_AUTOPLAY_MODE_SET, on)
        assert await queue.autoplay_mode() is True
        await handler.handle_message(MSG_TYPE_AUTOPLAY_MODE_SET, off)
        assert await queue.autoplay_mode() is False

    async def test_autoplay_suggestions_only_update_the_version(self) -> None:
        handler, queue, request = _handler()
        msg = pb.QConnectMessage()
        msg.messageType = MSG_TYPE_AUTOPLAY_TRACKS_LOADED
        msg.srvrCtrlAutoplayTracksLoaded.queueVersion.major = 9
        msg.srvrCtrlAutoplayTracksLoaded.queueVersion.minor = 5
        ref = msg.srvrCtrlAutoplayTracksLoaded.tracks.add()
        ref.queueItemId, ref.trackId = 14, 1066207

        await handler.handle_message(MSG_TYPE_AUTOPLAY_TRACKS_LOADED, msg)
        await _settle()

        assert str(await queue.get_version()) == "9.5"
        request.assert_not_awaited()

    async def test_shuffle_toggle_records_its_minor_bump(self) -> None:
        handler, queue, _ = _handler()
        msg = _edit_msg("shuffle")
        msg.srvrCtrlShuffleModeSet.queueVersion.major = 16
        msg.srvrCtrlShuffleModeSet.queueVersion.minor = 2

        await handler.handle_message(MSG_TYPE_SHUFFLE_MODE_SET, msg)

        assert str(await queue.get_version()) == "16.2"

    def test_the_new_types_are_registered(self) -> None:
        handler, _, _ = _handler()

        assert {96, 97, 102, 103} <= set(handler.get_message_types())

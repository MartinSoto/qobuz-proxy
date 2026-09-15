"""Tests for the Controller-role queue-editing messages other than
QUEUE_TRACKS_LOADED: QUEUE_STATE (90, a full snapshot like
QUEUE_TRACKS_LOADED) and QUEUE_TRACKS_ADDED/INSERTED/REMOVED/REORDERED/
QUEUE_CLEARED (89, 92-95 — genuine incremental deltas, confirmed live
2026-09-15). All of these feed QobuzQueue's membership tracking (see
queue.py's QueueMembership) — the one consumer today is Sonos's
approximated-album-gain grouping.
"""

from qobuz_proxy.playback.command_handler import PlaybackCommandHandler
from qobuz_proxy.playback.queue import QobuzQueue
from qobuz_proxy.proto import qconnect_payload_pb2 as pb

from tests.playback.test_player_serialization import _make_player


def _msg_with_tracks(
    message_type: int, field_name: str, tracks: list[tuple[int, int]]
) -> pb.QConnectMessage:
    """Build a message whose payload has a `tracks` repeated field of
    QueueTrackRef {queueItemId, trackId} — shared shape across QueueState/
    Added/Inserted."""
    msg = pb.QConnectMessage()
    msg.messageType = message_type
    payload = getattr(msg, field_name)
    for queue_item_id, track_id in tracks:
        ref = payload.tracks.add()
        ref.queueItemId = queue_item_id
        ref.trackId = track_id
    return msg


def _msg_with_queue_item_ids(
    message_type: int, field_name: str, queue_item_ids: list[int]
) -> pb.QConnectMessage:
    """Shared shape for Removed/Reordered: a bare repeated queueItemIds."""
    msg = pb.QConnectMessage()
    msg.messageType = message_type
    payload = getattr(msg, field_name)
    payload.queueItemIds.extend(queue_item_ids)
    return msg


def _handler_with_real_queue() -> tuple[PlaybackCommandHandler, QobuzQueue]:
    player, _backend = _make_player()
    queue = QobuzQueue()
    return PlaybackCommandHandler(player, queue=queue), queue


class TestQueueState:
    async def test_full_snapshot_replaces_membership(self) -> None:
        handler, queue = _handler_with_real_queue()
        msg = _msg_with_tracks(90, "srvrCtrlQueueState", [(1, 3001), (2, 3002)])

        await handler._handle_queue_state(msg)

        assert await queue.current_track_ids() == frozenset({"3001", "3002"})

    async def test_does_not_drive_playback(self) -> None:
        """Confirmed live: unlike QUEUE_TRACKS_LOADED, QUEUE_STATE never
        carries a queuePosition and never selects a track to play."""
        player, _backend = _make_player()
        player._is_active_renderer = True
        queue = QobuzQueue()
        handler = PlaybackCommandHandler(player, queue=queue)
        msg = _msg_with_tracks(90, "srvrCtrlQueueState", [(1, 3001)])

        await handler._handle_queue_state(msg)

        assert player.current_track is None


class TestQueueTracksAdded:
    async def test_adds_to_an_existing_snapshot(self) -> None:
        """Confirmed live 2026-09-15: Added carries only the new track(s),
        not the whole queue."""
        handler, queue = _handler_with_real_queue()
        await queue.replace_queue_tracks([(1, "3001")])
        msg = _msg_with_tracks(93, "srvrCtrlQueueTracksAdded", [(2, 3002)])

        await handler._handle_queue_tracks_added(msg)

        assert await queue.current_track_ids() == frozenset({"3001", "3002"})


class TestQueueTracksInserted:
    async def test_inserted_is_membership_identical_to_added(self) -> None:
        """insertAfter (the anchor position) isn't tracked — see
        QueueMembership — only that the track is now present."""
        handler, queue = _handler_with_real_queue()
        await queue.replace_queue_tracks([(1, "3001")])
        msg = _msg_with_tracks(92, "srvrCtrlQueueTracksInserted", [(2, 3002)])

        await handler._handle_queue_tracks_inserted(msg)

        assert await queue.current_track_ids() == frozenset({"3001", "3002"})


class TestQueueTracksRemoved:
    async def test_removes_by_queue_item_id(self) -> None:
        handler, queue = _handler_with_real_queue()
        await queue.replace_queue_tracks([(1, "3001"), (2, "3002")])
        msg = _msg_with_queue_item_ids(94, "srvrCtrlQueueTracksRemoved", [2])

        await handler._handle_queue_tracks_removed(msg)

        assert await queue.current_track_ids() == frozenset({"3001"})


class TestQueueTracksReordered:
    async def test_reorder_does_not_change_membership(self) -> None:
        handler, queue = _handler_with_real_queue()
        await queue.replace_queue_tracks([(1, "3001"), (2, "3002")])
        msg = _msg_with_queue_item_ids(95, "srvrCtrlQueueTracksReordered", [2])
        msg.srvrCtrlQueueTracksReordered.insertAfter = 1

        await handler._handle_queue_tracks_reordered(msg)

        assert await queue.current_track_ids() == frozenset({"3001", "3002"})


class TestQueueCleared:
    async def test_clears_membership(self) -> None:
        handler, queue = _handler_with_real_queue()
        await queue.replace_queue_tracks([(1, "3001")])
        msg = pb.QConnectMessage()
        msg.messageType = 89
        msg.srvrCtrlQueueCleared.SetInParent()  # mark presence — the field has no data of its own

        await handler._handle_queue_cleared(msg)

        assert await queue.current_track_ids() == frozenset()

"""Tests for QueueHandler protobuf message handling.

The messages are built as real QConnectMessage protobufs (not mocks) so a
field-name drift between the handler and the generated pb2 fails loudly here.
"""

from typing import Any

from qobuz_proxy.connect.protocol import QConnectMessageType
from qobuz_proxy.playback.queue import QobuzQueue
from qobuz_proxy.playback.queue_handler import QueueHandler
from qobuz_proxy.proto import qconnect_payload_pb2


def _tracks_loaded_message(track_ids: list[int], queue_position: int = 0) -> Any:
    """Build a real SRVR_CTRL_QUEUE_TRACKS_LOADED (type 91) message."""
    msg = qconnect_payload_pb2.QConnectMessage()
    loaded = msg.srvrCtrlQueueTracksLoaded
    loaded.queueVersion.major = 3
    loaded.queueVersion.minor = 1
    loaded.queuePosition = queue_position
    for i, track_id in enumerate(track_ids):
        ref = loaded.tracks.add()
        ref.queueItemId = i + 1
        ref.trackId = track_id
    return msg


def _queue_state_message(track_ids: list[int]) -> Any:
    """Build a real SRVR_CTRL_QUEUE_STATE (type 90) message."""
    msg = qconnect_payload_pb2.QConnectMessage()
    state = msg.srvrCtrlQueueState
    state.queueVersion.major = 2
    state.queueVersion.minor = 0
    for i, track_id in enumerate(track_ids):
        ref = state.tracks.add()
        ref.queueItemId = i + 1
        ref.trackId = track_id
    return msg


class TestQueueTracksLoaded:
    async def test_loads_queue_from_real_protobuf(self) -> None:
        """Regression for BUG-01: the handler read message.srvrCtrlQueueLoadTracks,
        but the field on QConnectMessage is srvrCtrlQueueTracksLoaded — every
        play-album/playlist queue load was silently dropped."""
        queue = QobuzQueue()
        handler = QueueHandler(queue)
        message = _tracks_loaded_message([101, 102, 103], queue_position=1)

        await handler.handle_message(QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED, message)

        state = await queue.get_state()
        assert state.track_count == 3
        assert state.version.major == 3
        assert state.version.minor == 1
        assert state.current_queue_item_id == 2  # queue_position 1 -> second entry

        current = await queue.get_current_track()
        assert current is not None
        assert current.track_id == "102"

    async def test_loads_queue_at_position_zero(self) -> None:
        queue = QobuzQueue()
        handler = QueueHandler(queue)
        message = _tracks_loaded_message([201, 202])

        await handler.handle_message(QConnectMessageType.SRVR_CTRL_QUEUE_TRACKS_LOADED, message)

        current = await queue.get_current_track()
        assert current is not None
        assert current.track_id == "201"


class TestQueueState:
    async def test_loads_full_queue_state(self) -> None:
        queue = QobuzQueue()
        handler = QueueHandler(queue)
        message = _queue_state_message([301, 302])

        await handler.handle_message(QConnectMessageType.SRVR_CTRL_QUEUE_STATE, message)

        state = await queue.get_state()
        assert state.track_count == 2
        assert state.version.major == 2

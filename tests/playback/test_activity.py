"""What counts as the app using a room (restarting the idle clock): its
commands, not the server's own traffic."""

from qobuz_proxy.playback.command_handler import MSG_TYPE_SET_STATE, counts_as_activity
from qobuz_proxy.proto import qconnect_payload_pb2 as pb


def _set_state(**fields: int) -> pb.QConnectMessage:
    msg = pb.QConnectMessage()
    msg.messageType = 41
    state = msg.srvrRndrSetState
    if "playing_state" in fields:
        state.playingState = fields["playing_state"]
    if "position" in fields:
        state.currentPosition = fields["position"]
    if "item" in fields:
        state.currentQueueItem.queueItemId = fields["item"]
        state.currentQueueItem.trackId = 1
    if "next" in fields:
        state.nextQueueItem.queueItemId = fields["next"]
        state.nextQueueItem.trackId = 2
    state.queueVersion.major = 3
    return msg


class TestSetState:
    def test_play_pause_a_seek_or_a_track_are_activity(self) -> None:
        assert counts_as_activity(MSG_TYPE_SET_STATE, _set_state(playing_state=2))
        assert counts_as_activity(MSG_TYPE_SET_STATE, _set_state(playing_state=3))
        assert counts_as_activity(MSG_TYPE_SET_STATE, _set_state(position=55_000))
        assert counts_as_activity(MSG_TYPE_SET_STATE, _set_state(item=4))

    def test_the_servers_echo_of_our_report_is_not(self) -> None:
        """A SET_STATE carrying only the queue version and the next pointer is
        what the server sends back after every report of ours."""
        assert not counts_as_activity(MSG_TYPE_SET_STATE, _set_state(next=5))

    def test_a_message_without_the_set_state_payload_is_not(self) -> None:
        msg = pb.QConnectMessage()
        msg.messageType = 41

        assert not counts_as_activity(MSG_TYPE_SET_STATE, msg)


class TestOtherMessages:
    def test_the_volume_command_and_queue_loads_and_edits_are(self) -> None:
        for msg_type in (42, 89, 91, 92, 93, 94, 95, 96):
            assert counts_as_activity(msg_type, pb.QConnectMessage()), msg_type

    def test_broadcasts_and_restore_traffic_are_not(self) -> None:
        # set active, quality, loop/shuffle/autoplay set, queue snapshot,
        # volume broadcast, autoplay lists, renderer-list upkeep
        for msg_type in (43, 44, 45, 46, 47, 90, 97, 102, 103, 83, 84, 85, 86, 87):
            assert not counts_as_activity(msg_type, pb.QConnectMessage()), msg_type

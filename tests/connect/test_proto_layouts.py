"""Pins the wire layout of messages whose .proto declarations were wrong until
they were checked against raw frames (docs/queue-and-shuffle-wire-observations.md,
O7 and O21). The byte strings are built the way the live frames were laid out
(same field numbers and wire types), with synthetic version/uuid values."""

from qobuz_proxy.proto import payload as payload_pb2

ACTION_UUID = bytes(range(16))
# QueueVersion { major: 25, minor: 3 } as field 1, actionUuid as field 2.
_HEADER = bytes.fromhex("0a0408191003") + bytes.fromhex("1210") + ACTION_UUID


class TestAutoplayModeSetLayout:
    def test_on_message_has_autoplay_on_set(self) -> None:
        # Live "on" frame: ... field 3 = 1, field 4 = 1.
        msg = payload_pb2.SrvrCtrlAutoplayModeSet()
        msg.ParseFromString(_HEADER + bytes.fromhex("1801") + bytes.fromhex("2001"))

        assert msg.queueVersion.major == 25
        assert msg.queueVersion.minor == 3
        assert msg.actionUuid == ACTION_UUID
        assert msg.HasField("autoplayOn") and msg.autoplayOn is True
        assert msg.autoplayFlag is True

    def test_off_message_has_no_autoplay_on_field(self) -> None:
        # Live "off" frame: field 3 absent, field 4 = 1 (still set).
        msg = payload_pb2.SrvrCtrlAutoplayModeSet()
        msg.ParseFromString(_HEADER + bytes.fromhex("2001"))

        assert not msg.HasField("autoplayOn")
        assert msg.autoplayOn is False
        assert msg.autoplayFlag is True


class TestShuffleModeSetLayout:
    def test_turn_on_carries_seed_and_pivot_item_id(self) -> None:
        msg = payload_pb2.SrvrCtrlShuffleModeSet()
        msg.queueVersion.major = 16
        msg.queueVersion.minor = 2
        msg.shuffleOn = True
        msg.shuffleSeed = 3219995040  # large fixed32, differs on every toggle
        msg.shufflePivot = 164  # queueItemId of the current item

        decoded = payload_pb2.SrvrCtrlShuffleModeSet()
        decoded.ParseFromString(msg.SerializeToString())

        assert decoded.shuffleOn is True
        assert decoded.shuffleSeed == 3219995040
        assert decoded.shufflePivot == 164

    def test_turn_off_carries_neither(self) -> None:
        msg = payload_pb2.SrvrCtrlShuffleModeSet()
        msg.ParseFromString(_HEADER)

        assert not msg.HasField("shuffleOn")
        assert not msg.HasField("shuffleSeed")
        assert not msg.HasField("shufflePivot")

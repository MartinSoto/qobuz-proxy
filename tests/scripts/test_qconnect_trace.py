"""Tests for scripts/qconnect_trace.py — the shared decode/normalize logic
behind comparing a browser HAR capture against this proxy's own RAW_WS
log (see ws_manager.py's _log_raw_frame). Frames are built with the real
ProtocolCodec wherever possible, the same way tests/connect/test_protocol.py
does, rather than hand-crafted bytes — this exercises the actual wire
format, not an assumption about it.
"""

import base64
import json
import uuid
from pathlib import Path

from qobuz_proxy.connect.protocol import ProtocolCodec

from scripts.qconnect_trace import (
    _is_volatile_field,
    build_trace,
    iter_har_frames,
    iter_proxy_log_frames,
    normalize,
    render_frame,
)


def _codec() -> ProtocolCodec:
    return ProtocolCodec(device_uuid=uuid.uuid4().bytes)


class TestIsVolatileField:
    def test_matches_known_volatile_names(self) -> None:
        for name in ("deviceUuid", "sessionUuid", "messagesId", "msgId", "messagesTime", "msgDate"):
            assert _is_volatile_field(name) is True

    def test_matches_credential_fields_explicitly(self) -> None:
        """jwt/token don't match the id/uuid/time/date suffix heuristic at
        all — they're only masked because they're named explicitly. This
        pins that down as a regression test: see this file's own comment
        for why (a first version leaked a real JWT into a trace file)."""
        assert _is_volatile_field("jwt") is True
        assert _is_volatile_field("token") is True

    def test_does_not_match_meaningful_fields(self) -> None:
        for name in ("friendlyName", "playingState", "volume", "isActive", "brand"):
            assert _is_volatile_field(name) is False


class TestNormalize:
    def test_masks_bytes_and_string_credential_fields(self) -> None:
        codec = _codec()
        frame = codec.encode_authenticate("super-secret-jwt-value")
        # Parse it back the same way render_frame does, to test normalize
        # directly against a real Authenticate message.
        from qobuz_proxy.proto import envelope as envelope_pb2

        length, offset = codec._decode_varint(frame, 1)
        msg = envelope_pb2.Authenticate()
        msg.ParseFromString(frame[offset : offset + length])

        normalize(msg)

        assert msg.jwt == "<redacted>"
        assert "super-secret-jwt-value" not in str(msg)

    def test_preserves_non_volatile_values(self) -> None:
        codec = _codec()
        session_uuid = uuid.uuid4().bytes
        frame = codec.encode_join_session(
            device_uuid=uuid.uuid4().bytes,
            friendly_name="Kitchen",
            session_uuid=session_uuid,
        )
        from qobuz_proxy.proto import payload as payload_pb2

        length, offset = codec._decode_varint(frame, 1)
        from qobuz_proxy.proto import envelope as envelope_pb2

        payload_msg = envelope_pb2.Payload()
        payload_msg.ParseFromString(frame[offset : offset + length])
        batch = payload_pb2.QConnectBatch()
        batch.ParseFromString(payload_msg.payload)

        normalize(batch)

        join = batch.messages[0].rndrSrvrJoinSession
        assert join.deviceInfo.friendlyName == "Kitchen"  # untouched
        assert join.sessionUuid == b"<redacted>"  # masked
        assert join.deviceInfo.deviceUuid == b"<redacted>"  # masked


class TestRenderFrame:
    def test_authenticate_frame_masks_jwt_in_output(self) -> None:
        codec = _codec()
        frame = codec.encode_authenticate("super-secret-jwt-value")

        text = render_frame(0, 0.0, "SEND", frame)

        assert "AUTHENTICATE" in text
        assert "super-secret-jwt-value" not in text
        assert "<redacted>" in text

    def test_payload_frame_names_the_inner_message_type(self) -> None:
        codec = _codec()
        frame = codec.encode_join_session(
            device_uuid=uuid.uuid4().bytes,
            friendly_name="Kitchen",
            session_uuid=uuid.uuid4().bytes,
        )

        text = render_frame(0, 0.0, "RECV", frame)

        assert "PAYLOAD" in text
        assert "MESSAGE_TYPE_RNDR_SRVR_JOIN_SESSION" in text
        assert "Kitchen" in text

    def test_malformed_frame_does_not_raise(self) -> None:
        text = render_frame(0, 0.0, "SEND", b"\x06")  # PAYLOAD, no length byte
        assert "malformed" in text or "bad" in text.lower()

    def test_unknown_outer_type_does_not_raise(self) -> None:
        text = render_frame(0, 0.0, "SEND", b"\xff\x00")
        assert "unknown outer type" in text


class TestIterHarFrames:
    def test_extracts_frames_relative_to_first_timestamp(self, tmp_path: Path) -> None:
        codec = _codec()
        frame = codec.encode_authenticate("x")
        har = {
            "log": {
                "entries": [
                    {
                        "_webSocketMessages": [
                            {
                                "type": "send",
                                "time": 10.0,
                                "data": base64.b64encode(frame).decode(),
                            },
                            {
                                "type": "receive",
                                "time": 10.5,
                                "data": base64.b64encode(frame).decode(),
                            },
                        ]
                    }
                ]
            }
        }
        har_path = tmp_path / "capture.har"
        har_path.write_text(json.dumps(har))

        frames = list(iter_har_frames(har_path))

        assert len(frames) == 2
        assert frames[0] == (0.0, "SEND", frame)
        assert frames[1] == (0.5, "RECV", frame)

    def test_entries_without_websocket_messages_are_skipped(self, tmp_path: Path) -> None:
        har = {"log": {"entries": [{"request": {"url": "https://example.com"}}]}}
        har_path = tmp_path / "capture.har"
        har_path.write_text(json.dumps(har))

        assert list(iter_har_frames(har_path)) == []


class TestIterProxyLogFrames:
    def test_extracts_raw_ws_lines_and_ignores_everything_else(self, tmp_path: Path) -> None:
        raw1, raw2 = b"\x01\x02", b"\x03\x04\x05"
        log_text = (
            "2026-09-13 00:00:00,000 - some.module - INFO - unrelated line\n"
            f"2026-09-13 00:00:00,000 - qobuz_proxy.connect.ws_manager.raw - INFO - "
            f"RAW_WS SEND 1000 {base64.b64encode(raw1).decode()}\n"
            "2026-09-13 00:00:00,100 - some.module - WARNING - also unrelated\n"
            f"2026-09-13 00:00:00,200 - qobuz_proxy.connect.ws_manager.raw - INFO - "
            f"RAW_WS RECV 1250 {base64.b64encode(raw2).decode()}\n"
        )
        log_path = tmp_path / "qobuz.log"
        log_path.write_text(log_text)

        frames = list(iter_proxy_log_frames(log_path))

        assert frames == [(0.0, "SEND", raw1), (0.25, "RECV", raw2)]

    def test_empty_or_non_matching_log_yields_nothing(self, tmp_path: Path) -> None:
        log_path = tmp_path / "qobuz.log"
        log_path.write_text("nothing interesting here\nor here\n")

        assert list(iter_proxy_log_frames(log_path)) == []


class TestBuildTrace:
    def test_numbers_frames_in_order_with_separators(self) -> None:
        codec = _codec()
        frame = codec.encode_authenticate("x")
        frames = iter(
            [
                (0.0, "SEND", frame),
                (0.1, "RECV", frame),
            ]
        )

        text = build_trace(frames)

        assert "[0000]" in text
        assert "[0001]" in text
        assert text.count("-" * 80) == 2

    def test_empty_input_yields_empty_string(self) -> None:
        assert build_trace(iter([])) == ""

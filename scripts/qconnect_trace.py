#!/usr/bin/env python3
"""
Decode a Qobuz Connect WebSocket capture into a normalized, diff-friendly
text trace — from either a browser DevTools HAR export or this proxy's own
raw-frame log (see ws_manager.py's RAW_WS logging, _log_raw_frame).

Purpose: this proxy joins the Connect session as both Renderer and
Controller (same deviceUuid — see ws_manager.py's own module-level
comment), the same way the phone app and the web player do, but something
about how it does so isn't quite right yet. This script exists to compare,
message for message, what a real client sends and receives against what
this proxy does, so a genuine protocol difference stands out instead of
being buried in incidental noise (every session has its own random UUIDs,
message ids, timestamps, catalog ids — none of that is meaningful to
compare).

Usage:
    # Decode a captured browser HAR (Chrome DevTools export with
    # _webSocketMessages, e.g. via "Copy all as HAR" on the Network panel):
    python3 scripts/qconnect_trace.py --har browser.json -o web-player.trace

    # Decode this proxy's own log (must contain RAW_WS lines — run with
    # the connect-controller-role branch, which logs them unconditionally):
    python3 scripts/qconnect_trace.py --proxy-log qobuz-queue.log -o proxy.trace

    diff web-player.trace proxy.trace

Run with .venv/bin/python3 (or `uv run python3`), not a bare `python3` —
this imports the proxy's own compiled protobuf modules.
"""

import argparse
import base64
import json
import re
import sys
from pathlib import Path
from typing import Iterator, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.protobuf.message import Message  # noqa: E402

from qobuz_proxy.connect.protocol import MessageType  # noqa: E402
from qobuz_proxy.proto import envelope as envelope_pb2  # noqa: E402
from qobuz_proxy.proto import payload as payload_pb2  # noqa: E402

# Outer envelope message classes, keyed by MessageType — everything this
# codec's decode_frame() also switches on (protocol.py), but here we parse
# into the *full* message object instead of cherry-picking a few fields
# into DecodedMessage, since a diff needs everything that was actually set.
#
# MessageType.ERROR (9) is deliberately absent: envelope_pb2 has no Error
# message class at all (protocol.py's own decoder references
# envelope_pb2.Error, which doesn't exist either — silently swallowed by
# its own broad except Exception, apparently never hit in practice since
# no capture this project has ever shows the server sending an outer-
# envelope-level ERROR frame; the "Protocol error: code=X" lines seen
# throughout this project are all the unrelated *inner* per-command error,
# QConnectMessage.error, messageType=1, arriving inside an ordinary
# PAYLOAD frame — handled fine below). Rather than guess at an unobserved
# shape, a real ERROR(9) frame just gets its header line with no body.
_ENVELOPE_TYPES = {
    MessageType.AUTHENTICATE: envelope_pb2.Authenticate,
    MessageType.SUBSCRIBE: envelope_pb2.Subscribe,
    MessageType.UNSUBSCRIBE: envelope_pb2.Unsubscribe,
    MessageType.PAYLOAD: envelope_pb2.Payload,
    MessageType.DISCONNECT: envelope_pb2.Disconnect,
}

# Field names that never carry anything meaningful to compare across two
# independent sessions — every capture has its own random UUIDs, message
# ids, timestamps, and catalog/queue ids, none of which will ever match
# between a browser session and a proxy session even when the *protocol*
# behavior is identical. Matched by suffix (case-insensitive) against
# every scalar field's own name, applied recursively through the whole
# decoded tree — see normalize(). A few fields (Subscribe/Unsubscribe's
# `channels`, Payload's `src`/`dests`) carry session-derived identity but
# don't match the suffix heuristic, so they're named explicitly instead.
#
# `jwt` and `token` are here for a different reason and must never be
# removed: Authenticate.jwt (qconnect_envelope.proto) and
# Authenticate_message.token (qconnect_common.proto) are live credentials,
# not incidental noise — a first version of this script printed a real
# JWT bearer token in full before this list existed. Never trust the
# suffix heuristic alone for a new credential-shaped field found later;
# name it explicitly here too.
_VOLATILE_SUFFIXES = ("id", "uuid", "time", "date")
_VOLATILE_EXACT_NAMES = {"timestamp", "channels", "src", "dests", "jwt", "token"}

_REDACTED_BYTES = b"<redacted>"
_REDACTED_STRING = "<redacted>"


def _is_volatile_field(name: str) -> bool:
    lname = name.lower()
    return lname in _VOLATILE_EXACT_NAMES or lname.endswith(_VOLATILE_SUFFIXES)


def normalize(msg: Message) -> Message:
    """Mask every volatile field's value in place, recursing into nested
    messages, and return `msg` for convenience. Presence is preserved
    (a field that was set stays set, just with its value replaced) —
    losing that distinction would hide a genuine "one side omits this
    field" difference, which is exactly the kind of thing this trace is
    meant to catch."""
    for field, value in list(msg.ListFields()):
        if field.type == field.TYPE_MESSAGE:
            if field.is_repeated:
                for item in value:
                    normalize(item)
            else:
                normalize(value)
        elif _is_volatile_field(field.name):
            if field.is_repeated:
                seq = getattr(msg, field.name)
                count = len(seq)
                del seq[:]
                if field.type == field.TYPE_BYTES:
                    seq.extend([_REDACTED_BYTES] * count)
                elif field.type == field.TYPE_STRING:
                    seq.extend([_REDACTED_STRING] * count)
                else:
                    seq.extend([0] * count)
            elif field.type == field.TYPE_BYTES:
                setattr(msg, field.name, _REDACTED_BYTES)
            elif field.type == field.TYPE_STRING:
                setattr(msg, field.name, _REDACTED_STRING)
            else:
                # Numeric/enum/bool: 0 keeps the field "set" (HasField
                # stays true) without leaking the real value.
                setattr(msg, field.name, 0)
    return msg


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line else line for line in text.splitlines())


def _decode_varint(data: bytes, start: int) -> Tuple[int, int]:
    """Mirrors ProtocolCodec._decode_varint — duplicated rather than
    imported since that one's a private method on an instance, and this
    has no ProtocolCodec instance (no device_uuid to construct one with,
    and doesn't need one — decoding needs no state)."""
    value = 0
    shift = 0
    offset = start
    while offset < len(data):
        b = data[offset]
        value |= (b & 0x7F) << shift
        offset += 1
        if not (b & 0x80):
            return value, offset
        shift += 7
    return -1, -1


def render_frame(index: int, t: float, direction: str, raw: bytes) -> str:
    """One frame, fully decoded and normalized, as a self-contained text
    block. Never raises — a malformed or unrecognized frame still gets a
    line in the trace (saying so) rather than aborting the whole run;
    losing frames silently would be worse than an ugly one."""
    header = f"[{index:04d}] t={t:8.3f}s {direction}"
    lines = [header]

    if len(raw) < 2:
        lines.append(f"  <malformed frame: only {len(raw)} byte(s)>")
        return "\n".join(lines)

    try:
        outer_type = MessageType(raw[0])
    except ValueError:
        lines.append(f"  <unknown outer type {raw[0]}, {len(raw)} byte(s) total>")
        return "\n".join(lines)

    length, offset = _decode_varint(raw, 1)
    if length < 0 or offset + length > len(raw):
        lines.append("  <bad/truncated varint length>")
        return "\n".join(lines)

    lines[0] += f" {outer_type.name}"
    body = raw[offset : offset + length]

    envelope_cls = _ENVELOPE_TYPES.get(outer_type)
    if envelope_cls is None:
        return "\n".join(lines)

    envelope_msg = envelope_cls()
    try:
        envelope_msg.ParseFromString(body)
    except Exception as e:
        lines.append(f"  <failed to parse {envelope_cls.__name__}: {e}>")
        return "\n".join(lines)

    if outer_type != MessageType.PAYLOAD:
        normalize(envelope_msg)
        text = str(envelope_msg).strip()
        if text:
            lines.append(_indent(text, 2))
        return "\n".join(lines)

    batch = payload_pb2.QConnectBatch()
    try:
        batch.ParseFromString(envelope_msg.payload)
    except Exception as e:
        lines.append(f"  <failed to parse QConnectBatch: {e}>")
        return "\n".join(lines)

    if not batch.messages:
        lines.append("  <empty batch>")
        return "\n".join(lines)

    for qc_msg in batch.messages:
        try:
            type_name = payload_pb2.QConnectMessageType.Name(qc_msg.messageType)
        except ValueError:
            type_name = str(qc_msg.messageType)
        normalize(qc_msg)
        lines.append(f"  -- {type_name} --")
        body_text = str(qc_msg).strip()
        if body_text:
            lines.append(_indent(body_text, 4))

    return "\n".join(lines)


def iter_har_frames(path: Path) -> Iterator[Tuple[float, str, bytes]]:
    """Yields (relative_seconds, "SEND"|"RECV", raw_bytes) for every
    WebSocket frame in a Chrome DevTools HAR export. Only entries that
    actually carry _webSocketMessages (i.e. were a WS upgrade) produce
    anything; there's normally exactly one such entry per capture."""
    data = json.loads(path.read_text())
    for entry in data.get("log", {}).get("entries", []):
        messages = entry.get("_webSocketMessages")
        if not messages:
            continue
        t0 = messages[0]["time"]
        for m in messages:
            direction = "SEND" if m["type"] == "send" else "RECV"
            raw = base64.b64decode(m["data"])
            yield (m["time"] - t0, direction, raw)


_RAW_WS_LINE_RE = re.compile(r"RAW_WS (SEND|RECV) (\d+) (\S+)")


def iter_proxy_log_frames(path: Path) -> Iterator[Tuple[float, str, bytes]]:
    """Yields (relative_seconds, "SEND"|"RECV", raw_bytes) for every
    RAW_WS line in a qobuz-proxy log — see ws_manager.py's
    _log_raw_frame for the exact format this regex depends on. Any
    non-matching line (everything else the log contains) is silently
    skipped, so this can be pointed at a full, mixed-content log file
    directly."""
    t0 = None
    for line in path.read_text(errors="replace").splitlines():
        match = _RAW_WS_LINE_RE.search(line)
        if not match:
            continue
        direction, epoch_ms_str, b64 = match.groups()
        epoch_ms = int(epoch_ms_str)
        if t0 is None:
            t0 = epoch_ms
        yield ((epoch_ms - t0) / 1000.0, direction, base64.b64decode(b64))


def build_trace(frames: Iterator[Tuple[float, str, bytes]]) -> str:
    out_lines = []
    for i, (t, direction, raw) in enumerate(frames):
        out_lines.append(render_frame(i, t, direction, raw))
        out_lines.append("-" * 80)
    return "\n".join(out_lines) + ("\n" if out_lines else "")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--har", type=Path, help="Chrome DevTools HAR export with _webSocketMessages"
    )
    source.add_argument(
        "--proxy-log", type=Path, help="qobuz-proxy log file containing RAW_WS lines"
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=None, help="Write the trace here instead of stdout"
    )
    args = parser.parse_args()

    frames = iter_har_frames(args.har) if args.har else iter_proxy_log_frames(args.proxy_log)
    text = build_trace(frames)

    if args.output:
        args.output.write_text(text)
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()

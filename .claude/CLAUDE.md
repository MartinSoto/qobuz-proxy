# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QobuzProxy is a headless Qobuz music player that appears as a Qobuz Connect device, controllable from the official Qobuz app. It supports two audio backends: DLNA renderers (Sonos, Denon HEOS, etc.) and local audio output via PortAudio.

## Commands

This project uses **[uv](https://docs.astral.sh/uv/)** to manage the local environment (`.venv` + `uv.lock`). Do not use `pip`/`venv` directly.

```bash
# Setup
uv sync                                     # Create .venv, install runtime deps + dev group (default), write uv.lock
uv sync --all-extras                        # Also install the local audio backend extra (sounddevice, numpy, soundfile)

# Run (uv run executes inside .venv — no manual activation needed)
uv run python -m qobuz_proxy
uv run qobuz-proxy --config config.yaml    # Then visit http://localhost:8689 to log in
uv run qobuz-proxy --discover              # Find DLNA renderers
uv run qobuz-proxy --discover-sonos        # Show Sonos household rooms/groups

# Test
uv run pytest                              # All tests
uv run pytest tests/connect/test_protocol.py      # Single file
uv run pytest tests/connect/test_protocol.py::TestProtocol::test_method  # Single test

# Code quality
uv run ruff format qobuz_proxy/ tests/     # Format (100 char line length)
uv run ruff check qobuz_proxy/ tests/      # Lint
uv run mypy qobuz_proxy/                    # Type check (strict)
```

Run Python via `uv run` (or `uv run python`), never bare `python`/`python3` and never `pip install`. Add/remove dependencies with `uv add` / `uv remove` so `pyproject.toml` and `uv.lock` stay in sync.

## Protocol Buffer Compilation

Must be run before first use. Re-run if `.proto` files change:

```bash
protoc --python_out=qobuz_proxy/proto -I protos protos/*.proto

# Fix relative imports in generated files (macOS uses sed -i '', Linux uses sed -i)
sed -i'' -e 's/^import qconnect_common_pb2/from . import qconnect_common_pb2/g' qobuz_proxy/proto/qconnect_payload_pb2.py qobuz_proxy/proto/qconnect_queue_pb2.py
sed -i'' -e 's/^import qconnect_queue_pb2/from . import qconnect_queue_pb2/g' qobuz_proxy/proto/qconnect_payload_pb2.py
```

Proto files in `protos/`: `qconnect_common.proto`, `qconnect_envelope.proto`, `qconnect_payload.proto`, `qconnect_queue.proto`, `ws.proto`

## Architecture

### Component Wiring (app.py)

`QobuzProxy` in `app.py` is the main orchestrator. It wires together:

1. **Auth** (`auth/`): OAuth login via Qobuz desktop app credentials (`oauth.py`), MD5-signed API requests (`api_client.py`), token persistence (`credentials.py`), session/JWT tokens (`tokens.py`). See `docs/authentication.md` for details.
2. **Connect** (`connect/`): Registers as mDNS device + HTTP discovery endpoints (`discovery.py`), manages WebSocket connection to Qobuz servers (`ws_manager.py`), encodes/decodes protobuf messages (`protocol.py`)
3. **Playback** (`playback/`): State machine player (`player.py`), queue management (`queue.py`), track metadata from Qobuz API (`metadata.py`), command handlers (`command_handler.py`, `queue_handler.py`, `volume_handler.py`), periodic state reporting to Qobuz app (`state_reporter.py`)
4. **Backend** (`backends/`): Abstract `AudioBackend` interface (`base.py`), factory/registry pattern (`factory.py`). Two implementations:
   - **DLNA** (`dlna/`): SOAP/UPnP client (`client.py`), device capability detection (`capabilities.py`), audio proxy server (`proxy_server.py`, with on-the-fly Hi-Res downsampling via `transcoding_reader.py`/`lazy_flac_source.py`). Deliberately generic — no manufacturer knowledge lives here; see `dlna/sonos/` below.
   - **Sonos** (`dlna/sonos/`): everything Sonos-specific, layered on the generic DLNA classes via subclassing rather than manufacturer-string branching — `SonosClient`/`SonosBackend` (queue-based playback, whole-group volume), `topology.py`/`discovery_manager.py`/`events.py` (continuous household discovery: SSDP + GENA eventing), `controller.py` (`SonosController`, turning discovered rooms into Speaker/Qobuz Connect sessions — see Sonos Auto-Discovery below). `BackendFactory` picks `DLNABackend` vs `SonosBackend` by probing the device's manufacturer before connecting.
   - **Local** (`local/`): Downloads FLAC, decodes to float32, plays via PortAudio (`backend.py`), ring buffer for streaming (`ring_buffer.py`), sounddevice output stream (`stream.py`). Optional deps: `sounddevice`, `numpy`, `soundfile`

### Key Data Flows

**Qobuz app command → audio playback**: WebSocket message → `protocol.py` decodes → `ws_manager.py` dispatches to handler → `command_handler.py`/`queue_handler.py` → `player.py` state machine → `DLNABackend` → DLNA SOAP commands to device

**Audio streaming (DLNA)**: Qobuz CDN → `proxy_server.py` (aiohttp server on port 7120) → DLNA device. The proxy is needed because DLNA devices fetch audio via HTTP GET, and Qobuz streaming URLs require specific headers.

**Audio streaming (Local)**: Qobuz CDN → aiohttp download → soundfile FLAC decode → float32 samples → `RingBuffer` → `AudioOutputStream` (PortAudio callback) → speakers

**State reporting**: `state_reporter.py` periodically builds state (playback state, position, queue) → `protocol.py` encodes to protobuf → WebSocket → Qobuz servers → Qobuz app UI

### Quality Auto-Detection

When `max_quality: auto`: DLNA `GetProtocolInfo` → `capabilities.py` parses Sink string → maps to Qobuz quality (27=Hi-Res 192k, 7=Hi-Res 96k, 6=CD, 5=MP3). Falls back to CD quality (6) on failure. Manufacturer-specific corrections (e.g. Sonos's 48kHz platform cap) go through `capabilities.py`'s `register_override()` registry rather than hardcoded branches — see `dlna/sonos/capabilities.py` for the one registered today.

### Sonos Auto-Discovery

`sonos_auto_discover: true` (or `QOBUZPROXY_SONOS_AUTO_DISCOVER`) replaces the static `speakers` list with continuous household discovery, mutually exclusive with it. `SonosController` (`dlna/sonos/controller.py`) is what `app.py` starts/stops in that mode — it owns every auto-discovered `Speaker` end to end and is the only thing `app.py` talks to for it (`_all_speakers()` merges its speakers with any manual ones for the web UI/shutdown).

```
SonosDiscoveryManager (SSDP poll + GENA eventing, Sonos topology only)
  → found/lost/renamed/retargeted/rekeyed/members_departed callbacks
  → SonosController (dlna/sonos/controller.py)
  → Speaker start/stop/rename/retarget (Qobuz Connect session lifecycle)
```

A group is tracked by its own `group_id` (confirmed stable across a coordinator handoff), not the coordinator's physical `uuid` — so a handoff renames/retargets the existing `Speaker` in place instead of tearing down and recreating it. `generate_sonos_speaker_uuid()` derives the Qobuz Connect device identity from that same `group_id`, so a promoted coordinator computes the identity its predecessor had and the Qobuz app sees a reconnect, not a new device. `Speaker.is_active` (wraps `Player.is_active_renderer`) is what gates whether a departing/idle Sonos room actually gets sent a live device Stop — a room that's merely discovered, not the one Qobuz is actually driving, must never be interrupted.

## Configuration Priority

1. CLI arguments (highest) → 2. Environment variables → 3. YAML config file → 4. Code defaults

Key env vars: `QOBUZ_AUTH_TOKEN`, `QOBUZ_USER_ID`, `QOBUZ_MAX_QUALITY`, `QOBUZPROXY_BACKEND`, `QOBUZPROXY_DEVICE_NAME`, `QOBUZPROXY_DLNA_IP`, `QOBUZPROXY_AUDIO_DEVICE`, `QOBUZPROXY_LOG_LEVEL`, `QOBUZPROXY_SONOS_AUTO_DISCOVER`, `QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING`

## Code Style

- **Ruff** for both formatting (`ruff format`) and linting, 100 char line length, **mypy** strict
- Type hints required on all public functions, Google-style docstrings only for non-obvious APIs
- All I/O is async. No blocking calls in main event loop
- Never log passwords or auth tokens

## Testing

- `asyncio_mode = "auto"` in pyproject.toml — no `@pytest.mark.asyncio` decorators needed
- Tests mirror source structure in `tests/`

## Commit Convention

`feat(module):`, `fix(module):`, `refactor(module):`, `test(module):`, `docs:`

## Reference Materials

- **Protocol reference**: [StreamCore32](https://github.com/tobiasguyer/StreamCore32) (C++ ESP32 Qobuz Connect implementation)

## Debugging

### Reference Implementation

For Qobuz Connect protocol issues, the key files in [StreamCore32](https://github.com/tobiasguyer/StreamCore32) are:
- `stream/qobuz/src/QobuzPlayer.cpp` — Position tracking, state management
- `stream/qobuz/src/QobuzStream.cpp` — WebSocket message handling

### Position Tracking Data Flow (DLNA)

```
DLNA Device (GetPositionInfo SOAP → RelTime string)
  → DLNAClient.get_position_info() (parses to ms)
  → DLNABackend.get_position() (updates _position_ms, notifies callback)
  → Player._on_position_update() (sets _position_value_ms + _position_timestamp_ms)
  → StateReporter._build_state_report() (reads player._position_value_ms)
  → Protocol.encode_state_update() (encodes Position{timestamp, value})
  → WebSocket → Qobuz app
```

The protocol uses `Position { timestamp: fixed64, value: uint32 }`. The app interpolates: `value + (now - timestamp)`.

### Common Issues

1. **Position always 0**: DLNA device may not support `GetPositionInfo` for the current source
2. **State not updating**: `_playback_monitor_loop` only polls when `_state == PlaybackState.PLAYING`
3. **Protocol encoding**: Log values passed to `encode_state_update()` — binary issues are invisible otherwise

### Known Issues

**Qobuz app shows wrong quality** (FR-DLNA-08): App always displays "Hi-Res 96k" regardless of actual streaming quality. Audio streams correctly at auto-detected quality. Investigation needed: check if a protocol message reports quality capability, review StreamCore32 for quality reporting fields.

## Docker

Uses `network_mode: host` (required for mDNS). Ports: 8689 (HTTP discovery), 7120 (audio proxy). See `docker-compose.yaml` and `.env.example`.

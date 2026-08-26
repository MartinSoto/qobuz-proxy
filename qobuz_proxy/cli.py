"""
QobuzProxy CLI entry point.

Provides command-line interface for running QobuzProxy.
"""

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from qobuz_proxy import __commit__, __version__
from qobuz_proxy.config import Config, ConfigError, load_config, AUTO_QUALITY
from qobuz_proxy.app import QobuzProxy
from qobuz_proxy.backends import BackendNotFoundError


def _version_string() -> str:
    """Format version string with optional commit suffix."""
    return f"v{__version__} ({__commit__})" if __commit__ else f"v{__version__}"


logger = logging.getLogger(__name__)


# Exit codes
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_AUTH_ERROR = 2
EXIT_NETWORK_ERROR = 3


def setup_logging(level: str = "info") -> None:
    """Configure logging to stdout."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )


def _parse_quality(value: str) -> int:
    """Parse quality argument, handling 'auto' and numeric values."""
    if value.lower() == "auto":
        return AUTO_QUALITY
    try:
        v = int(value)
        if v not in {5, 6, 7, 27}:
            raise argparse.ArgumentTypeError(f"Invalid quality: {v}. Use 5, 6, 7, 27, or 'auto'")
        return v
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid quality: {value}. Use 5, 6, 7, 27, or 'auto'")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="qobuz-proxy",
        description="Headless Qobuz music player service with DLNA support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  qobuz-proxy --discover
  qobuz-proxy --discover --timeout 10 --json
  qobuz-proxy --discover-sonos
  qobuz-proxy --sonos-auto-discover
  qobuz-proxy --config config.yaml
  qobuz-proxy --email user@example.com --auth-token TOKEN --user-id ID --dlna-ip 192.168.1.50

Environment Variables:
  QOBUZ_EMAIL, QOBUZ_AUTH_TOKEN, QOBUZ_USER_ID, QOBUZ_MAX_QUALITY
  QOBUZPROXY_DEVICE_NAME, QOBUZPROXY_DLNA_IP, QOBUZPROXY_DLNA_PORT
  QOBUZPROXY_HTTP_PORT, QOBUZPROXY_PROXY_PORT, QOBUZPROXY_LOG_LEVEL
""",
    )

    # General
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_version_string()}",
    )

    # Discovery mode
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Scan network for DLNA renderers and exit",
    )
    parser.add_argument(
        "--discover-sonos",
        action="store_true",
        help="Scan for Sonos players and show household rooms/groups and exit",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=3.0,
        metavar="SECONDS",
        help="Discovery timeout in seconds (used with --discover/--discover-sonos)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output as JSON (used with --discover/--discover-sonos)",
    )
    parser.add_argument(
        "--list-audio-devices",
        action="store_true",
        help="List available audio output devices and exit",
    )

    # Configuration
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to config file (default: ./config.yaml or $QOBUZPROXY_DATA_DIR/config.yaml)",
    )

    # Authentication
    auth_group = parser.add_argument_group("Authentication")
    auth_group.add_argument(
        "--email",
        metavar="TEXT",
        help="Qobuz account email",
    )
    auth_group.add_argument(
        "--auth-token",
        metavar="TOKEN",
        help="Qobuz auth token (from browser login)",
    )
    auth_group.add_argument(
        "--user-id",
        metavar="ID",
        help="Qobuz user ID (from browser login)",
    )
    auth_group.add_argument(
        "--password",
        metavar="TEXT",
        help=argparse.SUPPRESS,
    )
    auth_group.add_argument(
        "--max-quality",
        type=_parse_quality,
        metavar="INT|auto",
        help="Audio quality (5=MP3, 6=CD, 7=96k, 27=192k, auto=detect)",
    )

    # Device
    device_group = parser.add_argument_group("Device")
    device_group.add_argument(
        "--name",
        metavar="TEXT",
        help="Device name shown in Qobuz app",
    )
    device_group.add_argument(
        "--uuid",
        metavar="TEXT",
        help="Device UUID (auto-generated if omitted)",
    )

    # DLNA Backend
    dlna_group = parser.add_argument_group("DLNA Backend")
    dlna_group.add_argument(
        "--dlna-ip",
        metavar="TEXT",
        help="DLNA renderer IP address",
    )
    dlna_group.add_argument(
        "--dlna-port",
        type=int,
        metavar="INT",
        help="DLNA renderer port (default: 1400)",
    )
    dlna_group.add_argument(
        "--fixed-volume",
        action="store_true",
        help="Ignore volume commands (for external amp control)",
    )
    dlna_group.add_argument(
        "--hires-downsampling",
        action="store_true",
        help=(
            "Experimental: request Hi-Res from Qobuz for Sonos devices with "
            "real 24-bit support and downsample on the fly when a track "
            "exceeds the device's sample-rate cap (default: off)"
        ),
    )

    # Local Audio Backend
    local_group = parser.add_argument_group("Local Audio Backend")
    local_group.add_argument(
        "--audio-device",
        metavar="TEXT",
        help="Audio output device (name, index, or 'default')",
    )
    local_group.add_argument(
        "--audio-buffer-size",
        type=int,
        metavar="INT",
        help="Audio buffer size in frames (default: 2048)",
    )

    # Backend type
    parser.add_argument(
        "--backend-type",
        choices=["dlna", "local"],
        metavar="TYPE",
        help="Audio backend type: dlna or local",
    )
    parser.add_argument(
        "--sonos-auto-discover",
        action="store_true",
        help="Continuously discover Sonos rooms/groups instead of configured speakers",
    )

    # Server
    server_group = parser.add_argument_group("Server")
    server_group.add_argument(
        "--http-port",
        type=int,
        metavar="INT",
        help="HTTP server port (default: 8689)",
    )
    server_group.add_argument(
        "--proxy-port",
        type=int,
        metavar="INT",
        help="Audio proxy port (default: 7120)",
    )
    server_group.add_argument(
        "--bind",
        metavar="TEXT",
        help="Bind address (default: 0.0.0.0)",
    )

    # Logging
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        metavar="LEVEL",
        help="Log level: debug, info, warning, error",
    )

    return parser.parse_args()


def _set_nested(d: dict, path: tuple, value: Any) -> None:
    """Set a nested dictionary value."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


def args_to_dict(args: argparse.Namespace) -> dict:
    """Convert argparse namespace to nested config dict."""
    result: dict = {}

    # Map CLI args to config paths
    mappings = {
        "email": ("qobuz", "email"),
        "auth_token": ("qobuz", "auth_token"),
        "user_id": ("qobuz", "user_id"),
        "password": ("qobuz", "auth_token"),  # Deprecated alias
        "max_quality": ("qobuz", "max_quality"),
        "name": ("device", "name"),
        "uuid": ("device", "uuid"),
        "dlna_ip": ("backend", "dlna", "ip"),
        "dlna_port": ("backend", "dlna", "port"),
        "fixed_volume": ("backend", "dlna", "fixed_volume"),
        "hires_downsampling": ("backend", "dlna", "hires_downsampling"),
        "audio_device": ("backend", "local", "device"),
        "audio_buffer_size": ("backend", "local", "buffer_size"),
        "backend_type": ("backend", "type"),
        "http_port": ("server", "http_port"),
        "proxy_port": ("backend", "dlna", "proxy_port"),
        "bind": ("server", "bind_address"),
        "log_level": ("logging", "level"),
        "sonos_auto_discover": ("sonos_auto_discover",),
    }

    # store_true flags: only set (and thus override lower-priority sources)
    # when the user explicitly passed them, never for their False default.
    store_true_flags = {"fixed_volume", "hires_downsampling", "sonos_auto_discover"}

    for arg_name, path in mappings.items():
        value = getattr(args, arg_name, None)
        if value is None:
            continue
        if arg_name in store_true_flags and not value:
            continue
        _set_nested(result, path, value)

    return result


def log_config(config: Config) -> None:
    """Log configuration summary (without sensitive data)."""
    for i, sc in enumerate(config.speakers):
        prefix = f"Speaker {i + 1}" if len(config.speakers) > 1 else "Device"
        logger.info(f"{prefix}: {sc.name} ({sc.uuid[:8]}...)")
        if sc.backend_type == "dlna":
            logger.info(f"  DLNA target: {sc.dlna_ip}:{sc.dlna_port}")
            if sc.dlna_fixed_volume:
                logger.info("  Volume control: disabled (fixed_volume=true)")
            if sc.dlna_hires_downsampling:
                logger.info("  Hi-Res downsampling: enabled (experimental)")
            logger.info(f"  Proxy server: {sc.bind_address}:{sc.proxy_port}")
        elif sc.backend_type == "local":
            logger.info(f"  Audio device: {sc.audio_device}")
            logger.info(f"  Buffer size: {sc.audio_buffer_size} frames")
        logger.info(f"  HTTP server: {sc.bind_address}:{sc.http_port}")
        logger.info(f"  Max quality: {sc.max_quality}")


async def run_discovery(timeout: float, json_output: bool) -> int:
    """
    Run DLNA device discovery.

    Args:
        timeout: Discovery timeout in seconds
        json_output: Output as JSON if True

    Returns:
        Exit code
    """
    from qobuz_proxy.backends.dlna.sonos import discover_and_enrich

    if not json_output:
        print(f"Scanning for DLNA renderers ({timeout}s timeout)...")

    # Sonos-aware: hides bonded stereo pair members/HT satellites and shows
    # room names instead of raw Sonos friendlyNames when a Sonos household
    # is present.
    devices = await discover_and_enrich(timeout=timeout)

    if json_output:
        output = {
            "devices": [
                {
                    "name": d.friendly_name,
                    "ip": d.ip,
                    "port": d.port,
                    "model": d.model_name,
                    "manufacturer": d.manufacturer,
                    "udn": d.udn,
                    "location": d.location,
                }
                for d in devices
            ],
            "count": len(devices),
        }
        print(json.dumps(output, indent=2))
    else:
        if not devices:
            print("\nNo DLNA renderers found.")
            print("\nTroubleshooting tips:")
            print("  - Ensure your DLNA device is powered on and connected")
            print("  - Try increasing timeout with --timeout 10")
            print("  - Check that your device supports UPnP/DLNA")
            return EXIT_SUCCESS

        print(f"\nFound {len(devices)} DLNA renderer(s):\n")

        for d in devices:
            print(f"  {d.friendly_name}")
            print(f"    IP: {d.ip}")
            print(f"    Port: {d.port}")
            if d.model_name:
                print(f"    Model: {d.model_name}")
            if d.manufacturer:
                print(f"    Manufacturer: {d.manufacturer}")
            print()

        # Show config example using first device
        first = devices[0]
        print("Config example (add to config.yaml):")
        print("  backend:")
        print("    dlna:")
        print(f'      ip: "{first.ip}"')
        print(f"      port: {first.port}")

    return EXIT_SUCCESS


# Quality ID -> display name, matching speaker.py's quality_names mapping
QUALITY_NAMES = {5: "MP3", 6: "CD (FLAC 16/44)", 7: "Hi-Res (24/96)", 27: "Hi-Res (24/192)"}


@dataclass
class _CoordinatorQuality:
    """Max streamable quality for one group's coordinator.

    ``advertised`` is what the device's own GetProtocolInfo Sink claims;
    ``effective`` is after known-device overrides (see
    apply_device_overrides in capabilities.py) are applied — i.e. what
    QobuzProxy would use out of the box, with the experimental
    hires_downsampling flag off (this diagnostic command has no config
    context to read it from, so every Sonos here shows the conservative
    16-bit/48kHz default regardless of model; enabling the flag in a real
    deployment unlocks 24-bit for most models — see
    SONOS_16BIT_ONLY_MODELS). They differ from ``advertised`` exactly when
    an override kicked in.
    """

    advertised: int
    effective: int
    confirmed: bool


async def _fetch_coordinator_quality(ip: str, port: int) -> Optional[_CoordinatorQuality]:
    """Query a group coordinator's max streamable quality via GetProtocolInfo.

    Returns None if the device couldn't be reached or queried.
    """
    from qobuz_proxy.backends.dlna.capabilities import (
        apply_device_overrides,
        parse_protocol_info_sink,
    )
    from qobuz_proxy.backends.dlna.client import DLNAClient

    client = DLNAClient(ip, port)
    try:
        device_info = await client.connect()
        sink = await client.get_protocol_info()
        if not sink:
            return None
        advertised = parse_protocol_info_sink(sink)
        effective = parse_protocol_info_sink(sink)
        apply_device_overrides(effective, device_info.manufacturer, device_info.model_name)
        return _CoordinatorQuality(
            advertised=advertised.max_quality,
            effective=effective.max_quality,
            confirmed=advertised.format_info_confirmed,
        )
    except Exception as e:
        logger.debug(f"Quality query failed for {ip}:{port}: {e}")
        return None
    finally:
        await client.disconnect()


async def run_discover_sonos(timeout: float, json_output: bool) -> int:
    """
    Discover Sonos players and show household rooms/groups.

    Runs plain SSDP discovery first (to find candidate IPs to query), then
    fetches the household's ZoneGroupTopology from whichever Sonos device
    answers first — this shows every room, marks bonded stereo
    pairs/invisible members, and shows current dynamic groups with each
    group's coordinator (the player that must be targeted to control
    playback for the whole group).

    Args:
        timeout: SSDP discovery timeout in seconds
        json_output: Output as JSON if True

    Returns:
        Exit code
    """
    from qobuz_proxy.backends.dlna.discovery import discover_dlna_devices
    from qobuz_proxy.backends.dlna.sonos.topology import fetch_sonos_groups, fetch_sonos_topology

    if not json_output:
        print(f"Scanning for Sonos players ({timeout}s timeout)...")

    # A plain SSDP scan just needs to find *some* reachable Sonos IP to query
    # GetZoneGroupState on — that response then describes the whole household
    # (including members SSDP alone would filter out), so this doesn't need
    # to find every player.
    devices = await discover_dlna_devices(timeout=timeout)
    sonos_devices = [d for d in devices if "sonos" in d.manufacturer.lower()]

    members = await fetch_sonos_topology(sonos_devices)
    groups = await fetch_sonos_groups(sonos_devices)

    if not members or not groups:
        if json_output:
            print(json.dumps({"groups": [], "count": 0}, indent=2))
        else:
            print("\nNo Sonos household found.")
            print("\nTroubleshooting tips:")
            print("  - Ensure your Sonos players are powered on and connected")
            print("  - Try increasing timeout with --timeout 10")
        return EXIT_SUCCESS

    # Query each group's coordinator for its max streamable quality — that's
    # the only member whose format ceiling matters, since audio reaches the
    # whole group through it. Concurrent: each is its own SOAP round trip.
    quality_by_coordinator: dict[str, Optional[_CoordinatorQuality]] = {}
    coordinators_to_query = {
        g.coordinator_uuid: members[g.coordinator_uuid]
        for g in groups
        if g.coordinator_uuid in members and members[g.coordinator_uuid].ip
    }
    if coordinators_to_query:
        results = await asyncio.gather(
            *[_fetch_coordinator_quality(m.ip, m.port) for m in coordinators_to_query.values()]
        )
        quality_by_coordinator = dict(zip(coordinators_to_query.keys(), results))

    if json_output:
        output = {
            "groups": [
                {
                    "group_id": g.group_id,
                    "coordinator_uuid": g.coordinator_uuid,
                    "coordinator_name": (
                        members[g.coordinator_uuid].zone_name
                        if g.coordinator_uuid in members
                        else ""
                    ),
                    "max_quality": (
                        {
                            "advertised": q.advertised,
                            "advertised_name": QUALITY_NAMES.get(q.advertised, str(q.advertised)),
                            "effective": q.effective,
                            "effective_name": QUALITY_NAMES.get(q.effective, str(q.effective)),
                            "confirmed": q.confirmed,
                        }
                        if (q := quality_by_coordinator.get(g.coordinator_uuid))
                        else None
                    ),
                    "members": [
                        {
                            "uuid": uuid,
                            "zone_name": member.zone_name,
                            "ip": member.ip,
                            "is_coordinator": uuid == g.coordinator_uuid,
                            "invisible": member.invisible,
                            "is_stereo_pair": member.is_stereo_pair,
                        }
                        for uuid in g.member_uuids
                        if (member := members.get(uuid)) is not None
                    ],
                }
                for g in groups
            ],
            "count": len(groups),
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"\nSonos household: {len(groups)} group(s), {len(members)} player(s):\n")
        for g in groups:
            coordinator_name = (
                members[g.coordinator_uuid].zone_name if g.coordinator_uuid in members else "?"
            )
            print(f"Group — coordinator: {coordinator_name}  [id: {g.group_id or '?'}]")

            quality = quality_by_coordinator.get(g.coordinator_uuid)
            if quality is None:
                print("  Max quality: unknown (coordinator unreachable)")
            elif quality.effective != quality.advertised:
                print(
                    f"  Max quality: {QUALITY_NAMES.get(quality.effective, quality.effective)}"
                    f" (device advertises {QUALITY_NAMES.get(quality.advertised, quality.advertised)}"
                    f", capped by a known-device override — see apply_device_overrides)"
                )
            else:
                confirmed_note = (
                    "" if quality.confirmed else " — not confirmed by device, conservative default"
                )
                print(
                    f"  Max quality: {QUALITY_NAMES.get(quality.effective, quality.effective)}"
                    f"{confirmed_note}"
                )

            for uuid in g.member_uuids:
                member = members.get(uuid)
                if member is None:
                    continue
                tags = []
                if uuid == g.coordinator_uuid:
                    tags.append("coordinator")
                if member.is_stereo_pair:
                    tags.append("stereo pair")
                if member.invisible:
                    tags.append("hidden — bonded/satellite")
                tag_str = f" ({', '.join(tags)})" if tags else ""
                ip = member.ip or "?"
                print(f"  • {member.zone_name or uuid:<20} {ip:<15}{tag_str}")
            print()

    return EXIT_SUCCESS


def run_list_audio_devices() -> int:
    """List available audio output devices."""
    try:
        from qobuz_proxy.backends.local.device import format_device_list, list_audio_devices
    except ImportError:
        print("Error: sounddevice not installed. Install with: pip install qobuz-proxy[local]")
        return EXIT_CONFIG_ERROR

    devices = list_audio_devices()
    if not devices:
        print("No audio output devices found.")
        return EXIT_SUCCESS

    print(f"Found {len(devices)} audio output device(s):\n")
    print(format_device_list(devices))
    print()
    print("Config example (add to config.yaml):")
    print("  backend:")
    print("    type: local")
    print("    local:")
    print(f'      device: "{devices[0].name}"')
    return EXIT_SUCCESS


def run_serve(args: argparse.Namespace) -> int:
    """
    Run the proxy server.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    # Setup basic logging first (will be reconfigured after config load)
    setup_logging("info")

    logger.info(f"qobuz-proxy {_version_string()}")

    try:
        # Load configuration
        cli_config = args_to_dict(args)
        config = load_config(args.config, cli_config)

        # Reconfigure logging with loaded level
        setup_logging(config.logging.level)

        log_config(config)

    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return EXIT_CONFIG_ERROR

    # Run the application
    try:
        app = QobuzProxy(config)
        asyncio.run(app.run())
        return EXIT_SUCCESS

    except BackendNotFoundError as e:
        logger.error(f"Backend error: {e}")
        return EXIT_NETWORK_ERROR

    except (ConnectionError, OSError) as e:
        logger.error(f"Network error: {e}")
        return EXIT_NETWORK_ERROR

    except KeyboardInterrupt:
        logger.info("Interrupted")
        return EXIT_SUCCESS

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return EXIT_NETWORK_ERROR


def main() -> int:
    """
    Main entry point.

    Returns:
        Exit code: 0=success, 1=config error, 2=auth error, 3=network error
    """
    args = parse_args()

    if args.discover:
        return asyncio.run(run_discovery(args.timeout, args.json_output))
    elif args.discover_sonos:
        return asyncio.run(run_discover_sonos(args.timeout, args.json_output))
    elif args.list_audio_devices:
        return run_list_audio_devices()
    else:
        return run_serve(args)


if __name__ == "__main__":
    sys.exit(main())

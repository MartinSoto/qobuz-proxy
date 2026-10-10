"""
QobuzProxy Configuration System.

Priority order (highest to lowest):
1. Command-line arguments
2. Environment variables
3. Configuration file (YAML)
4. Default values
"""

import logging
import os
import platform
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


# Valid quality values (0 = auto-detect from device capabilities)
AUTO_QUALITY = 0
AUTO_FALLBACK_QUALITY = 6  # CD quality fallback when auto-detection fails
VALID_QUALITIES = {0, 5, 6, 7, 27}

# Default ports
DEFAULT_HTTP_PORT = 8689
DEFAULT_PROXY_PORT = 7120

# Namespace UUID for deterministic speaker UUID generation
_SPEAKER_UUID_NAMESPACE = uuid.UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

# Valid log levels
VALID_LOG_LEVELS = {"debug", "info", "warning", "error"}

# Environment variable mappings
ENV_MAPPINGS = {
    # Qobuz
    "QOBUZ_EMAIL": ("qobuz", "email"),
    "QOBUZ_AUTH_TOKEN": ("qobuz", "auth_token"),
    "QOBUZ_USER_ID": ("qobuz", "user_id"),
    "QOBUZ_PASSWORD": ("qobuz", "auth_token"),  # Deprecated alias
    "QOBUZ_MAX_QUALITY": ("qobuz", "max_quality"),
    # Device
    "QOBUZPROXY_DEVICE_NAME": ("device", "name"),
    # DLNA
    "QOBUZPROXY_DLNA_IP": ("backend", "dlna", "ip"),
    "QOBUZPROXY_DLNA_PORT": ("backend", "dlna", "port"),
    "QOBUZPROXY_DLNA_FIXED_VOLUME": ("backend", "dlna", "fixed_volume"),
    "QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING": ("backend", "dlna", "hires_downsampling"),
    "QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING_GAIN": ("backend", "dlna", "hires_downsampling_gain"),
    # Backend type
    "QOBUZPROXY_BACKEND": ("backend", "type"),
    # Local audio
    "QOBUZPROXY_AUDIO_DEVICE": ("backend", "local", "device"),
    "QOBUZPROXY_AUDIO_BUFFER_SIZE": ("backend", "local", "buffer_size"),
    # Server
    "QOBUZPROXY_HTTP_PORT": ("server", "http_port"),
    "QOBUZPROXY_PROXY_PORT": ("backend", "dlna", "proxy_port"),
    # Logging
    "QOBUZPROXY_LOG_LEVEL": ("logging", "level"),
    # Sonos auto-discovery (replaces the `speakers` list when enabled)
    "QOBUZPROXY_SONOS_AUTO_DISCOVER": ("sonos_auto_discover",),
    # Hand the whole Qobuz queue to Sonos devices (experimental)
    "QOBUZPROXY_SONOS_QUEUE_HANDOVER": ("sonos_queue_handover",),
}


class ConfigError(Exception):
    """Configuration error."""

    pass


@dataclass
class QobuzConfig:
    """Qobuz account configuration."""

    email: str = ""
    auth_token: str = ""
    user_id: str = ""
    max_quality: int = 27  # 5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k


@dataclass
class DeviceConfig:
    """Device identification configuration."""

    name: str = "QobuzProxy"
    uuid: str = ""  # Auto-generated if empty

    def __post_init__(self) -> None:
        if not self.uuid:
            self.uuid = str(uuid.uuid4())


@dataclass
class DLNAConfig:
    """DLNA backend configuration."""

    ip: str = ""
    port: int = 1400
    fixed_volume: bool = False
    proxy_port: int = 7120
    description_url: str = ""  # Full URL to UPnP device description XML (auto-discovered via SSDP)
    # Experimental, opt-in: Hi-Res quality requests + on-the-fly downsampling
    # for devices with real 24-bit support below the 96k tier — see
    # DLNABackend.__init__ and apply_device_overrides. False keeps the old,
    # conservative behavior.
    hires_downsampling: bool = False
    # Experimental, opt-in, Sonos only: route every track through the
    # decode pipeline above (not just ones exceeding the device's cap) so
    # playback gain (track, or an approximated album gain — see
    # backends/dlna/sonos/gain.py) can be applied consistently. Implies
    # hires_downsampling — see build_speaker_configs.
    hires_downsampling_gain: bool = False
    # Experimental, Sonos only: publish the whole Qobuz queue to the device and
    # let it play through it (see SonosQueueBackend). Set from the global
    # sonos_queue_handover setting for every speaker.
    queue_handover: bool = False


@dataclass
class LocalConfig:
    """Local audio backend configuration."""

    device: str = "default"  # Device name, index, or "default"
    buffer_size: int = 2048  # Audio buffer size in frames


@dataclass
class BackendConfig:
    """Audio backend configuration."""

    type: str = "dlna"
    dlna: DLNAConfig = field(default_factory=DLNAConfig)
    local: LocalConfig = field(default_factory=LocalConfig)


@dataclass
class ServerConfig:
    """Server configuration."""

    http_port: int = 8689
    bind_address: str = "0.0.0.0"


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "info"


@dataclass
class SpeakerConfig:
    """Per-speaker configuration for multi-speaker mode."""

    name: str = "QobuzProxy"
    uuid: str = ""
    backend_type: str = "dlna"
    max_quality: int = 27
    http_port: int = 0  # 0 = auto-assign
    bind_address: str = "0.0.0.0"
    dlna_ip: str = ""
    dlna_port: int = 1400
    dlna_fixed_volume: bool = False
    dlna_hires_downsampling: bool = False
    dlna_hires_downsampling_gain: bool = False
    # Not persisted per speaker: it follows the global sonos_queue_handover.
    dlna_queue_handover: bool = False
    dlna_description_url: str = ""
    proxy_port: int = 0  # 0 = auto-assign
    audio_device: str = "default"
    audio_buffer_size: int = 2048
    # True for speakers created by an auto-discovery mechanism (e.g. Sonos
    # household discovery) rather than user config. Never appended to
    # Config.speakers / persisted to config.yaml — they're re-derived from
    # the network instead.
    auto_managed: bool = False


@dataclass
class Config:
    """Complete QobuzProxy configuration."""

    qobuz: QobuzConfig = field(default_factory=QobuzConfig)
    device: DeviceConfig = field(default_factory=DeviceConfig)
    backend: BackendConfig = field(default_factory=BackendConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    speakers: list[SpeakerConfig] = field(default_factory=list)
    # When True, speakers come from continuous Sonos household discovery
    # instead of the `speakers` list above (which is ignored, with a
    # warning, if also set) — no manual per-room configuration needed.
    sonos_auto_discover: bool = False
    # Experimental: hand the whole Qobuz queue to Sonos devices instead of
    # sequencing it track by track (docs/sonos-queue-handover-design.md).
    sonos_queue_handover: bool = False
    config_path: Optional[Path] = None  # Set by load_config() when loading from file


def validate_email(email: str) -> bool:
    """Validate email format."""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def validate_port(port: int) -> bool:
    """Validate port number."""
    return 1 <= port <= 65535


def validate_config(config: Config) -> None:
    """
    Validate configuration.

    Raises:
        ConfigError: If configuration is invalid
    """
    errors = []

    # Qobuz credentials (optional — token-based auth may provide them later)
    if config.qobuz.email and not validate_email(config.qobuz.email):
        errors.append(f"Invalid email format: {config.qobuz.email}")

    if config.qobuz.max_quality not in VALID_QUALITIES:
        errors.append(
            f"Invalid max_quality: {config.qobuz.max_quality}. "
            f"Valid values: {sorted(VALID_QUALITIES)}"
        )

    # Backend
    if config.backend.type == "dlna":
        if not config.backend.dlna.ip:
            errors.append("DLNA IP address is required when backend type is 'dlna'")
        if not validate_port(config.backend.dlna.port):
            errors.append(f"Invalid DLNA port: {config.backend.dlna.port}")
        if not validate_port(config.backend.dlna.proxy_port):
            errors.append(f"Invalid proxy port: {config.backend.dlna.proxy_port}")
    elif config.backend.type == "local":
        if not (64 <= config.backend.local.buffer_size <= 16384):
            errors.append(
                f"Invalid buffer_size: {config.backend.local.buffer_size}. "
                f"Must be between 64 and 16384"
            )
    elif config.backend.type != "stub":
        errors.append(f"Unknown backend type: {config.backend.type}")

    # Server ports
    if not validate_port(config.server.http_port):
        errors.append(f"Invalid HTTP port: {config.server.http_port}")

    # Logging
    if config.logging.level.lower() not in VALID_LOG_LEVELS:
        errors.append(
            f"Invalid log level: {config.logging.level}. Valid values: {sorted(VALID_LOG_LEVELS)}"
        )

    if errors:
        raise ConfigError("Configuration validation failed:\n  - " + "\n  - ".join(errors))


def generate_speaker_uuid(speaker_name: str) -> str:
    """Generate a deterministic UUID for a speaker based on hostname and name."""
    return str(uuid.uuid5(_SPEAKER_UUID_NAMESPACE, f"{platform.node()}:{speaker_name}"))


def generate_sonos_speaker_uuid(sonos_identity_key: str) -> str:
    """Generate a deterministic UUID for an auto-discovered Sonos speaker.

    Keyed by a stable Sonos-side identifier — a group's `group_id` (a
    coordinator handoff keeps the same group_id, confirmed against a real
    household, so the promoted coordinator's Speaker computes the same
    device identity its predecessor had) or, failing that, a player's own
    RINCON uuid — rather than `hostname:name` like generate_speaker_uuid.
    Either way this must survive DHCP IP changes and renames, neither of
    which affect the input.
    """
    return str(uuid.uuid5(_SPEAKER_UUID_NAMESPACE, f"sonos:{sonos_identity_key}"))


def slugify_name(name: str) -> str:
    """Convert a speaker name to a URL-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def speaker_config_to_dict(sc: SpeakerConfig) -> dict:
    """Convert a SpeakerConfig to a YAML-serializable dict.

    Auto-assigned fields (uuid, http_port, proxy_port) are omitted so the
    config file stays clean and ports can be re-assigned on next startup.
    """
    d: dict = {
        "name": sc.name,
        "backend": sc.backend_type,
        "max_quality": "auto" if sc.max_quality == AUTO_QUALITY else sc.max_quality,
    }
    if sc.backend_type == "dlna":
        d["dlna_ip"] = sc.dlna_ip
        d["dlna_port"] = sc.dlna_port
        d["dlna_fixed_volume"] = sc.dlna_fixed_volume
        d["dlna_hires_downsampling"] = sc.dlna_hires_downsampling
        d["dlna_hires_downsampling_gain"] = sc.dlna_hires_downsampling_gain
        if sc.dlna_description_url:
            d["dlna_description_url"] = sc.dlna_description_url
    elif sc.backend_type == "local":
        d["audio_device"] = sc.audio_device
        d["audio_buffer_size"] = sc.audio_buffer_size
    return d


def _single_speaker_from_config(config: Config) -> SpeakerConfig:
    """Map flat Config fields to a single SpeakerConfig."""
    return SpeakerConfig(
        name=config.device.name,
        uuid=config.device.uuid,
        backend_type=config.backend.type,
        max_quality=config.qobuz.max_quality,
        http_port=0,  # auto-assigned by _assign_ports (avoids conflict with web UI port)
        bind_address=config.server.bind_address,
        dlna_ip=config.backend.dlna.ip,
        dlna_port=config.backend.dlna.port,
        dlna_fixed_volume=config.backend.dlna.fixed_volume,
        dlna_hires_downsampling=config.backend.dlna.hires_downsampling,
        dlna_hires_downsampling_gain=config.backend.dlna.hires_downsampling_gain,
        dlna_description_url=config.backend.dlna.description_url,
        proxy_port=config.backend.dlna.proxy_port,
        audio_device=config.backend.local.device,
        audio_buffer_size=config.backend.local.buffer_size,
    )


def _assign_ports(speakers: list[SpeakerConfig], webui_port: int = DEFAULT_HTTP_PORT) -> None:
    """Auto-assign http_port and proxy_port to speakers that have 0 (auto)."""
    used_http: set[int] = {s.http_port for s in speakers if s.http_port != 0}
    # Reserve the web UI port so speakers don't collide with it
    used_http.add(webui_port)
    used_proxy: set[int] = {s.proxy_port for s in speakers if s.proxy_port != 0}

    next_http = DEFAULT_HTTP_PORT
    next_proxy = DEFAULT_PROXY_PORT

    for speaker in speakers:
        if speaker.http_port == 0:
            while next_http in used_http:
                next_http += 1
            speaker.http_port = next_http
            used_http.add(next_http)
            next_http += 1

        if speaker.backend_type == "dlna" and speaker.proxy_port == 0:
            while next_proxy in used_proxy:
                next_proxy += 1
            speaker.proxy_port = next_proxy
            used_proxy.add(next_proxy)
            next_proxy += 1


def _generate_uuids(speakers: list[SpeakerConfig]) -> None:
    """Generate deterministic UUIDs for speakers that have an empty uuid."""
    for speaker in speakers:
        if not speaker.uuid:
            speaker.uuid = generate_speaker_uuid(speaker.name)


def _validate_speakers(speakers: list[SpeakerConfig]) -> None:
    """
    Validate speaker list.

    Raises:
        ConfigError: If speakers list is empty, has duplicate names, or port conflicts.
    """
    if not speakers:
        return  # Empty is valid — speakers can be added at runtime via the web UI

    names = [s.name for s in speakers]
    if len(names) != len(set(names)):
        seen: set[str] = set()
        dupes: list[str] = []
        for n in names:
            if n in seen:
                dupes.append(n)
            else:
                seen.add(n)
        raise ConfigError(f"Duplicate speaker names: {dupes}")

    http_ports = [s.http_port for s in speakers]
    if len(http_ports) != len(set(http_ports)):
        raise ConfigError(f"HTTP port conflicts among speakers: {http_ports}")

    proxy_ports = [s.proxy_port for s in speakers if s.backend_type == "dlna" and s.proxy_port]
    if len(proxy_ports) != len(set(proxy_ports)):
        raise ConfigError(f"Proxy port conflicts among speakers: {proxy_ports}")

    # Per-speaker field validation
    errors = []
    for s in speakers:
        if s.backend_type == "dlna" and not s.dlna_ip:
            errors.append(f"Speaker '{s.name}': DLNA IP address is required")
        if s.backend_type not in ("dlna", "local", "stub"):
            errors.append(f"Speaker '{s.name}': unknown backend type '{s.backend_type}'")
        if s.http_port and not validate_port(s.http_port):
            errors.append(f"Speaker '{s.name}': invalid HTTP port {s.http_port}")
        if s.proxy_port and not validate_port(s.proxy_port):
            errors.append(f"Speaker '{s.name}': invalid proxy port {s.proxy_port}")
    if errors:
        raise ConfigError("Speaker validation failed:\n  - " + "\n  - ".join(errors))


def _parse_quality_value(value: Any) -> int:
    """Parse a quality value, handling 'auto' string -> AUTO_QUALITY (0)."""
    if isinstance(value, str) and value.lower() == "auto":
        return AUTO_QUALITY
    return int(value)


def _parse_yaml_speakers(raw_speakers: list[dict], config: Config) -> list[SpeakerConfig]:
    """Parse a list of raw YAML dicts into SpeakerConfig objects."""
    speakers = []
    for raw in raw_speakers:
        speaker = SpeakerConfig(
            name=raw.get("name", "QobuzProxy"),
            uuid=raw.get("uuid", ""),
            backend_type=raw.get("backend", "dlna"),
            max_quality=_parse_quality_value(raw.get("max_quality", 27)),
            http_port=int(raw.get("http_port", 0)),
            bind_address=raw.get("bind_address", config.server.bind_address),
            dlna_ip=raw.get("dlna_ip", ""),
            dlna_port=int(raw.get("dlna_port", 1400)),
            dlna_fixed_volume=bool(raw.get("dlna_fixed_volume", False)),
            dlna_hires_downsampling=bool(raw.get("dlna_hires_downsampling", False)),
            dlna_hires_downsampling_gain=bool(raw.get("dlna_hires_downsampling_gain", False)),
            dlna_description_url=raw.get("dlna_description_url", ""),
            proxy_port=int(raw.get("proxy_port", 0)),
            audio_device=raw.get("audio_device", "default"),
            audio_buffer_size=int(raw.get("audio_buffer_size", 2048)),
        )
        speakers.append(speaker)
    return speakers


def _split_env(var: str) -> list[str]:
    """Split a comma-separated environment variable value into a list of strings."""
    value = os.environ.get(var, "")
    if not value:
        return []
    return [v.strip() for v in value.split(",")]


def _split_env_padded(var: str, count: int, default: str) -> list[str]:
    """
    Split env var, broadcasting a single value to count items.

    Raises:
        ConfigError: If length is not 1 and not count.
    """
    values = _split_env(var)
    if not values:
        return [default] * count
    if len(values) == 1:
        return values * count
    if len(values) != count:
        raise ConfigError(f"Env var {var} has {len(values)} values but expected 1 or {count}")
    return values


def _parse_env_speakers(config: Config) -> list[SpeakerConfig]:
    """Parse multi-speaker configuration from environment variables."""
    names = _split_env("QOBUZPROXY_DEVICE_NAME")
    if not names:
        return []

    count = len(names)

    backend_types = _split_env_padded("QOBUZPROXY_BACKEND", count, "dlna")
    dlna_ips = _split_env_padded("QOBUZPROXY_DLNA_IP", count, "")
    dlna_ports_raw = _split_env_padded("QOBUZPROXY_DLNA_PORT", count, "1400")
    dlna_fixed_volumes_raw = _split_env_padded("QOBUZPROXY_DLNA_FIXED_VOLUME", count, "false")
    dlna_hires_downsampling_raw = _split_env_padded(
        "QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING", count, "false"
    )
    dlna_hires_downsampling_gain_raw = _split_env_padded(
        "QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING_GAIN", count, "false"
    )
    http_ports_raw = _split_env_padded("QOBUZPROXY_HTTP_PORT", count, "0")
    proxy_ports_raw = _split_env_padded("QOBUZPROXY_PROXY_PORT", count, "0")
    audio_devices = _split_env_padded("QOBUZPROXY_AUDIO_DEVICE", count, "default")
    audio_buffer_sizes_raw = _split_env_padded("QOBUZPROXY_AUDIO_BUFFER_SIZE", count, "2048")
    qualities_raw = _split_env_padded("QOBUZ_MAX_QUALITY", count, "27")

    speakers = []
    for i, name in enumerate(names):
        speaker = SpeakerConfig(
            name=name,
            backend_type=backend_types[i],
            max_quality=_parse_quality_value(qualities_raw[i]),
            http_port=int(http_ports_raw[i]),
            bind_address=config.server.bind_address,
            dlna_ip=dlna_ips[i],
            dlna_port=int(dlna_ports_raw[i]),
            dlna_fixed_volume=dlna_fixed_volumes_raw[i].lower() in ("true", "1", "yes", "on"),
            dlna_hires_downsampling=dlna_hires_downsampling_raw[i].lower()
            in ("true", "1", "yes", "on"),
            dlna_hires_downsampling_gain=dlna_hires_downsampling_gain_raw[i].lower()
            in ("true", "1", "yes", "on"),
            proxy_port=int(proxy_ports_raw[i]),
            audio_device=audio_devices[i],
            audio_buffer_size=int(audio_buffer_sizes_raw[i]),
        )
        speakers.append(speaker)

    return speakers


def build_speaker_configs(
    config: Config, raw_yaml_speakers: Optional[list[dict]] = None
) -> list[SpeakerConfig]:
    """
    Build speaker configs from YAML, env vars, or flat config.

    Priority: YAML speakers > comma-separated env vars > single flat config.
    After building, assigns ports, generates UUIDs, and validates.
    """
    speakers: list[SpeakerConfig] = []

    if raw_yaml_speakers is not None:
        speakers = _parse_yaml_speakers(raw_yaml_speakers, config)
    else:
        env_speakers = _parse_env_speakers(config)
        if env_speakers:
            speakers = env_speakers
        else:
            # Only create a speaker from flat config if a backend is actually configured
            has_backend = config.backend.dlna.ip or config.backend.type == "local"
            if has_backend:
                speakers = [_single_speaker_from_config(config)]

    _assign_ports(speakers, webui_port=config.server.http_port)
    _generate_uuids(speakers)
    _validate_speakers(speakers)

    return speakers


def load_yaml_config(path: Path) -> dict:
    """
    Load configuration from YAML file.

    Args:
        path: Path to YAML file

    Returns:
        Configuration dictionary

    Raises:
        ConfigError: If file cannot be read or parsed
    """
    if not path.exists():
        logger.debug(f"Config file not found: {path}")
        return {}

    try:
        with open(path) as f:
            data = yaml.safe_load(f)
            return data if data else {}
    except yaml.YAMLError as e:
        raise ConfigError(f"Error parsing YAML config: {e}")
    except IOError as e:
        raise ConfigError(f"Error reading config file: {e}")


def _set_nested(d: dict, path: tuple, value: Any) -> None:
    """Set a nested dictionary value using a path tuple."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


def _get_nested(d: dict, path: tuple) -> Any:
    """Read a nested dictionary value using a path tuple, or None if any
    key along the way is missing."""
    for key in path:
        if not isinstance(d, dict) or key not in d:
            return None
        d = d[key]
    return d


def load_env_config() -> dict:
    """
    Load configuration from environment variables.

    Returns:
        Configuration dictionary with values from environment
    """
    result: dict = {}

    for env_var, path in ENV_MAPPINGS.items():
        value: Any = os.environ.get(env_var)
        if value is not None:
            # Handle max_quality specially to support "auto"
            if env_var == "QOBUZ_MAX_QUALITY":
                if value.lower() == "auto":
                    value = AUTO_QUALITY
                else:
                    try:
                        value = int(value)
                    except ValueError:
                        logger.warning(f"Invalid value for {env_var}: {value}")
                        continue
            # Convert other numeric values
            elif env_var in (
                "QOBUZPROXY_DLNA_PORT",
                "QOBUZPROXY_HTTP_PORT",
                "QOBUZPROXY_PROXY_PORT",
                "QOBUZPROXY_AUDIO_BUFFER_SIZE",
            ):
                try:
                    value = int(value)
                except ValueError:
                    logger.warning(f"Invalid integer for {env_var}: {value}")
                    continue
            # Convert boolean values
            elif env_var in (
                "QOBUZPROXY_DLNA_FIXED_VOLUME",
                "QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING",
                "QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING_GAIN",
                "QOBUZPROXY_SONOS_AUTO_DISCOVER",
                "QOBUZPROXY_SONOS_QUEUE_HANDOVER",
            ):
                value = value.lower() in ("true", "1", "yes", "on")

            # Set nested value
            _set_nested(result, path, value)

    return result


def _deep_merge(base: dict, override: dict) -> None:
    """Recursively merge override into base."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def merge_configs(*configs: dict) -> dict:
    """
    Deep merge multiple configuration dictionaries.
    Later configs override earlier ones.
    """
    result: dict = {}
    for config in configs:
        _deep_merge(result, config)
    return result


def dict_to_config(d: dict) -> Config:
    """Convert a dictionary to Config dataclass."""
    config = Config()

    # Qobuz
    if "qobuz" in d:
        q = d["qobuz"]
        config.qobuz.email = q.get("email", config.qobuz.email)
        config.qobuz.auth_token = q.get("auth_token", config.qobuz.auth_token)
        config.qobuz.user_id = q.get("user_id", config.qobuz.user_id)

        # Accept "password" as deprecated alias for "auth_token"
        if "password" in q and "auth_token" not in q:
            logger.warning(
                "Config key 'password' is deprecated and will be removed "
                "in a future release. Use 'auth_token' instead."
            )
            config.qobuz.auth_token = q["password"]

        max_quality = q.get("max_quality", config.qobuz.max_quality)
        # Handle "auto" string from YAML
        if isinstance(max_quality, str) and max_quality.lower() == "auto":
            config.qobuz.max_quality = AUTO_QUALITY
        else:
            config.qobuz.max_quality = max_quality

    # Device
    if "device" in d:
        dev = d["device"]
        config.device.name = dev.get("name", config.device.name)
        if dev.get("uuid"):
            config.device.uuid = dev["uuid"]

    # Backend
    if "backend" in d:
        b = d["backend"]
        config.backend.type = b.get("type", config.backend.type)
        if "dlna" in b:
            dlna = b["dlna"]
            config.backend.dlna.ip = dlna.get("ip", config.backend.dlna.ip)
            config.backend.dlna.port = dlna.get("port", config.backend.dlna.port)
            config.backend.dlna.fixed_volume = dlna.get(
                "fixed_volume", config.backend.dlna.fixed_volume
            )
            config.backend.dlna.hires_downsampling = dlna.get(
                "hires_downsampling", config.backend.dlna.hires_downsampling
            )
            config.backend.dlna.hires_downsampling_gain = dlna.get(
                "hires_downsampling_gain", config.backend.dlna.hires_downsampling_gain
            )
            config.backend.dlna.proxy_port = dlna.get("proxy_port", config.backend.dlna.proxy_port)
            config.backend.dlna.description_url = dlna.get(
                "description_url", config.backend.dlna.description_url
            )
        if "local" in b:
            local = b["local"]
            config.backend.local.device = local.get("device", config.backend.local.device)
            config.backend.local.buffer_size = local.get(
                "buffer_size", config.backend.local.buffer_size
            )

    # Server
    if "server" in d:
        s = d["server"]
        config.server.http_port = s.get("http_port", config.server.http_port)
        config.server.bind_address = s.get("bind_address", config.server.bind_address)

    # Logging
    if "logging" in d:
        config.logging.level = d["logging"].get("level", config.logging.level)

    # Sonos auto-discovery
    if "sonos_auto_discover" in d:
        config.sonos_auto_discover = bool(d["sonos_auto_discover"])
    if "sonos_queue_handover" in d:
        config.sonos_queue_handover = bool(d["sonos_queue_handover"])

    return config


def load_config(
    config_path: Optional[Path] = None,
    cli_args: Optional[dict] = None,
) -> Config:
    """
    Load configuration from all sources.

    Priority (highest to lowest):
    1. CLI arguments
    2. Environment variables
    3. Config file
    4. Defaults

    Args:
        config_path: Path to YAML config file
        cli_args: Dictionary of CLI arguments

    Returns:
        Merged Config object

    Raises:
        ConfigError: If configuration is invalid
    """
    # Start with empty dict (defaults come from dataclasses)
    configs = []
    raw_yaml_speakers: Optional[list[dict]] = None

    # Resolve config path: explicit > ./config.yaml > $QOBUZPROXY_DATA_DIR/config.yaml
    if config_path is None:
        local = Path("./config.yaml")
        if local.is_file():
            config_path = local
        else:
            data_dir = os.environ.get("QOBUZPROXY_DATA_DIR")
            if data_dir:
                candidate = Path(data_dir) / "config.yaml"
                if candidate.is_file():
                    config_path = candidate

    # 1. Load from file (lowest priority of explicit configs)
    if config_path:
        file_config = load_yaml_config(config_path)
        if file_config:
            # Extract speakers key before merging into flat config
            raw_yaml_speakers = file_config.pop("speakers", None)
            configs.append(file_config)
            logger.debug(f"Loaded config from {config_path}")

    # 2. Load from environment
    env_config = load_env_config()
    if env_config:
        configs.append(env_config)
        logger.debug("Loaded config from environment variables")

    # 3. Load from CLI (highest priority)
    if cli_args:
        configs.append(cli_args)
        logger.debug("Loaded config from CLI arguments")

    # Merge all configs
    merged = merge_configs(*configs) if configs else {}

    # Convert to Config object (fills in defaults)
    config = dict_to_config(merged)

    # Remember where to persist config changes (for web UI speaker management).
    # When no file exists, prefer $QOBUZPROXY_DATA_DIR/config.yaml so Docker
    # users — whose working directory is not writable — get saves into the
    # mounted /data volume alongside credentials.json.
    if config_path:
        config.config_path = config_path
    else:
        data_dir = os.environ.get("QOBUZPROXY_DATA_DIR")
        if data_dir:
            config.config_path = Path(data_dir) / "config.yaml"
        else:
            config.config_path = Path("./config.yaml")

    # Build speaker configs
    config.speakers = build_speaker_configs(config, raw_yaml_speakers)

    # Queue handover is one switch for the whole process: every speaker follows it.
    for sc in config.speakers:
        sc.dlna_queue_handover = config.sonos_queue_handover

    # --hires-downsampling-gain on the CLI is a blanket override: unlike
    # the field's own per-speaker YAML/env forms, passing it on the
    # command line turns it on for every speaker regardless of their
    # individual config — see cli.py's --hires-downsampling-gain help.
    if cli_args and _get_nested(cli_args, ("backend", "dlna", "hires_downsampling_gain")):
        for sc in config.speakers:
            sc.dlna_hires_downsampling_gain = True

    # hires_downsampling_gain only does anything through the downsampling
    # pipeline it forces every track through — implicitly enable that too
    # rather than have gain silently do nothing for someone who set only
    # the gain flag.
    for sc in config.speakers:
        if sc.dlna_hires_downsampling_gain and not sc.dlna_hires_downsampling:
            logger.info(
                f"Speaker '{sc.name}': hires_downsampling_gain implies "
                "hires_downsampling; enabling it"
            )
            sc.dlna_hires_downsampling = True

    # Validate only for single-speaker (non-multi) mode; skip when no speakers
    # (empty config is valid — speakers can be added via the web UI)
    device_name_env = os.environ.get("QOBUZPROXY_DEVICE_NAME", "")
    is_multi = raw_yaml_speakers is not None or "," in device_name_env
    if not is_multi and config.speakers:
        validate_config(config)

    return config

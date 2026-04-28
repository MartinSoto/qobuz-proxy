"""Tests for SpeakerConfig and multi-speaker config normalization."""

import uuid
from pathlib import Path

import pytest

from qobuz_proxy.config import (
    AUTO_QUALITY,
    DEFAULT_HTTP_PORT,
    DEFAULT_PROXY_PORT,
    Config,
    ConfigError,
    SpeakerConfig,
    _assign_ports,
    _generate_uuids,
    _parse_env_speakers,
    _parse_yaml_speakers,
    _single_speaker_from_config,
    _split_env,
    _split_env_padded,
    _validate_speakers,
    build_speaker_configs,
    generate_speaker_uuid,
    load_config,
)


class TestSpeakerConfigDefaults:
    def test_default_values(self):
        s = SpeakerConfig()
        assert s.name == "QobuzProxy"
        assert s.uuid == ""
        assert s.backend_type == "dlna"
        assert s.max_quality == 27
        assert s.http_port == 0
        assert s.bind_address == "0.0.0.0"
        assert s.dlna_ip == ""
        assert s.dlna_port == 1400
        assert s.dlna_fixed_volume is False
        assert s.proxy_port == 0
        assert s.audio_device == "default"
        assert s.audio_buffer_size == 2048

    def test_custom_construction(self):
        s = SpeakerConfig(
            name="Living Room",
            backend_type="local",
            max_quality=6,
            http_port=8690,
            audio_device="hw:0",
        )
        assert s.name == "Living Room"
        assert s.backend_type == "local"
        assert s.max_quality == 6
        assert s.http_port == 8690
        assert s.audio_device == "hw:0"


class TestSingleSpeakerNormalization:
    def test_flat_dlna_config_to_speaker(self):
        config = Config()
        config.device.name = "My Speaker"
        config.device.uuid = "test-uuid"
        config.backend.type = "dlna"
        config.backend.dlna.ip = "192.168.1.100"
        config.backend.dlna.port = 1400
        config.backend.dlna.fixed_volume = True
        config.backend.dlna.proxy_port = 7120
        config.qobuz.max_quality = 7
        config.server.http_port = 8689
        config.server.bind_address = "192.168.1.1"

        speaker = _single_speaker_from_config(config)

        assert speaker.name == "My Speaker"
        assert speaker.uuid == "test-uuid"
        assert speaker.backend_type == "dlna"
        assert speaker.dlna_ip == "192.168.1.100"
        assert speaker.dlna_port == 1400
        assert speaker.dlna_fixed_volume is True
        assert speaker.proxy_port == 7120
        assert speaker.max_quality == 7
        assert speaker.http_port == 0  # auto-assigned by _assign_ports to avoid web UI conflict
        assert speaker.bind_address == "192.168.1.1"

    def test_flat_local_config_to_speaker(self):
        config = Config()
        config.device.name = "Local Speaker"
        config.backend.type = "local"
        config.backend.local.device = "hw:1"
        config.backend.local.buffer_size = 4096
        config.qobuz.max_quality = 6

        speaker = _single_speaker_from_config(config)

        assert speaker.backend_type == "local"
        assert speaker.audio_device == "hw:1"
        assert speaker.audio_buffer_size == 4096
        assert speaker.max_quality == 6


class TestYAMLSpeakersParsing:
    def test_two_dlna_speakers(self):
        config = Config()
        raw = [
            {"name": "Living Room", "backend": "dlna", "dlna_ip": "192.168.1.10"},
            {"name": "Kitchen", "backend": "dlna", "dlna_ip": "192.168.1.11"},
        ]
        speakers = _parse_yaml_speakers(raw, config)
        assert len(speakers) == 2
        assert speakers[0].name == "Living Room"
        assert speakers[0].dlna_ip == "192.168.1.10"
        assert speakers[1].name == "Kitchen"
        assert speakers[1].dlna_ip == "192.168.1.11"

    def test_auto_quality_string(self):
        config = Config()
        raw = [{"name": "Speaker", "max_quality": "auto"}]
        speakers = _parse_yaml_speakers(raw, config)
        assert speakers[0].max_quality == AUTO_QUALITY

    def test_mixed_backends(self):
        config = Config()
        raw = [
            {"name": "DLNA Speaker", "backend": "dlna", "dlna_ip": "10.0.0.1"},
            {"name": "Local Speaker", "backend": "local", "audio_device": "hw:0"},
        ]
        speakers = _parse_yaml_speakers(raw, config)
        assert speakers[0].backend_type == "dlna"
        assert speakers[1].backend_type == "local"
        assert speakers[1].audio_device == "hw:0"

    def test_explicit_ports_respected(self):
        config = Config()
        raw = [{"name": "Speaker", "http_port": 9000, "proxy_port": 8000}]
        speakers = _parse_yaml_speakers(raw, config)
        assert speakers[0].http_port == 9000
        assert speakers[0].proxy_port == 8000

    def test_explicit_uuid_preserved(self):
        config = Config()
        my_uuid = str(uuid.uuid4())
        raw = [{"name": "Speaker", "uuid": my_uuid}]
        speakers = _parse_yaml_speakers(raw, config)
        assert speakers[0].uuid == my_uuid

    def test_uses_config_bind_address_as_default(self):
        config = Config()
        config.server.bind_address = "10.0.0.5"
        raw = [{"name": "Speaker"}]
        speakers = _parse_yaml_speakers(raw, config)
        assert speakers[0].bind_address == "10.0.0.5"


class TestEnvVarSpeakersParsing:
    def test_comma_separated_names(self, monkeypatch):
        monkeypatch.setenv("QOBUZPROXY_DEVICE_NAME", "Room A,Room B")
        monkeypatch.setenv("QOBUZPROXY_DLNA_IP", "10.0.0.1,10.0.0.2")
        config = Config()
        speakers = _parse_env_speakers(config)
        assert len(speakers) == 2
        assert speakers[0].name == "Room A"
        assert speakers[0].dlna_ip == "10.0.0.1"
        assert speakers[1].name == "Room B"
        assert speakers[1].dlna_ip == "10.0.0.2"

    def test_single_backend_broadcasts(self, monkeypatch):
        monkeypatch.setenv("QOBUZPROXY_DEVICE_NAME", "A,B,C")
        monkeypatch.setenv("QOBUZPROXY_BACKEND", "dlna")
        config = Config()
        speakers = _parse_env_speakers(config)
        assert all(s.backend_type == "dlna" for s in speakers)

    def test_quality_with_auto(self, monkeypatch):
        monkeypatch.setenv("QOBUZPROXY_DEVICE_NAME", "X,Y")
        monkeypatch.setenv("QOBUZ_MAX_QUALITY", "auto")
        config = Config()
        speakers = _parse_env_speakers(config)
        assert all(s.max_quality == AUTO_QUALITY for s in speakers)

    def test_mismatched_lengths_raises(self, monkeypatch):
        monkeypatch.setenv("QOBUZPROXY_DEVICE_NAME", "A,B,C")
        monkeypatch.setenv("QOBUZPROXY_DLNA_IP", "10.0.0.1,10.0.0.2")
        config = Config()
        with pytest.raises(ConfigError, match="expected 1 or 3"):
            _parse_env_speakers(config)

    def test_no_env_returns_empty(self, monkeypatch):
        monkeypatch.delenv("QOBUZPROXY_DEVICE_NAME", raising=False)
        config = Config()
        speakers = _parse_env_speakers(config)
        assert speakers == []

    def test_split_env_empty(self, monkeypatch):
        monkeypatch.delenv("MY_VAR", raising=False)
        assert _split_env("MY_VAR") == []

    def test_split_env_padded_broadcasts(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "val")
        result = _split_env_padded("MY_VAR", 3, "default")
        assert result == ["val", "val", "val"]

    def test_split_env_padded_exact_count(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "a,b,c")
        result = _split_env_padded("MY_VAR", 3, "default")
        assert result == ["a", "b", "c"]

    def test_split_env_padded_wrong_count_raises(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "a,b")
        with pytest.raises(ConfigError):
            _split_env_padded("MY_VAR", 3, "default")


class TestPortAssignment:
    def test_auto_assign_from_defaults_three_speakers(self):
        speakers = [
            SpeakerConfig(name="A", backend_type="dlna"),
            SpeakerConfig(name="B", backend_type="dlna"),
            SpeakerConfig(name="C", backend_type="dlna"),
        ]
        _assign_ports(speakers)
        # Port 8689 is reserved for the web UI, speakers start at 8690
        assert speakers[0].http_port == DEFAULT_HTTP_PORT + 1
        assert speakers[1].http_port == DEFAULT_HTTP_PORT + 2
        assert speakers[2].http_port == DEFAULT_HTTP_PORT + 3
        assert speakers[0].proxy_port == DEFAULT_PROXY_PORT
        assert speakers[1].proxy_port == DEFAULT_PROXY_PORT + 1
        assert speakers[2].proxy_port == DEFAULT_PROXY_PORT + 2

    def test_skip_explicit_ports(self):
        speakers = [
            SpeakerConfig(name="A", backend_type="dlna", http_port=9000, proxy_port=8000),
            SpeakerConfig(name="B", backend_type="dlna"),
        ]
        _assign_ports(speakers)
        assert speakers[0].http_port == 9000
        assert speakers[0].proxy_port == 8000
        # Port 8689 is reserved for the web UI, auto-assign starts at 8690
        assert speakers[1].http_port == DEFAULT_HTTP_PORT + 1
        assert speakers[1].proxy_port == DEFAULT_PROXY_PORT

    def test_local_backend_gets_no_proxy_port(self):
        speakers = [SpeakerConfig(name="Local", backend_type="local")]
        _assign_ports(speakers)
        assert speakers[0].proxy_port == 0  # not assigned for local

    def test_explicit_ports_not_reused(self):
        # Speaker A takes DEFAULT_HTTP_PORT+1; Speaker B should get DEFAULT_HTTP_PORT+2
        speakers = [
            SpeakerConfig(name="A", backend_type="dlna", http_port=DEFAULT_HTTP_PORT + 1),
            SpeakerConfig(name="B", backend_type="dlna"),
        ]
        _assign_ports(speakers)
        assert speakers[1].http_port == DEFAULT_HTTP_PORT + 2


class TestUUIDGeneration:
    def test_deterministic(self):
        uuid1 = generate_speaker_uuid("My Speaker")
        uuid2 = generate_speaker_uuid("My Speaker")
        assert uuid1 == uuid2

    def test_different_names_different_uuids(self):
        uuid1 = generate_speaker_uuid("Speaker A")
        uuid2 = generate_speaker_uuid("Speaker B")
        assert uuid1 != uuid2

    def test_valid_uuid_format(self):
        result = generate_speaker_uuid("Test")
        # Should not raise
        uuid.UUID(result)

    def test_generate_uuids_fills_empty(self):
        speakers = [SpeakerConfig(name="A"), SpeakerConfig(name="B", uuid="existing-uuid")]
        _generate_uuids(speakers)
        assert speakers[0].uuid != ""
        assert speakers[1].uuid == "existing-uuid"


class TestSpeakerValidation:
    def test_duplicate_names_rejected(self):
        speakers = [
            SpeakerConfig(name="Same", http_port=8689, proxy_port=7120, dlna_ip="1.2.3.4"),
            SpeakerConfig(name="Same", http_port=8690, proxy_port=7121, dlna_ip="1.2.3.5"),
        ]
        with pytest.raises(ConfigError, match="Duplicate speaker names"):
            _validate_speakers(speakers)

    def test_http_port_conflict_rejected(self):
        speakers = [
            SpeakerConfig(name="A", http_port=8689, proxy_port=7120, dlna_ip="1.2.3.4"),
            SpeakerConfig(name="B", http_port=8689, proxy_port=7121, dlna_ip="1.2.3.5"),
        ]
        with pytest.raises(ConfigError, match="HTTP port conflicts"):
            _validate_speakers(speakers)

    def test_proxy_port_conflict_rejected(self):
        speakers = [
            SpeakerConfig(
                name="A", backend_type="dlna", http_port=8689, proxy_port=7120, dlna_ip="1.2.3.4"
            ),
            SpeakerConfig(
                name="B", backend_type="dlna", http_port=8690, proxy_port=7120, dlna_ip="1.2.3.5"
            ),
        ]
        with pytest.raises(ConfigError, match="Proxy port conflicts"):
            _validate_speakers(speakers)

    def test_empty_speakers_allowed(self):
        """Empty speakers list is valid — speakers can be added via the web UI."""
        _validate_speakers([])  # Should not raise

    def test_dlna_speaker_missing_ip_rejected(self):
        speakers = [
            SpeakerConfig(name="Bad", backend_type="dlna", http_port=8689, dlna_ip=""),
        ]
        with pytest.raises(ConfigError, match="DLNA IP address is required"):
            _validate_speakers(speakers)

    def test_unknown_backend_type_rejected(self):
        speakers = [
            SpeakerConfig(name="Bad", backend_type="bluetooth", http_port=8689),
        ]
        with pytest.raises(ConfigError, match="unknown backend type"):
            _validate_speakers(speakers)

    def test_valid_speakers_pass(self):
        speakers = [
            SpeakerConfig(name="A", http_port=8689, proxy_port=7120, dlna_ip="192.168.1.50"),
            SpeakerConfig(name="B", http_port=8690, proxy_port=7121, dlna_ip="192.168.1.51"),
        ]
        # Should not raise
        _validate_speakers(speakers)


class TestLoadConfigSpeakers:
    def test_yaml_speakers_key_extracted(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
qobuz:
  email: test@example.com
  password: secret
speakers:
  - name: Room A
    backend: dlna
    dlna_ip: 192.168.1.10
  - name: Room B
    backend: dlna
    dlna_ip: 192.168.1.11
""")
        config = load_config(config_path=config_file)
        assert len(config.speakers) == 2
        assert config.speakers[0].name == "Room A"
        assert config.speakers[1].name == "Room B"

    def test_flat_config_produces_one_speaker(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
qobuz:
  email: test@example.com
  password: secret
device:
  name: Single Speaker
backend:
  type: dlna
  dlna:
    ip: 192.168.1.50
""")
        config = load_config(config_path=config_file)
        assert len(config.speakers) == 1
        assert config.speakers[0].name == "Single Speaker"
        assert config.speakers[0].dlna_ip == "192.168.1.50"

    def test_build_speaker_configs_yaml_priority_over_env(self, monkeypatch):
        monkeypatch.setenv("QOBUZPROXY_DEVICE_NAME", "Env Speaker")
        config = Config()
        raw_yaml = [{"name": "YAML Speaker", "dlna_ip": "10.0.0.1"}]
        speakers = build_speaker_configs(config, raw_yaml_speakers=raw_yaml)
        assert len(speakers) == 1
        assert speakers[0].name == "YAML Speaker"

    def test_build_speaker_configs_env_over_flat(self, monkeypatch):
        monkeypatch.setenv("QOBUZPROXY_DEVICE_NAME", "Env A,Env B")
        monkeypatch.setenv("QOBUZPROXY_DLNA_IP", "10.0.0.1,10.0.0.2")
        config = Config()
        speakers = build_speaker_configs(config)
        assert len(speakers) == 2
        assert speakers[0].name == "Env A"
        assert speakers[1].name == "Env B"

    def test_build_speaker_configs_fallback_to_flat(self, monkeypatch):
        monkeypatch.delenv("QOBUZPROXY_DEVICE_NAME", raising=False)
        config = Config()
        config.device.name = "Flat Speaker"
        config.backend.dlna.ip = "192.168.1.50"
        config.server.http_port = 8689
        config.backend.dlna.proxy_port = 7120
        speakers = build_speaker_configs(config)
        assert len(speakers) == 1
        assert speakers[0].name == "Flat Speaker"


class TestConfigPathFallback:
    """When no config file exists, the writable fallback path must respect
    QOBUZPROXY_DATA_DIR. Otherwise Docker users (CWD=/app, /data mounted)
    silently lose every speaker change made via the web UI.
    """

    def test_data_dir_used_when_no_config_file(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        monkeypatch.setenv("QOBUZPROXY_DATA_DIR", str(data_dir))

        config = load_config()

        assert config.config_path == data_dir / "config.yaml"

    def test_local_cwd_used_when_no_data_dir(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("QOBUZPROXY_DATA_DIR", raising=False)

        config = load_config()

        assert config.config_path == Path("./config.yaml")

    def test_explicit_path_overrides_data_dir(self, monkeypatch, tmp_path):
        explicit = tmp_path / "custom.yaml"
        explicit.write_text("server:\n  http_port: 9000\n")
        monkeypatch.setenv("QOBUZPROXY_DATA_DIR", str(tmp_path / "data"))

        config = load_config(config_path=explicit)

        assert config.config_path == explicit

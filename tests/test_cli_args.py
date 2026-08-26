"""Tests for CLI/env-var configuration of hires_downsampling — this must
work without a config.yaml file at all, since a containerized deployment
(the actual reason this flag exists) configures purely via env vars or
CLI args passed to the entrypoint."""

import sys

from qobuz_proxy.cli import args_to_dict, parse_args
from qobuz_proxy.config import load_config


class TestHiresDownsamplingCliFlag:
    def test_flag_present_sets_it_true(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        monkeypatch.setattr(
            sys, "argv", ["qobuz-proxy", "--dlna-ip", "10.0.0.5", "--hires-downsampling"]
        )
        args = parse_args()

        d = args_to_dict(args)

        assert d["backend"]["dlna"]["hires_downsampling"] is True

    def test_flag_absent_is_not_set_at_all(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Matches --fixed-volume's own pattern: an absent store_true flag
        must not appear in the dict at all (so it doesn't clobber a value
        set by a lower-priority source — see args_to_dict's store_true_flags
        handling), not merely be present-and-False."""
        monkeypatch.setattr(sys, "argv", ["qobuz-proxy", "--dlna-ip", "10.0.0.5"])
        args = parse_args()

        d = args_to_dict(args)

        assert "hires_downsampling" not in d.get("backend", {}).get("dlna", {})


class TestHiresDownsamplingEnvVar:
    def test_env_var_alone_enables_it_without_any_config_file(self, monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
        monkeypatch.delenv("QOBUZPROXY_DATA_DIR", raising=False)
        monkeypatch.setenv("QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING", "true")
        monkeypatch.chdir(tmp_path)  # no ./config.yaml here

        config = load_config()

        assert config.backend.dlna.hires_downsampling is True

    def test_default_is_off_with_nothing_set(self, monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
        monkeypatch.delenv("QOBUZPROXY_DATA_DIR", raising=False)
        monkeypatch.delenv("QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING", raising=False)
        monkeypatch.chdir(tmp_path)

        config = load_config()

        assert config.backend.dlna.hires_downsampling is False

    def test_cli_flag_overrides_env_var(self, monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
        """CLI args are the highest-priority source (see load_config's
        docstring) — an env var of false must not survive a CLI flag."""
        monkeypatch.delenv("QOBUZPROXY_DATA_DIR", raising=False)
        monkeypatch.setenv("QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING", "false")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["qobuz-proxy", "--hires-downsampling"])

        args = parse_args()
        config = load_config(cli_args=args_to_dict(args))

        assert config.backend.dlna.hires_downsampling is True

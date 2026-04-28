"""Tests for version/commit detection."""

import subprocess
from unittest.mock import patch

import qobuz_proxy


def test_detect_commit_prefers_env_var(monkeypatch):
    monkeypatch.setenv("QOBUZPROXY_COMMIT", "abcdef1234567890")
    assert qobuz_proxy._detect_commit() == "abcdef1"


def test_detect_commit_truncates_long_env_value(monkeypatch):
    monkeypatch.setenv("QOBUZPROXY_COMMIT", "a" * 40)
    assert qobuz_proxy._detect_commit() == "a" * 7


def test_detect_commit_returns_empty_when_no_env_and_no_git(monkeypatch):
    monkeypatch.delenv("QOBUZPROXY_COMMIT", raising=False)
    with patch.object(subprocess, "run", side_effect=FileNotFoundError):
        assert qobuz_proxy._detect_commit() == ""


def test_detect_commit_handles_git_failure(monkeypatch):
    monkeypatch.delenv("QOBUZPROXY_COMMIT", raising=False)
    failed = subprocess.CompletedProcess(args=[], returncode=128, stdout="", stderr="fatal")
    with patch.object(subprocess, "run", return_value=failed):
        assert qobuz_proxy._detect_commit() == ""

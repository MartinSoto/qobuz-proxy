"""Shared fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _isolated_data_dir(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    """State the proxy keeps next to its credentials (the audio port registry)
    must never land in the real data directory while testing."""
    monkeypatch.setenv("QOBUZPROXY_DATA_DIR", str(tmp_path / "data"))

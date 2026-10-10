"""A room keeps its audio proxy port across restarts (see port_registry.py)."""

import json
from pathlib import Path

from qobuz_proxy.backends.dlna.sonos.port_registry import PortRegistry
from qobuz_proxy.config import DEFAULT_PROXY_PORT


class TestPorts:
    def test_the_first_device_gets_the_first_port(self, tmp_path: Path) -> None:
        assert PortRegistry(tmp_path / "p.json").port_for("A", []) == DEFAULT_PROXY_PORT

    def test_a_port_in_use_is_skipped(self, tmp_path: Path) -> None:
        registry = PortRegistry(tmp_path / "p.json")

        assert registry.port_for("A", [DEFAULT_PROXY_PORT]) == DEFAULT_PROXY_PORT + 1

    def test_the_same_device_gets_the_same_port_after_a_restart(self, tmp_path: Path) -> None:
        path = tmp_path / "p.json"
        first = PortRegistry(path)
        first.port_for("A", [])
        b = first.port_for("B", [DEFAULT_PROXY_PORT])
        c = first.port_for("C", [DEFAULT_PROXY_PORT, b])

        after = PortRegistry(path)  # a new process, other order, fewer rooms
        assert after.port_for("C", []) == c
        assert after.port_for("B", [c]) == b

    def test_a_port_promised_to_an_absent_device_is_not_given_away(self, tmp_path: Path) -> None:
        path = tmp_path / "p.json"
        first = PortRegistry(path)
        a = first.port_for("A", [])

        after = PortRegistry(path)  # A is not (yet) running
        assert after.port_for("B", []) != a

    def test_a_port_remembered_but_held_by_a_running_speaker_is_not_taken(
        self, tmp_path: Path
    ) -> None:
        registry = PortRegistry(tmp_path / "p.json")
        a = registry.port_for("A", [])

        assert registry.port_for("A", [a]) != a

    def test_a_missing_or_unreadable_file_is_just_empty(self, tmp_path: Path) -> None:
        bad = tmp_path / "p.json"
        bad.write_text("not json")

        assert PortRegistry(bad).port_for("A", []) == DEFAULT_PROXY_PORT
        assert PortRegistry(tmp_path / "missing.json").port_for("A", []) == DEFAULT_PROXY_PORT

    def test_it_is_saved_where_asked(self, tmp_path: Path) -> None:
        path = tmp_path / "sub" / "p.json"

        PortRegistry(path).port_for("A", [])

        assert json.loads(path.read_text()) == {"A": DEFAULT_PROXY_PORT}

    def test_without_a_path_it_only_remembers_in_memory(self) -> None:
        registry = PortRegistry(None)

        assert registry.port_for("A", []) == registry.port_for("A", [])

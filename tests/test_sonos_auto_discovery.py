"""Tests for wiring SonosDiscoveryManager into QobuzProxy's speaker lifecycle."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from qobuz_proxy.app import QobuzProxy
from qobuz_proxy.backends.dlna.sonos_discovery_manager import SonosRoom
from qobuz_proxy.config import Config, QobuzConfig, SpeakerConfig


def _make_config(sonos_auto_discover: bool = True, speakers: list | None = None) -> Config:
    config = Config()
    config.qobuz = QobuzConfig(email="test@example.com", auth_token="secret", user_id="12345")
    config.sonos_auto_discover = sonos_auto_discover
    config.speakers = speakers or []
    return config


def _mock_speaker(name: str = "Kitchen", starts: bool = True) -> MagicMock:
    speaker = MagicMock()
    speaker.start = AsyncMock(return_value=starts)
    speaker.stop = AsyncMock()
    speaker.name = name
    return speaker


ROOM = SonosRoom(uuid="RINCON_A", name="Kitchen", ip="10.0.1.30", port=1400, is_stereo_pair=False)


class TestStartSpeakersBranching:
    async def test_auto_discover_starts_discovery_manager_not_static_speakers(self) -> None:
        config = _make_config(sonos_auto_discover=True, speakers=[SpeakerConfig(name="Manual")])
        app = QobuzProxy(config)
        app._api_client = MagicMock()

        mock_manager = MagicMock()
        mock_manager.start = AsyncMock()

        with (
            patch("qobuz_proxy.app.Speaker") as MockSpeaker,
            patch("qobuz_proxy.app.SonosDiscoveryManager", return_value=mock_manager) as MockMgr,
        ):
            await app._start_speakers()

        MockMgr.assert_called_once()
        mock_manager.start.assert_awaited_once()
        MockSpeaker.assert_not_called()  # the static 'Manual' entry is ignored
        assert app._sonos_discovery is mock_manager

    async def test_disabled_uses_static_speakers_as_before(self) -> None:
        config = _make_config(sonos_auto_discover=False, speakers=[SpeakerConfig(name="Manual")])
        app = QobuzProxy(config)
        app._api_client = MagicMock()

        speaker = _mock_speaker("Manual")
        with (
            patch("qobuz_proxy.app.Speaker", return_value=speaker),
            patch("qobuz_proxy.app.SonosDiscoveryManager") as MockMgr,
        ):
            await app._start_speakers()

        MockMgr.assert_not_called()
        speaker.start.assert_awaited_once()
        assert app._speakers == [speaker]


class TestRoomFoundAndLost:
    async def test_room_found_starts_and_registers_speaker(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()

        speaker = _mock_speaker("Kitchen")
        with patch("qobuz_proxy.app.Speaker", return_value=speaker) as MockSpeaker:
            result = await app._on_sonos_room_found(ROOM)

        assert result is True
        assert app._speakers == [speaker]
        assert app._sonos_speakers_by_uuid["RINCON_A"] is speaker

        sc = MockSpeaker.call_args.kwargs["config"]
        assert sc.name == "Kitchen"
        assert sc.dlna_ip == "10.0.1.30"
        assert sc.dlna_port == 1400
        assert sc.auto_managed is True
        assert sc.uuid  # deterministic uuid was assigned

    async def test_room_found_returns_false_when_not_authenticated(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = None

        result = await app._on_sonos_room_found(ROOM)

        assert result is False
        assert app._speakers == []

    async def test_room_found_returns_false_on_name_collision(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()
        existing = _mock_speaker("Kitchen")
        app._speakers.append(existing)

        with patch("qobuz_proxy.app.Speaker") as MockSpeaker:
            result = await app._on_sonos_room_found(ROOM)

        assert result is False
        MockSpeaker.assert_not_called()
        assert app._speakers == [existing]  # unchanged

    async def test_room_found_returns_false_when_speaker_fails_to_start(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()
        speaker = _mock_speaker("Kitchen", starts=False)

        with patch("qobuz_proxy.app.Speaker", return_value=speaker):
            result = await app._on_sonos_room_found(ROOM)

        assert result is False
        assert app._speakers == []
        assert "RINCON_A" not in app._sonos_speakers_by_uuid

    async def test_room_lost_stops_and_removes_speaker(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        app._speakers.append(speaker)
        app._sonos_speakers_by_uuid["RINCON_A"] = speaker

        await app._on_sonos_room_lost("RINCON_A")

        speaker.stop.assert_awaited_once()
        assert app._speakers == []
        assert "RINCON_A" not in app._sonos_speakers_by_uuid

    async def test_room_lost_is_a_noop_for_unknown_uuid(self) -> None:
        app = QobuzProxy(_make_config())

        await app._on_sonos_room_lost("RINCON_UNKNOWN")  # must not raise


class TestRoomRenamed:
    async def test_renames_running_speaker_in_place(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_uuid["RINCON_A"] = speaker

        grouped_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
        )
        result = await app._on_sonos_room_renamed(grouped_room)

        assert result is True
        speaker.rename.assert_awaited_once_with("Kitchen, Living Room")

    async def test_returns_false_when_speaker_not_running(self) -> None:
        app = QobuzProxy(_make_config())

        result = await app._on_sonos_room_renamed(ROOM)

        assert result is False

    async def test_returns_false_on_name_collision(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_uuid["RINCON_A"] = speaker

        other = _mock_speaker("Kitchen, Living Room")
        app._speakers.append(other)

        grouped_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
        )
        result = await app._on_sonos_room_renamed(grouped_room)

        assert result is False
        speaker.rename.assert_not_called()


class TestAddSpeakerGuard:
    async def test_manual_add_rejected_while_auto_discover_enabled(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=True))

        with pytest.raises(ValueError, match="sonos_auto_discover"):
            await app._on_add_speaker({"name": "New Speaker", "dlna_ip": "10.0.1.50"})


class TestStopSpeakers:
    async def test_stop_speakers_stops_discovery_manager(self) -> None:
        app = QobuzProxy(_make_config())
        mock_manager = MagicMock()
        mock_manager.stop = AsyncMock()
        app._sonos_discovery = mock_manager
        app._sonos_speakers_by_uuid["RINCON_A"] = _mock_speaker()

        await app._stop_speakers()

        mock_manager.stop.assert_awaited_once()
        assert app._sonos_discovery is None
        assert app._sonos_speakers_by_uuid == {}

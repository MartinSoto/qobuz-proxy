"""Tests for wiring SonosController into QobuzProxy's speaker lifecycle.

The actual found/lost/renamed/retargeted/rekeyed/members-departed policy
lives in (and is tested by) SonosController itself — see
tests/backends/test_sonos_controller.py. This file only covers what
QobuzProxy does around it: which path _start_speakers() takes, the manual
add-speaker guard, and how a Sonos-managed speaker joins the picture for
cross-cutting concerns (the web UI's list, app-wide shutdown).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from qobuz_proxy.app import QobuzProxy
from qobuz_proxy.config import Config, QobuzConfig, SpeakerConfig


def _make_config(sonos_auto_discover: bool = True, speakers: list | None = None) -> Config:
    config = Config()
    config.qobuz = QobuzConfig(email="test@example.com", auth_token="secret", user_id="12345")
    config.sonos_auto_discover = sonos_auto_discover
    config.speakers = speakers or []
    return config


def _mock_speaker(name: str = "Manual", starts: bool = True) -> MagicMock:
    speaker = MagicMock()
    speaker.start = AsyncMock(return_value=starts)
    speaker.stop = AsyncMock()
    speaker.name = name
    speaker.is_active = False  # explicit default — a MagicMock attribute is truthy otherwise
    return speaker


class TestStartSpeakersBranching:
    async def test_auto_discover_starts_the_sonos_controller_not_static_speakers(self) -> None:
        config = _make_config(sonos_auto_discover=True, speakers=[SpeakerConfig(name="Manual")])
        app = QobuzProxy(config)
        app._api_client = MagicMock()
        app._sonos_event_subscriber = MagicMock()

        mock_controller = MagicMock()
        mock_controller.start = AsyncMock()
        mock_controller.speakers = []

        with (
            patch("qobuz_proxy.app.Speaker") as MockSpeaker,
            patch("qobuz_proxy.app.SonosController", return_value=mock_controller) as MockCtrl,
        ):
            await app._start_speakers()

        MockCtrl.assert_called_once()
        mock_controller.start.assert_awaited_once()
        MockSpeaker.assert_not_called()  # the static 'Manual' entry is ignored
        assert app._sonos_controller is mock_controller

    async def test_disabled_uses_static_speakers_as_before(self) -> None:
        config = _make_config(sonos_auto_discover=False, speakers=[SpeakerConfig(name="Manual")])
        app = QobuzProxy(config)
        app._api_client = MagicMock()

        speaker = _mock_speaker("Manual")
        with (
            patch("qobuz_proxy.app.Speaker", return_value=speaker),
            patch("qobuz_proxy.app.SonosController") as MockCtrl,
        ):
            await app._start_speakers()

        MockCtrl.assert_not_called()
        speaker.start.assert_awaited_once()
        assert app._speakers == [speaker]

    async def test_sonos_controller_only_started_once(self) -> None:
        """A re-login after logout must not spin up a second controller
        while one is already tracked."""
        config = _make_config(sonos_auto_discover=True)
        app = QobuzProxy(config)
        app._api_client = MagicMock()
        app._sonos_event_subscriber = MagicMock()
        existing = MagicMock()
        app._sonos_controller = existing

        with patch("qobuz_proxy.app.SonosController") as MockCtrl:
            await app._start_speakers()

        MockCtrl.assert_not_called()
        assert app._sonos_controller is existing


class TestAddSpeakerGuard:
    async def test_manual_add_rejected_while_auto_discover_enabled(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=True))

        with pytest.raises(ValueError, match="sonos_auto_discover"):
            await app._on_add_speaker({"name": "New Speaker", "dlna_ip": "10.0.1.50"})


class TestAllSpeakers:
    def test_combines_manual_and_sonos_speakers(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=False))
        manual = _mock_speaker("Manual")
        app._speakers.append(manual)

        sonos_speaker = _mock_speaker("Kitchen")
        mock_controller = MagicMock()
        mock_controller.speakers = [sonos_speaker]
        app._sonos_controller = mock_controller

        assert app._all_speakers() == [manual, sonos_speaker]

    def test_manual_only_when_no_sonos_controller(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=False))
        manual = _mock_speaker("Manual")
        app._speakers.append(manual)

        assert app._all_speakers() == [manual]


class TestStopSpeakers:
    async def test_stop_speakers_stops_the_sonos_controller(self) -> None:
        app = QobuzProxy(_make_config())
        mock_controller = MagicMock()
        mock_controller.stop = AsyncMock()
        app._sonos_controller = mock_controller

        await app._stop_speakers()

        mock_controller.stop.assert_awaited_once()
        assert app._sonos_controller is None

    async def test_active_manual_speaker_gets_a_device_stop(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=False))
        speaker = _mock_speaker("Manual")
        speaker.is_active = True
        app._speakers.append(speaker)

        await app._stop_speakers()

        speaker.stop.assert_awaited_once_with(send_device_stop=True)

    async def test_idle_manual_speaker_is_not_sent_a_device_stop(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=False))
        speaker = _mock_speaker("Manual")
        speaker.is_active = False
        app._speakers.append(speaker)

        await app._stop_speakers()

        speaker.stop.assert_awaited_once_with(send_device_stop=False)

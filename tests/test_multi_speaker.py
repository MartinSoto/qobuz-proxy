"""Tests for multi-speaker orchestration and auth lifecycle in QobuzProxy."""

from unittest.mock import AsyncMock, MagicMock, patch

from qobuz_proxy.app import QobuzProxy
from qobuz_proxy.config import Config, QobuzConfig, SpeakerConfig


def _make_speaker_config(
    name: str = "Test Speaker", http_port: int = 8689, **kwargs
) -> SpeakerConfig:
    """Return a minimal SpeakerConfig for tests."""
    defaults = dict(
        name=name,
        uuid=f"uuid-{name.lower().replace(' ', '-')}",
        backend_type="dlna",
        max_quality=27,
        http_port=http_port,
        bind_address="0.0.0.0",
        dlna_ip="192.168.1.100",
        dlna_port=1400,
        dlna_fixed_volume=False,
        proxy_port=7120,
        audio_device="default",
        audio_buffer_size=2048,
    )
    defaults.update(kwargs)
    return SpeakerConfig(**defaults)


def _make_config(*speaker_configs: SpeakerConfig) -> Config:
    """Return a Config with the given speakers and auth credentials."""
    config = Config()
    config.qobuz = QobuzConfig(email="test@example.com", auth_token="secret", user_id="12345")
    config.speakers = list(speaker_configs)
    return config


class TestMultiSpeakerOrchestration:
    async def test_starts_multiple_speakers(self):
        """Two speakers constructed, both start() called, app is running."""
        sc1 = _make_speaker_config("Living Room", http_port=8689)
        sc2 = _make_speaker_config("Bedroom", http_port=8690)
        config = _make_config(sc1, sc2)

        mock_speaker_instances = [MagicMock(), MagicMock()]
        for m in mock_speaker_instances:
            m.start = AsyncMock(return_value=True)
            m.stop = AsyncMock()
            m.name = "mock"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", side_effect=mock_speaker_instances) as MockSpeaker,
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()

            assert MockSpeaker.call_count == 2
            for instance in mock_speaker_instances:
                instance.start.assert_called_once()
            assert app.is_running

    async def test_continues_when_one_speaker_fails(self):
        """One speaker succeeds, one returns False -> app still running with one speaker."""
        sc1 = _make_speaker_config("Living Room", http_port=8689)
        sc2 = _make_speaker_config("Bedroom", http_port=8690)
        config = _make_config(sc1, sc2)

        good = MagicMock()
        good.start = AsyncMock(return_value=True)
        good.stop = AsyncMock()
        good.name = "Living Room"

        bad = MagicMock()
        bad.start = AsyncMock(return_value=False)
        bad.stop = AsyncMock()
        bad.name = "Bedroom"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", side_effect=[good, bad]),
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()

            assert app.is_running
            assert len(app._speakers) == 1
            assert app._speakers[0] is good

    async def test_no_speakers_started_still_running(self):
        """All speakers fail -> app stays running (waiting for auth or retry)."""
        sc1 = _make_speaker_config("Living Room", http_port=8689)
        config = _make_config(sc1)

        bad = MagicMock()
        bad.start = AsyncMock(return_value=False)
        bad.stop = AsyncMock()
        bad.name = "Living Room"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", side_effect=[bad]),
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()

            # App stays running even if speakers failed (no RuntimeError)
            assert app.is_running
            assert len(app._speakers) == 0

    async def test_stop_stops_all_speakers(self):
        """After a successful start, stop() calls stop() on all started speakers."""
        sc1 = _make_speaker_config("Living Room", http_port=8689)
        sc2 = _make_speaker_config("Bedroom", http_port=8690)
        config = _make_config(sc1, sc2)

        mock_instances = []
        for name in ("Living Room", "Bedroom"):
            m = MagicMock()
            m.start = AsyncMock(return_value=True)
            m.stop = AsyncMock()
            m.name = name
            mock_instances.append(m)

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", side_effect=mock_instances),
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()
            await app.stop()

            for instance in mock_instances:
                instance.stop.assert_called_once()
            assert not app.is_running


class TestGracefulStartup:
    """Tests for the new graceful startup behavior."""

    async def test_starts_without_credentials(self):
        """App starts in waiting-for-auth state when no credentials in config or cache."""
        config = Config()  # No qobuz credentials
        config.speakers = [_make_speaker_config()]

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.load_user_token", return_value=None),
        ):
            app = QobuzProxy(config)
            await app.start()

            assert app.is_running
            assert app._auth_state["authenticated"] is False
            assert len(app._speakers) == 0

    async def test_starts_with_cached_token(self):
        """App picks up cached token and authenticates automatically."""
        config = Config()  # No config credentials
        config.speakers = [_make_speaker_config()]

        mock_speaker = MagicMock()
        mock_speaker.start = AsyncMock(return_value=True)
        mock_speaker.stop = AsyncMock()
        mock_speaker.name = "mock"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch(
                "qobuz_proxy.app.load_user_token",
                return_value={
                    "user_id": "99",
                    "user_auth_token": "cached_tok",
                    "email": "cached@example.com",
                },
            ),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", return_value=mock_speaker),
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()

            assert app._auth_state["authenticated"] is True
            assert app._auth_state["user_id"] == "99"
            assert len(app._speakers) == 1

    async def test_config_token_takes_priority_over_cache(self):
        """Config credentials are used even if cache exists."""
        config = _make_config(_make_speaker_config())

        mock_speaker = MagicMock()
        mock_speaker.start = AsyncMock(return_value=True)
        mock_speaker.stop = AsyncMock()
        mock_speaker.name = "mock"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.load_user_token") as mock_load_cache,
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", return_value=mock_speaker),
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()

            # Cache should not have been consulted
            mock_load_cache.assert_not_called()
            assert app._auth_state["authenticated"] is True
            assert app._auth_state["user_id"] == "12345"

    async def test_invalid_cached_token_enters_waiting(self):
        """When cached token is invalid, app enters waiting-for-auth state."""
        config = Config()
        config.speakers = [_make_speaker_config()]

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch(
                "qobuz_proxy.app.load_user_token",
                return_value={
                    "user_id": "99",
                    "user_auth_token": "bad_tok",
                    "email": "",
                },
            ),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=False)

            app = QobuzProxy(config)
            await app.start()

            assert app.is_running
            assert app._auth_state["authenticated"] is False
            assert len(app._speakers) == 0


class TestWebUICallbacks:
    """Tests for auth token submission and logout callbacks."""

    async def test_on_auth_token_success(self):
        """Successful token submission sets up client and starts speakers."""
        config = Config()
        config.speakers = [_make_speaker_config()]

        mock_speaker = MagicMock()
        mock_speaker.start = AsyncMock(return_value=True)
        mock_speaker.stop = AsyncMock()
        mock_speaker.name = "mock"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.load_user_token", return_value=None),
            patch("qobuz_proxy.app.QobuzAPIClient"),
            patch("qobuz_proxy.app.Speaker", return_value=mock_speaker),
            patch("qobuz_proxy.app.save_user_token", return_value=True),
        ):
            app = QobuzProxy(config)
            await app.start()
            assert not app._auth_state["authenticated"]

            result = await app._on_auth_token("42", "valid_token")

            assert result is True
            assert app._auth_state["authenticated"] is True
            assert app._auth_state["user_id"] == "42"
            assert len(app._speakers) == 1

    async def test_on_logout_stops_speakers_and_clears_state(self):
        """Logout stops speakers and resets auth state."""
        config = _make_config(_make_speaker_config())

        mock_speaker = MagicMock()
        mock_speaker.start = AsyncMock(return_value=True)
        mock_speaker.stop = AsyncMock()
        mock_speaker.name = "mock"

        with (
            patch.object(QobuzProxy, "_start_web_server", new_callable=AsyncMock),
            patch.object(QobuzProxy, "_stop_web_server", new_callable=AsyncMock),
            patch("qobuz_proxy.app.QobuzAPIClient") as MockAPIClient,
            patch("qobuz_proxy.app.Speaker", return_value=mock_speaker),
            patch("qobuz_proxy.app.clear_user_token") as mock_clear,
        ):
            MockAPIClient.return_value.login_with_token = AsyncMock(return_value=True)

            app = QobuzProxy(config)
            await app.start()
            assert app._auth_state["authenticated"] is True
            assert len(app._speakers) == 1

            await app._on_logout()

            assert app._auth_state["authenticated"] is False
            assert app._auth_state["user_id"] == ""
            assert len(app._speakers) == 0
            mock_speaker.stop.assert_called_once()
            mock_clear.assert_called_once()

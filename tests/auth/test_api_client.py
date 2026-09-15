from unittest.mock import AsyncMock, MagicMock, patch


from qobuz_proxy.auth.api_client import QobuzAPIClient


class TestLoginWithToken:
    async def test_successful_login(self):
        client = QobuzAPIClient("app123", "secret456")
        mock_response_json = {
            "user_auth_token": "fresh_token",
            "user": {"id": 999, "email": "test@example.com"},
        }

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response_json)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.login_with_token("999", "old_token")

        assert result is True
        assert client.user_auth_token == "fresh_token"
        assert client.user_id == "999"

        mock_session.post.assert_called_once()
        call_kwargs = mock_session.post.call_args
        assert call_kwargs.kwargs["data"] == "extra=partner"
        headers = call_kwargs.kwargs["headers"]
        assert headers["X-App-Id"] == "app123"
        assert headers["X-User-Auth-Token"] == "old_token"

    async def test_failed_login_returns_false(self):
        client = QobuzAPIClient("app123", "secret456")

        mock_resp = AsyncMock()
        mock_resp.status = 401
        mock_resp.text = AsyncMock(return_value="Unauthorized")
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.login_with_token("999", "bad_token")

        assert result is False
        assert client.user_auth_token is None

    async def test_login_exception_returns_false(self):
        client = QobuzAPIClient("app123", "secret456")

        with patch(
            "qobuz_proxy.auth.api_client.aiohttp.ClientSession",
            side_effect=Exception("network error"),
        ):
            result = await client.login_with_token("999", "token")

        assert result is False


class TestGetTracksAudioInfo:
    """track/getList (batch) — the source of gain data for
    backends/dlna/sonos/gain.py. Confirmed live 2026-09-15 (qobuz-player.har)."""

    async def test_empty_input_makes_no_request(self):
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock()

        result = await client.get_tracks_audio_info([])

        assert result == {}
        client._request_signed.assert_not_awaited()

    async def test_parses_audio_info_and_album_id(self):
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(
            return_value={
                "tracks": {
                    "items": [
                        {
                            "id": 111,
                            "duration": 200,
                            "audio_info": {
                                "replaygain_track_gain": -9.35,
                                "replaygain_track_peak": 0.97052,
                            },
                            "album": {"id": "ALB123"},
                        }
                    ]
                }
            }
        )

        result = await client.get_tracks_audio_info(["111"])

        assert client._request_signed.await_args.kwargs["method"] == "POST"
        info = result["111"]
        assert info.gain_db == -9.35
        assert info.peak == 0.97052
        assert info.album_id == "ALB123"
        assert info.duration_ms == 200_000

    async def test_missing_audio_info_defaults_to_no_adjustment(self):
        """A real catalog gap (no audio_info at all) must read the same
        as a track that genuinely needs no gain, not as an error."""
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(
            return_value={"tracks": {"items": [{"id": 111, "duration": 200}]}}
        )

        result = await client.get_tracks_audio_info(["111"])

        assert result["111"].gain_db == 0.0
        assert result["111"].peak == 1.0

    async def test_unrecognized_id_is_simply_absent(self):
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(return_value={"tracks": {"items": []}})

        result = await client.get_tracks_audio_info(["999"])

        assert result == {}

    async def test_request_failure_returns_empty_dict_not_raises(self):
        client = QobuzAPIClient("app123", "secret456")
        client._request_signed = AsyncMock(side_effect=RuntimeError("boom"))

        result = await client.get_tracks_audio_info(["111"])

        assert result == {}

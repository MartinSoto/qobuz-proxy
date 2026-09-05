"""Tests for QobuzAPIClient.get_ws_token — the device-initiated WS-token
refresh path (see docs/gapless-arming-and-token-expiry.md). Mirrors the
reference StreamCore32 device's own getWSToken(): POST .../qws/createToken
or .../qws/refreshToken, authenticated the same way as every other signed
call this client makes.
"""

from unittest.mock import AsyncMock, MagicMock, patch

from qobuz_proxy.auth.api_client import QobuzAPIClient


def _client() -> QobuzAPIClient:
    client = QobuzAPIClient("app123", "secret456")
    client.user_auth_token = "user-token"
    client.x_session_id = "sess-id"
    # Bypass start_session()'s own HTTP call entirely — isolates these
    # tests to get_ws_token()'s own request.
    client.start_session = AsyncMock(return_value=True)  # type: ignore[method-assign]
    return client


def _mock_post(status: int, json_body: object) -> MagicMock:
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=json_body)
    mock_resp.text = AsyncMock(return_value=str(json_body))
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.post = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


class TestGetWsToken:
    async def test_no_previous_jwt_requests_create_token(self) -> None:
        client = _client()
        mock_session = _mock_post(
            200, {"jwt_qws": {"jwt": "new-jwt", "exp": 123456, "endpoint": "wss://qws/ws"}}
        )

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.get_ws_token()

        assert result == {"jwt": "new-jwt", "exp": 123456, "endpoint": "wss://qws/ws"}
        call = mock_session.post.call_args
        assert call.args[0] == f"{QobuzAPIClient.API_BASE}/qws/createToken"
        assert call.kwargs["data"] == "jwt=jwt_qws"
        headers = call.kwargs["headers"]
        assert headers["X-App-Id"] == "app123"
        assert headers["X-Session-Id"] == "sess-id"
        assert headers["X-User-Auth-Token"] == "user-token"
        assert headers["Origin"] == "https://play.qobuz.com"

    async def test_previous_jwt_requests_refresh_token(self) -> None:
        client = _client()
        mock_session = _mock_post(
            200,
            {"jwt_qws": {"jwt": "refreshed-jwt", "exp": 999, "endpoint": "wss://qws/ws"}},
        )

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.get_ws_token(previous_jwt="old-jwt")

        assert result is not None
        assert result["jwt"] == "refreshed-jwt"
        call = mock_session.post.call_args
        assert call.args[0] == f"{QobuzAPIClient.API_BASE}/qws/refreshToken"

    async def test_non_200_response_returns_none(self) -> None:
        client = _client()
        mock_session = _mock_post(403, {"error": "expired"})

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.get_ws_token()

        assert result is None

    async def test_missing_jwt_qws_in_response_returns_none(self) -> None:
        client = _client()
        mock_session = _mock_post(200, {"unexpected": "shape"})

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.get_ws_token()

        assert result is None

    async def test_request_exception_returns_none(self) -> None:
        client = _client()

        with patch(
            "qobuz_proxy.auth.api_client.aiohttp.ClientSession", side_effect=OSError("no network")
        ):
            result = await client.get_ws_token()

        assert result is None

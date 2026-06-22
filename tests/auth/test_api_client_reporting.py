"""Tests for Qobuz streaming-event reporting (play tracking / Last.fm scrobbling).

Qobuz scrobbles to a linked Last.fm account server-side, but only for plays a
Connect device reports via track/reportStreamingStart and
track/reportStreamingEndJson. These tests pin the request shape of those calls.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

from qobuz_proxy.auth.api_client import QobuzAPIClient


def _mock_session(status: int = 200):
    """Build a mocked aiohttp ClientSession whose post() returns `status`."""
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value="")
    resp.json = AsyncMock(return_value={})
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)

    session = AsyncMock()
    session.post = MagicMock(return_value=resp)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


def _authed_client() -> QobuzAPIClient:
    client = QobuzAPIClient("app123", "secret456")
    client.user_auth_token = "user_token"
    client.user_id = "999"
    client.x_session_id = "sess_abc"
    client.x_session_expires_at = 9_999_999_999_999  # valid; start_session is a no-op
    return client


class TestReportStreamingStart:
    async def test_posts_start_event_with_auth_headers(self):
        client = _authed_client()
        session = _mock_session(200)

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=session):
            ok = await client.report_streaming_start(track_id="555", format_id=27)

        assert ok is True
        session.post.assert_called_once()
        args, kwargs = session.post.call_args
        url = args[0] if args else kwargs["url"]
        assert url.endswith("/track/reportStreamingStart")

        headers = kwargs["headers"]
        assert headers["X-App-Id"] == "app123"
        assert headers["X-User-Auth-Token"] == "user_token"
        assert headers["X-Session-Id"] == "sess_abc"

        # Body is `events=[{...}]` with the play details.
        body = kwargs["data"]
        assert body.startswith("events=")
        events = json.loads(body[len("events=") :])
        assert events[0]["track_id"] == 555
        assert events[0]["format_id"] == 27
        assert events[0]["user_id"] == 999
        assert events[0]["online"] is True
        assert events[0]["local"] is False

    async def test_201_created_is_success(self):
        """Qobuz answers reportStreamingStart with HTTP 201 — that's success."""
        client = _authed_client()
        session = _mock_session(201)
        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=session):
            ok = await client.report_streaming_start(track_id="555", format_id=27)
        assert ok is True

    async def test_non_2xx_returns_false(self):
        client = _authed_client()
        session = _mock_session(401)
        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=session):
            ok = await client.report_streaming_start(track_id="555", format_id=27)
        assert ok is False

    async def test_network_error_returns_false(self):
        client = _authed_client()
        with patch(
            "qobuz_proxy.auth.api_client.aiohttp.ClientSession",
            side_effect=Exception("boom"),
        ):
            ok = await client.report_streaming_start(track_id="555", format_id=27)
        assert ok is False

    async def test_refreshes_session_before_reporting(self):
        """A long track can outlive the session; reporting must refresh it first."""
        client = _authed_client()
        client.start_session = AsyncMock(return_value=True)  # type: ignore[method-assign]
        session = _mock_session(200)
        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=session):
            await client.report_streaming_start(track_id="555", format_id=27)
        client.start_session.assert_awaited_once()


class TestReportStreamingEnd:
    async def test_posts_end_event_json_with_played_duration(self):
        client = _authed_client()
        session = _mock_session(200)

        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=session):
            ok = await client.report_streaming_end(
                track_id="555",
                blob="opaque-blob",
                context_uuid="123e4567-e89b-12d3-a456-426614174000",
                started_at_ms=1700000000000,
                played_seconds=183,
            )

        assert ok is True
        args, kwargs = session.post.call_args
        url = args[0] if args else kwargs["url"]
        assert url.endswith("/track/reportStreamingEndJson")

        headers = kwargs["headers"]
        assert headers["X-User-Auth-Token"] == "user_token"

        payload = json.loads(kwargs["data"])
        event = payload["events"][0]
        assert event["blob"] == "opaque-blob"
        assert event["track_context_uuid"] == "123e4567-e89b-12d3-a456-426614174000"
        assert event["duration"] == 183
        assert event["online"] is True
        assert event["local"] is False
        # start_stream is ISO8601 UTC with millis; 1700000000000ms -> 2023-11-14T22:13:20.000Z
        assert event["start_stream"] == "2023-11-14T22:13:20.000Z"
        assert "renderer_context" in payload

    async def test_non_200_returns_false(self):
        client = _authed_client()
        session = _mock_session(500)
        with patch("qobuz_proxy.auth.api_client.aiohttp.ClientSession", return_value=session):
            ok = await client.report_streaming_end(
                track_id="555",
                blob="b",
                context_uuid=None,
                started_at_ms=1700000000000,
                played_seconds=10,
            )
        assert ok is False

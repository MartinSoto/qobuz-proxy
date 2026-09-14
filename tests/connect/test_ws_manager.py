"""Tests for WebSocket manager."""

import asyncio
import base64
import logging
import re
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from qobuz_proxy.config import Config
from qobuz_proxy.connect.protocol import MessageType
from qobuz_proxy.connect.types import ConnectTokens, JWTConnectToken
from qobuz_proxy.connect import ws_manager as ws_manager_module
from qobuz_proxy.connect.ws_manager import (
    HIJACK_RECONNECT_HOLD_SECONDS,
    INITIAL_RECONNECT_DELAY,
    MAX_RECONNECT_DELAY,
    RECONNECT_BACKOFF_MULTIPLIER,
    IdleShutdownRequired,
    TokenRefreshRequired,
    WsManager,
    _log_raw_frame,
)
from qobuz_proxy.proto import common as common_pb2


@pytest.fixture
def config() -> Config:
    """Create a test configuration."""
    cfg = Config()
    cfg.qobuz.email = "test@test.com"
    cfg.qobuz.auth_token = "test_password"
    cfg.device.name = "Test Device"
    cfg.device.uuid = str(uuid.uuid4())
    cfg.backend.dlna.ip = "192.168.1.100"
    return cfg


@pytest.fixture
def ws_manager(config: Config) -> WsManager:
    """Create a WsManager instance."""
    return WsManager(config)


@pytest.fixture
def valid_tokens() -> ConnectTokens:
    """Create valid test tokens."""
    return ConnectTokens(
        session_id=str(uuid.uuid4()),
        ws_token=JWTConnectToken(
            jwt="test_jwt_token",
            exp=9999999999,  # Far future
            endpoint="wss://test.qobuz.com/ws",
        ),
    )


class TestWsManagerInit:
    """Tests for WsManager initialization."""

    def test_init_creates_codec(self, ws_manager: WsManager) -> None:
        """Test that initialization creates a protocol codec."""
        assert ws_manager._codec is not None

    def test_init_not_connected(self, ws_manager: WsManager) -> None:
        """Test that manager starts disconnected."""
        assert ws_manager.is_connected is False

    def test_init_no_tokens(self, ws_manager: WsManager) -> None:
        """Test that manager starts without tokens."""
        assert ws_manager._ws_token is None
        assert ws_manager._session_uuid is None

    def test_init_empty_handlers(self, ws_manager: WsManager) -> None:
        """Test that handler dict is empty initially."""
        assert len(ws_manager._handlers) == 0

    def test_init_empty_pending_messages(self, ws_manager: WsManager) -> None:
        """Test that pending messages queue is empty."""
        assert len(ws_manager._pending_messages) == 0

    def test_init_reconnect_delay(self, ws_manager: WsManager) -> None:
        """Test initial reconnect delay."""
        assert ws_manager._reconnect_delay == INITIAL_RECONNECT_DELAY


class TestTokenManagement:
    """Tests for token management."""

    def test_set_tokens(self, ws_manager: WsManager, valid_tokens: ConnectTokens) -> None:
        """Test setting connection tokens."""
        ws_manager.set_tokens(valid_tokens)

        assert ws_manager._ws_token is not None
        assert ws_manager._ws_token.jwt == "test_jwt_token"
        assert ws_manager._ws_token.endpoint == "wss://test.qobuz.com/ws"
        assert ws_manager._session_uuid is not None

    def test_set_tokens_converts_session_uuid(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        """Test that session ID is converted to bytes."""
        ws_manager.set_tokens(valid_tokens)
        assert isinstance(ws_manager._session_uuid, bytes)
        assert len(ws_manager._session_uuid) == 16

    @pytest.mark.asyncio
    async def test_wait_for_valid_token_blocks_until_refresh(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        """Expired tokens should wait for refresh instead of looping reconnects."""
        expired_tokens = ConnectTokens(
            session_id=str(uuid.uuid4()),
            ws_token=JWTConnectToken(
                jwt="expired_jwt_token",
                exp=1,
                endpoint="wss://test.qobuz.com/ws",
            ),
        )

        ws_manager.set_tokens(expired_tokens)
        ws_manager._should_run = True

        wait_task = asyncio.create_task(ws_manager._wait_for_valid_token(buffer_s=60))
        await asyncio.sleep(0)
        assert wait_task.done() is False

        ws_manager.set_tokens(valid_tokens)

        assert await asyncio.wait_for(wait_task, timeout=1.0) is True

    @pytest.mark.asyncio
    async def test_set_tokens_closes_existing_connection_for_refresh(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        """Receiving fresh tokens while connected should force a reconnect."""
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True
        ws_manager._ws = AsyncMock()

        refreshed_tokens = ConnectTokens(
            session_id=str(uuid.uuid4()),
            ws_token=JWTConnectToken(
                jwt="refreshed_jwt_token",
                exp=9999999999,
                endpoint="wss://test.qobuz.com/ws",
            ),
        )

        ws_manager.set_tokens(refreshed_tokens)
        await asyncio.sleep(0)

        ws_manager._ws.close.assert_awaited_once()


class TestSelfRefreshWsToken:
    """_refresh_token_from_api / _wait_for_valid_token's use of it — the
    control WebSocket refreshing its own token directly via the Qobuz API
    instead of only ever waiting for the phone app to reconnect and push
    one. See docs/gapless-arming-and-token-expiry.md for the failure this
    closes: nothing could recover here on its own once the app's own OS
    stopped keeping it reachable in the background."""

    async def test_no_api_client_falls_back_to_waiting(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        """The `ws_manager` fixture wires up no api_client at all — self-
        refresh must be a clean no-op, not an error."""
        ws_manager.set_tokens(valid_tokens)  # establishes a session

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is False

    async def test_no_prior_session_falls_back_to_waiting(self, config: Config) -> None:
        """The very first token can only ever come from the app — there's
        no session identity yet to refresh a token *for*."""
        ws_manager = WsManager(config, api_client=AsyncMock())

        assert ws_manager._session_uuid is None
        assert await ws_manager._refresh_token_from_api(buffer_s=60) is False

    async def test_successful_self_refresh_updates_the_token(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(
            return_value={"jwt": "self-refreshed", "exp": 9999999999, "endpoint": "wss://x/ws"}
        )
        ws_manager = WsManager(config, api_client=api_client)
        ws_manager.set_tokens(valid_tokens)
        old_version = ws_manager._token_version

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is True

        assert ws_manager._ws_token is not None
        assert ws_manager._ws_token.jwt == "self-refreshed"
        assert ws_manager._token_version == old_version + 1
        api_client.get_ws_token.assert_awaited_once_with(previous_jwt="test_jwt_token")

    async def test_failed_self_refresh_returns_false(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(return_value=None)
        ws_manager = WsManager(config, api_client=api_client)
        ws_manager.set_tokens(valid_tokens)

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is False

    async def test_self_refresh_exception_returns_false_instead_of_raising(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(side_effect=OSError("unreachable"))
        ws_manager = WsManager(config, api_client=api_client)
        ws_manager.set_tokens(valid_tokens)

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is False

    async def test_invalid_refreshed_token_is_rejected(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        """A response missing a real field must not be accepted as a
        genuine refresh — falls back to waiting instead."""
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(
            return_value={"jwt": "", "exp": 9999999999, "endpoint": "wss://x/ws"}
        )
        ws_manager = WsManager(config, api_client=api_client)
        ws_manager.set_tokens(valid_tokens)

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is False

    async def test_wait_for_valid_token_self_refreshes_without_the_app(
        self, config: Config
    ) -> None:
        """End-to-end: an expired token recovers on its own via
        _wait_for_valid_token, with set_tokens() (the app) never called."""
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(
            return_value={"jwt": "self-refreshed", "exp": 9999999999, "endpoint": "wss://x/ws"}
        )
        ws_manager = WsManager(config, api_client=api_client)
        ws_manager.set_tokens(
            ConnectTokens(
                session_id=str(uuid.uuid4()),
                ws_token=JWTConnectToken(jwt="expiring", exp=1, endpoint="wss://x/ws"),
            )
        )
        ws_manager._should_run = True

        result = await asyncio.wait_for(ws_manager._wait_for_valid_token(buffer_s=60), timeout=1.0)

        assert result is True
        assert ws_manager._ws_token is not None
        assert ws_manager._ws_token.jwt == "self-refreshed"

    async def test_not_worth_keeping_alive_skips_the_refresh(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        """See Player.is_worth_keeping_ws_alive — a long-idle session must
        stop self-renewing and let its token lapse naturally instead of
        refreshing forever with nobody listening."""
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(
            return_value={"jwt": "self-refreshed", "exp": 9999999999, "endpoint": "wss://x/ws"}
        )
        ws_manager = WsManager(config, api_client=api_client, is_worth_keeping_alive=lambda: False)
        ws_manager.set_tokens(valid_tokens)

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is False
        api_client.get_ws_token.assert_not_awaited()

    async def test_worth_keeping_alive_still_refreshes(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(
            return_value={"jwt": "self-refreshed", "exp": 9999999999, "endpoint": "wss://x/ws"}
        )
        ws_manager = WsManager(config, api_client=api_client, is_worth_keeping_alive=lambda: True)
        ws_manager.set_tokens(valid_tokens)

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is True

    async def test_no_callback_keeps_refreshing_as_before(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        """None (the default — no policy wired up, e.g. most existing
        tests) must keep the pre-existing always-refresh behavior."""
        api_client = AsyncMock()
        api_client.get_ws_token = AsyncMock(
            return_value={"jwt": "self-refreshed", "exp": 9999999999, "endpoint": "wss://x/ws"}
        )
        ws_manager = WsManager(config, api_client=api_client)
        ws_manager.set_tokens(valid_tokens)

        assert await ws_manager._refresh_token_from_api(buffer_s=60) is True


class TestWaitForValidTokenGating:
    """_wait_for_valid_token's eager "already good, connect now" check
    must not fire just because the token itself happens to still be
    valid — it also has to agree the connection is worth holding open at
    all, or IdleShutdownRequired's proactive close (see
    TestReceiveLoopIdleShutdown below) would be undone the instant the
    connection loop asked again, and invalidate_session()'s whole point
    (stop retrying a session_uuid the server already rejected) would be
    defeated the same way."""

    async def test_idle_playback_blocks_even_with_a_valid_token(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager = WsManager(config, is_worth_keeping_alive=lambda: False)
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True

        wait_task = asyncio.create_task(ws_manager._wait_for_valid_token(buffer_s=60))
        await asyncio.sleep(0)

        assert wait_task.done() is False
        wait_task.cancel()

    async def test_invalidated_session_blocks_even_with_a_valid_token(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager = WsManager(config)
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True
        ws_manager.invalidate_session("test")

        wait_task = asyncio.create_task(ws_manager._wait_for_valid_token(buffer_s=60))
        await asyncio.sleep(0)

        assert wait_task.done() is False
        wait_task.cancel()

    async def test_resuming_playback_unblocks_once_the_app_reconnects(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        """Mirrors the real recovery path: is_worth_keeping_alive flips
        True (a resume), but there's no separate "wake up, resume
        happened" signal into this loop — same pre-existing shape as
        test_wait_for_valid_token_blocks_until_refresh, just gated on the
        idle flag instead of token expiry."""
        worth_it = False
        ws_manager = WsManager(config, is_worth_keeping_alive=lambda: worth_it)
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True

        wait_task = asyncio.create_task(ws_manager._wait_for_valid_token(buffer_s=60))
        await asyncio.sleep(0)
        assert wait_task.done() is False

        worth_it = True
        ws_manager.set_tokens(valid_tokens)  # app-style nudge, unblocks the wait

        assert await asyncio.wait_for(wait_task, timeout=1.0) is True


class TestReceiveLoopIdleShutdown:
    """_receive_loop's own idle check — the counterpart to
    _wait_for_valid_token's gating above. An *already open* connection
    only ever finds out playback went idle by hitting this check on its
    next receive-timeout tick, since nothing else interrupts a live
    connection just because Player's state changed underneath it."""

    @staticmethod
    def _make_timing_out_ws() -> AsyncMock:
        """recv() that never returns — the real target of _receive_loop's
        asyncio.wait_for(..., timeout=RECV_TIMEOUT), so the TimeoutError
        branch under test fires deterministically instead of racing a
        short real message."""
        ws = AsyncMock()

        async def hang_forever() -> bytes:
            await asyncio.sleep(3600)
            return b""  # pragma: no cover - never reached

        ws.recv = hang_forever
        return ws

    @pytest.fixture(autouse=True)
    def fast_recv_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Every test in this class hits the TimeoutError branch on
        purpose — no need to actually burn RECV_TIMEOUT's real second
        getting there."""
        monkeypatch.setattr(ws_manager_module, "RECV_TIMEOUT", 0.01)

    async def test_raises_when_idle_past_the_keep_alive_window(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager = WsManager(config, is_worth_keeping_alive=lambda: False)
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True
        ws_manager._ws = self._make_timing_out_ws()

        with pytest.raises(IdleShutdownRequired):
            await ws_manager._receive_loop()

    async def test_does_not_raise_while_still_worth_keeping_alive(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager = WsManager(config, is_worth_keeping_alive=lambda: True)
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True
        ws_manager._ws = self._make_timing_out_ws()

        task = asyncio.create_task(ws_manager._receive_loop())
        await asyncio.sleep(0.05)
        assert task.done() is False
        task.cancel()

    async def test_no_callback_never_raises_idle_shutdown(
        self, config: Config, valid_tokens: ConnectTokens
    ) -> None:
        """None (no policy wired up) must keep the pre-existing
        behavior — never proactively closing — exactly like
        _refresh_token_from_api's own no-callback case."""
        ws_manager = WsManager(config)
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True
        ws_manager._ws = self._make_timing_out_ws()

        task = asyncio.create_task(ws_manager._receive_loop())
        await asyncio.sleep(0.05)
        assert task.done() is False
        task.cancel()

    async def test_token_expiry_takes_priority_over_idle_shutdown(self, config: Config) -> None:
        """Both conditions can be true on the same tick (a long-expired
        token on a connection that also happens to be idle) —
        TokenRefreshRequired must win so self-refresh still gets a
        chance rather than the connection being written off as merely
        idle."""
        expiring_tokens = ConnectTokens(
            session_id=str(uuid.uuid4()),
            ws_token=JWTConnectToken(jwt="expiring", exp=1, endpoint="wss://x/ws"),
        )
        ws_manager = WsManager(config, is_worth_keeping_alive=lambda: False)
        ws_manager.set_tokens(expiring_tokens)
        ws_manager._should_run = True
        ws_manager._ws = self._make_timing_out_ws()

        with pytest.raises(TokenRefreshRequired):
            await ws_manager._receive_loop()


class TestForceReconnect:
    """force_reconnect() — used when a renderer detects an external
    takeover (see Player.is_playing_our_content()). The protocol has no
    message for voluntarily giving up "active" status, and neither for a
    device that just crashed or lost power, so the server likely relies
    on its own WS heartbeat/timeout to notice a renderer is gone — this
    holds the connection closed for a while (via the existing
    _reconnect_delay/backoff mechanism) to give that a real chance to
    fire, rather than reconnecting immediately."""

    async def test_closes_the_current_connection(self, ws_manager: WsManager) -> None:
        ws_manager._ws = AsyncMock()

        await ws_manager.force_reconnect("test reason")

        ws_manager._ws.close.assert_awaited_once()

    async def test_sets_the_reconnect_delay_to_the_hold_duration(
        self, ws_manager: WsManager
    ) -> None:
        ws_manager._ws = AsyncMock()
        ws_manager._reconnect_delay = INITIAL_RECONNECT_DELAY

        await ws_manager.force_reconnect("test reason", hold_seconds=8.0)

        assert ws_manager._reconnect_delay == 8.0

    async def test_the_next_join_will_not_reclaim_active_status(
        self, ws_manager: WsManager
    ) -> None:
        # The server recognizes the *same* renderer reconnecting (by
        # deviceUuid/sessionUuid) and would otherwise hand control right
        # back, evicting whatever the app fell back to in the meantime.
        ws_manager._ws = AsyncMock()
        assert ws_manager._next_join_claims_active is True

        await ws_manager.force_reconnect("test reason")

        assert ws_manager._next_join_claims_active is False

    async def test_default_hold_is_used_when_not_specified(self, ws_manager: WsManager) -> None:
        ws_manager._ws = AsyncMock()

        await ws_manager.force_reconnect("test reason")

        assert ws_manager._reconnect_delay == HIJACK_RECONNECT_HOLD_SECONDS

    async def test_is_a_noop_when_not_connected(self, ws_manager: WsManager) -> None:
        ws_manager._ws = None
        ws_manager._reconnect_delay = INITIAL_RECONNECT_DELAY

        await ws_manager.force_reconnect("test reason")  # must not raise

        assert ws_manager._reconnect_delay == INITIAL_RECONNECT_DELAY  # untouched
        assert ws_manager._next_join_claims_active is True  # untouched

    async def test_swallows_close_errors(self, ws_manager: WsManager) -> None:
        ws_manager._ws = AsyncMock()
        ws_manager._ws.close.side_effect = OSError("already gone")

        await ws_manager.force_reconnect("test reason")  # must not raise


class TestInvalidateSession:
    """invalidate_session() — called by speaker.py's _handle_protocol_error
    when the server rejects our cached session_uuid outright (observed
    live as error code 1001, "session data is missing" /
    "sessionUuid mismatch"). Unlike force_reconnect(), there's no hold to
    wait out and rejoin after — the identity itself is gone, so the next
    connection attempt has to wait for the app instead of retrying it."""

    async def test_clears_the_session_uuid(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager.set_tokens(valid_tokens)
        assert ws_manager._session_uuid is not None

        ws_manager.invalidate_session("sessionUuid mismatch")

        assert ws_manager._session_uuid is None

    async def test_closes_the_current_connection(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager.set_tokens(valid_tokens)
        ws_manager._ws = AsyncMock()

        ws_manager.invalidate_session("session data is missing")
        await asyncio.sleep(0)  # let the spawned close task run

        ws_manager._ws.close.assert_awaited_once()

    async def test_is_a_noop_without_a_prior_session(self, ws_manager: WsManager) -> None:
        """Nothing to invalidate yet (e.g. before the app has ever
        connected) — must not touch a connection that isn't ours to close
        for this reason."""
        assert ws_manager._session_uuid is None
        ws_manager._ws = AsyncMock()

        ws_manager.invalidate_session("test reason")
        await asyncio.sleep(0)

        ws_manager._ws.close.assert_not_awaited()

    async def test_is_a_noop_without_a_live_connection(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        ws_manager.set_tokens(valid_tokens)
        ws_manager._ws = None

        ws_manager.invalidate_session("test reason")  # must not raise

        assert ws_manager._session_uuid is None

    async def test_next_reconnect_waits_for_the_app_instead_of_retrying(
        self, ws_manager: WsManager, valid_tokens: ConnectTokens
    ) -> None:
        """End-to-end with _wait_for_valid_token: invalidating a session
        with an otherwise still-valid token must not immediately hand
        back a green light to reconnect with — see
        TestWaitForValidTokenGating."""
        ws_manager.set_tokens(valid_tokens)
        ws_manager._should_run = True

        ws_manager.invalidate_session("sessionUuid mismatch")

        wait_task = asyncio.create_task(ws_manager._wait_for_valid_token(buffer_s=60))
        await asyncio.sleep(0)
        assert wait_task.done() is False

        ws_manager.set_tokens(valid_tokens)  # the app reconnects with a fresh session

        assert await asyncio.wait_for(wait_task, timeout=1.0) is True


class TestSendJoinSession:
    """_send_join_session() — is_active is a one-shot override (see
    force_reconnect()), consumed and reset on every join so only the
    rejoin immediately following a forced reconnect skips reclaiming
    active-renderer status."""

    async def test_normal_join_claims_active(self, ws_manager: WsManager) -> None:
        ws_manager._session_uuid = b"0" * 16
        ws_manager._ws = AsyncMock()

        with patch.object(
            ws_manager._codec, "encode_join_session", return_value=b"frame"
        ) as mock_encode:
            await ws_manager._send_join_session()

        assert mock_encode.call_args.kwargs["is_active"] is True

    async def test_overridden_join_does_not_claim_active_and_resets(
        self, ws_manager: WsManager
    ) -> None:
        ws_manager._session_uuid = b"0" * 16
        ws_manager._ws = AsyncMock()
        ws_manager._next_join_claims_active = False

        with patch.object(
            ws_manager._codec, "encode_join_session", return_value=b"frame"
        ) as mock_encode:
            await ws_manager._send_join_session()

        assert mock_encode.call_args.kwargs["is_active"] is False
        assert ws_manager._next_join_claims_active is True  # reset for next time

    async def test_noop_without_a_session_uuid(self, ws_manager: WsManager) -> None:
        ws_manager._session_uuid = None
        ws_manager._ws = AsyncMock()

        await ws_manager._send_join_session()  # must not raise

        ws_manager._ws.send.assert_not_called()

    async def test_join_carries_an_explicit_stopped_initial_state(
        self, ws_manager: WsManager
    ) -> None:
        """Experiment (2026-09-13, docs/connect-activation-model-hypotheses.md):
        tell the server explicitly that we have nothing of our own
        playing, rather than leaving initialState unset. Every field but
        playingState stays unset (all are optional)."""
        ws_manager._session_uuid = b"0" * 16
        ws_manager._ws = AsyncMock()

        with patch.object(
            ws_manager._codec, "encode_join_session", return_value=b"frame"
        ) as mock_encode:
            await ws_manager._send_join_session()

        initial_state = mock_encode.call_args.kwargs["initial_state"]
        assert initial_state.playingState == common_pb2.PLAYING_STATE_STOPPED
        assert not initial_state.HasField("bufferState")
        assert not initial_state.HasField("currentPosition")
        assert not initial_state.HasField("duration")
        assert not initial_state.HasField("currentQueueIndex")
        assert not initial_state.HasField("nextQueueItemId")

    # A has_resumable_playback-gated variant of is_active was tried and
    # reverted 2026-09-13 — see docs/connect-activation-model-hypotheses.md,
    # "The isActive=false experiment broke real device selection
    # entirely". Confirmed live: with isActive=false, SRVR_RNDR_SET_ACTIVE
    # never arrives at all, not even for a brand-new device identity the
    # app had just explicitly selected via connect-to-qconnect.


class TestHandlerRegistration:
    """Tests for message handler registration."""

    def test_register_handler(self, ws_manager: WsManager) -> None:
        """Test registering a message handler."""
        handler = MagicMock()
        ws_manager.register_handler(41, handler)  # SET_STATE

        assert 41 in ws_manager._handlers
        assert ws_manager._handlers[41] is handler

    def test_register_multiple_handlers(self, ws_manager: WsManager) -> None:
        """Test registering multiple handlers."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        handler3 = MagicMock()

        ws_manager.register_handler(41, handler1)
        ws_manager.register_handler(42, handler2)
        ws_manager.register_handler(43, handler3)

        assert len(ws_manager._handlers) == 3

    def test_register_handler_overwrites(self, ws_manager: WsManager) -> None:
        """Test that registering same type overwrites."""
        handler1 = MagicMock()
        handler2 = MagicMock()

        ws_manager.register_handler(41, handler1)
        ws_manager.register_handler(41, handler2)

        assert ws_manager._handlers[41] is handler2


class TestCallbacks:
    """Tests for connection callbacks."""

    def test_on_connected_callback(self, ws_manager: WsManager) -> None:
        """Test setting connected callback."""
        callback = MagicMock()
        ws_manager.on_connected(callback)
        assert ws_manager._on_connected is callback

    def test_on_disconnected_callback(self, ws_manager: WsManager) -> None:
        """Test setting disconnected callback."""
        callback = MagicMock()
        ws_manager.on_disconnected(callback)
        assert ws_manager._on_disconnected is callback


class TestUuidConversion:
    """Tests for UUID conversion utility."""

    def test_uuid_to_bytes_valid_uuid(self, ws_manager: WsManager) -> None:
        """Test converting valid UUID string to bytes."""
        uuid_str = "12345678-1234-5678-1234-567812345678"
        result = ws_manager._uuid_to_bytes(uuid_str)

        assert isinstance(result, bytes)
        assert len(result) == 16

    def test_uuid_to_bytes_invalid_uuid_uses_hash(self, ws_manager: WsManager) -> None:
        """Test that invalid UUID falls back to hash."""
        invalid_uuid = "not-a-valid-uuid"
        result = ws_manager._uuid_to_bytes(invalid_uuid)

        assert isinstance(result, bytes)
        assert len(result) == 16  # MD5 hash is 16 bytes


class TestReconnectionConstants:
    """Tests for reconnection constants."""

    def test_initial_delay(self) -> None:
        """Test initial reconnect delay value."""
        assert INITIAL_RECONNECT_DELAY == 1.0

    def test_max_delay(self) -> None:
        """Test maximum reconnect delay value."""
        assert MAX_RECONNECT_DELAY == 60.0

    def test_backoff_multiplier(self) -> None:
        """Test backoff multiplier value."""
        assert RECONNECT_BACKOFF_MULTIPLIER == 2.0

    def test_backoff_sequence(self) -> None:
        """Test expected backoff sequence."""
        delay = INITIAL_RECONNECT_DELAY
        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]

        for expected_delay in expected:
            assert delay == expected_delay
            delay = min(delay * RECONNECT_BACKOFF_MULTIPLIER, MAX_RECONNECT_DELAY)


class TestMessageQueueing:
    """Tests for message queueing during disconnect."""

    @pytest.mark.asyncio
    async def test_send_message_queues_when_disconnected(self, ws_manager: WsManager) -> None:
        """Test that messages are queued when not connected."""
        assert ws_manager.is_connected is False

        data = b"test_message_data"
        result = await ws_manager.send_message(data)

        assert result is False
        assert len(ws_manager._pending_messages) == 1
        assert ws_manager._pending_messages[0] == data

    @pytest.mark.asyncio
    async def test_multiple_messages_queued(self, ws_manager: WsManager) -> None:
        """Test that multiple messages can be queued."""
        messages = [b"msg1", b"msg2", b"msg3"]

        for msg in messages:
            await ws_manager.send_message(msg)

        assert len(ws_manager._pending_messages) == 3

    @pytest.mark.asyncio
    async def test_send_state_update_queues(self, ws_manager: WsManager) -> None:
        """Test that state updates are queued when disconnected."""
        result = await ws_manager.send_state_update(
            playing_state=2,
            buffer_state=2,
            position_ms=1000,
            duration_ms=60000,
            queue_item_id=1,
            queue_version_major=1,
            queue_version_minor=0,
        )

        assert result is False
        assert len(ws_manager._pending_messages) == 1

    @pytest.mark.asyncio
    async def test_send_volume_changed_queues(self, ws_manager: WsManager) -> None:
        """Test that volume changes are queued when disconnected."""
        result = await ws_manager.send_volume_changed(75)

        assert result is False
        assert len(ws_manager._pending_messages) == 1

    @pytest.mark.asyncio
    async def test_send_device_info_updated_queues(self, ws_manager: WsManager) -> None:
        """Test that a rename is queued when disconnected."""
        result = await ws_manager.send_device_info_updated("Kitchen, Living Room")

        assert result is False
        assert len(ws_manager._pending_messages) == 1

    @pytest.mark.asyncio
    async def test_send_device_info_updated_updates_config_name(
        self, ws_manager: WsManager
    ) -> None:
        """A rename updates config.device.name even while disconnected, so a
        future JOIN_SESSION on reconnect already carries the new name."""
        await ws_manager.send_device_info_updated("Kitchen, Living Room")

        assert ws_manager.config.device.name == "Kitchen, Living Room"


class TestRawFrameLogging:
    """The RAW_WS trace log — every frame this proxy sends or receives,
    one line each, base64-encoded — is what scripts/qconnect_trace.py's
    --proxy-log mode parses to build a trace comparable against a
    browser HAR capture (see the Controller-role dual-join
    investigation). See _log_raw_frame's own comment for the exact line
    format this locks in — scripts/qconnect_trace.py's regex must keep
    matching it."""

    _RAW_WS_RE = re.compile(r"RAW_WS (SEND|RECV) (\d+) (\S+)")

    def test_log_raw_frame_emits_a_parseable_line(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger="qobuz_proxy.connect.ws_manager.raw"):
            _log_raw_frame("SEND", b"\x01\x02\x03")

        [record] = caplog.records
        match = self._RAW_WS_RE.match(record.message)
        assert match is not None
        direction, epoch_ms, b64 = match.groups()
        assert direction == "SEND"
        assert base64.b64decode(b64) == b"\x01\x02\x03"
        # Recent enough to be "now", not a stale/garbage placeholder.
        assert abs(int(time.time() * 1000) - int(epoch_ms)) < 5000

    def test_log_raw_frame_round_trips_arbitrary_bytes(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Including bytes that would break a naive string encoding —
        base64 must be what actually carries this, not repr() or similar."""
        raw = bytes(range(256))
        with caplog.at_level(logging.INFO, logger="qobuz_proxy.connect.ws_manager.raw"):
            _log_raw_frame("RECV", raw)

        [record] = caplog.records
        match = self._RAW_WS_RE.match(record.message)
        assert match is not None
        assert base64.b64decode(match.group(3)) == raw

    @pytest.mark.asyncio
    async def test_raw_send_logs_then_sends(
        self, ws_manager: WsManager, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_ws = AsyncMock()
        ws_manager._ws = mock_ws

        with caplog.at_level(logging.INFO, logger="qobuz_proxy.connect.ws_manager.raw"):
            await ws_manager._raw_send(b"\xaa\xbb\xcc")

        mock_ws.send.assert_awaited_once_with(b"\xaa\xbb\xcc")
        [record] = caplog.records
        match = self._RAW_WS_RE.match(record.message)
        assert match is not None
        assert match.group(1) == "SEND"
        assert base64.b64decode(match.group(3)) == b"\xaa\xbb\xcc"

    @pytest.mark.asyncio
    async def test_send_message_traced_goes_through_raw_send(
        self, ws_manager: WsManager, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Every public send path funnels through _raw_send — spot-check
        one (send_message) rather than every call site individually."""
        mock_ws = AsyncMock()
        ws_manager._ws = mock_ws
        ws_manager._is_connected = True

        with caplog.at_level(logging.INFO, logger="qobuz_proxy.connect.ws_manager.raw"):
            await ws_manager.send_message(b"hello")

        [record] = caplog.records
        assert self._RAW_WS_RE.match(record.message) is not None


class TestStartStop:
    """Tests for start/stop behavior."""

    @pytest.mark.asyncio
    async def test_start_without_tokens_logs_error(self, ws_manager: WsManager) -> None:
        """Test that starting without tokens doesn't crash."""
        # Should log error and return, not raise
        await ws_manager.start()
        # Manager should not be running
        assert ws_manager._should_run is False or ws_manager._receive_task is None

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, ws_manager: WsManager) -> None:
        """Test that stopping when not running is safe."""
        await ws_manager.stop()
        assert ws_manager._should_run is False

    @pytest.mark.asyncio
    async def test_stop_sends_disconnect_before_closing(self, ws_manager: WsManager) -> None:
        """A real Speaker.stop() (process shutdown, or Sonos topology loss
        tearing down a departed room) must tell the server this specific
        connection is gone for good — see ProtocolCodec.encode_disconnect.
        Without it, this proxy's own past device identities keep showing
        up as permanently stale, unusable renderers (live-confirmed
        2026-09-12)."""
        mock_ws = AsyncMock()
        ws_manager._ws = mock_ws

        await ws_manager.stop()

        assert mock_ws.send.await_count == 1
        sent_frame = mock_ws.send.await_args[0][0]
        assert sent_frame[0] == MessageType.DISCONNECT
        mock_ws.close.assert_awaited_once()
        # Sent before closing, not after or racing it.
        assert mock_ws.method_calls.index(("send", (sent_frame,), {})) < mock_ws.method_calls.index(
            ("close", (), {})
        )

    @pytest.mark.asyncio
    async def test_stop_swallows_disconnect_send_errors(self, ws_manager: WsManager) -> None:
        """A connection already unhealthy enough to be getting stopped may
        refuse this final send too — that must never block or fail the
        shutdown itself."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = OSError("connection already gone")
        ws_manager._ws = mock_ws

        await ws_manager.stop()  # must not raise

        mock_ws.close.assert_awaited_once()

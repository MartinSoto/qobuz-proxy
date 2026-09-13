"""
WebSocket connection manager.

Handles connection lifecycle, authentication, and message routing.
"""

import asyncio
import base64
import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

import websockets
from websockets import ClientConnection

from qobuz_proxy.auth.tokens import WSToken
from qobuz_proxy.config import Config
from qobuz_proxy.proto import common as common_pb2

from .protocol import DecodedMessage, MessageType, ProtocolCodec
from .types import ConnectTokens

if TYPE_CHECKING:
    from qobuz_proxy.auth.api_client import QobuzAPIClient

logger = logging.getLogger(__name__)

# Connection constants
PING_INTERVAL = 10.0  # seconds
PONG_TIMEOUT = 30.0  # seconds
RECV_TIMEOUT = 1.0  # seconds (for periodic checks)
TOKEN_REFRESH_BUFFER = 60  # seconds before expiry
INITIAL_RECONNECT_DELAY = 1.0  # seconds
MAX_RECONNECT_DELAY = 60.0  # seconds
RECONNECT_BACKOFF_MULTIPLIER = 2.0
# How often to retry a failed self-refresh while otherwise waiting on the
# app — mirrors the reference device's own token-heartbeat cadence.
SELF_REFRESH_RETRY_SECONDS = 30.0

# How long force_reconnect() holds the connection closed before letting the
# normal reconnect cycle resume — see its docstring.
HIJACK_RECONNECT_HOLD_SECONDS = 8.0

# A SRVR_CTRL_ADD_RENDERER entry for a Sonos-backed speaker used to never
# get reaped once its WS connection ended (2026-09-12 investigation,
# against a real household): a plain graceful close and an explicit
# application-level DISCONNECT(reconnect=False) sent right before it
# (see WsManager.stop's _send_disconnect) both produced an identical,
# permanently-stale result — at the time, traced to the Controller-role
# join using a second, synthetic deviceUuid distinct from the Renderer's
# own, which apparently left the server unable to tell which single
# identity a given connection really represented. Once the Controller
# join was switched to reuse the Renderer's own deviceUuid (see
# _send_ctrl_join_session), a room reselected and used again *did*
# properly reclaim its old, previously-stale identity and then
# disappeared instantly on a later clean stop — matching real-client
# behavior for the first time. Not fully generalized yet: identities
# only ever passively probed (is_active=False, see git history around
# 2026-09-12) picked up a renamed display but were never actually
# reactivated, and a pure Controller-only ghost from before this fix
# (no Renderer role at all) has no path back — nothing will ever join
# under that synthetic identity again. generate_sonos_speaker_uuid is
# deliberately deterministic per Sonos group_id, so any surviving stale
# ghost shares not just a name but the literal same deviceUuid with its
# live counterpart. A "LIVE - " name prefix was carried on every live
# join for a while as a way to tell the two apart in the app's picker;
# removed once that stopped being needed day to day (see git history
# around 2026-09-13 for the prefix itself, if it's ever worth reviving).
#
# A tempting-looking further step — deliberately claiming is_active=True
# for a second, already-known deviceUuid on top of an already-live
# connection, to try to reclaim a specific stale entry on demand — was
# tried and abandoned: the server tracks a strict one-to-one link between
# a socket and "the identity it currently represents", so the claim
# *reassigns* that link instead of adding to it. The room the connection
# was actually representing gets orphaned in exactly the same way the
# instant the claim lands. It relocates staleness rather than curing it,
# so there's no code for it here — reselecting a room for real, the way
# the app already does, is the only supported way to reclaim it.

# Message handler callback type
MessageHandler = Callable[[int, Any], None]

# Server -> Controller queue/session message types the proxy doesn't drive
# playback from. Originally believed Renderer-unreachable entirely (see
# queue.py's module docstring) — wrong, see command_handler.py's
# MSG_TYPE_QUEUE_TRACKS_LOADED: type 91 both arrives (because of the dual
# Renderer+Controller join) and turned out to be the real "play this
# track" signal, confirmed live 2026-09-13
# (docs/connect-activation-model-hypotheses.md) and now handled by
# PlaybackCommandHandler — removed from this dict since a real handler is
# registered for it and this fallback branch can no longer fire for it.
# The rest are kept as a standing, zero-cost tripwire logging a clearly
# labeled line (rather than blending into the generic "no handler"
# fallback below) in case one of them turns out to matter too.
# Deliberately observation-only otherwise: no parsing, no state kept.
# Names mirror the QConnectMessageType enum (qconnect_payload.proto) — kept
# as a plain literal dict rather than looked up via the generated pb2 module
# (untyped: no stubs installed for google.protobuf, same gap protocol.py
# already lives with for every other pb2 attribute access) so this doesn't
# add a new mypy attr-defined error to a file that otherwise has none.
_QUEUE_POSITION_MESSAGE_TYPES = {
    81: "SRVR_CTRL_SESSION_STATE",  # trackIndex
    89: "SRVR_CTRL_QUEUE_CLEARED",
    90: "SRVR_CTRL_QUEUE_STATE",  # full ordered queue snapshot
    92: "SRVR_CTRL_QUEUE_TRACKS_INSERTED",
    93: "SRVR_CTRL_QUEUE_TRACKS_ADDED",
    94: "SRVR_CTRL_QUEUE_TRACKS_REMOVED",
    95: "SRVR_CTRL_QUEUE_TRACKS_REORDERED",
    105: "SRVR_CTRL_QUEUE_VERSION_CHANGED",
}

# TEMP, investigative (2026-09-13) — a dedicated logger for the raw bytes
# of every WS frame this proxy sends or receives, one line each, so a
# capture from a real session can be diffed against a browser HAR of the
# web player over the same protocol — see scripts/qconnect_trace.py,
# which decodes both into the same normalized, diff-friendly text format.
# A separate logger (rather than reusing `logger` above) so this can be
# silenced independently if it gets too noisy; propagates to the root
# logger by default, so it lands in the normal log output unless
# something explicitly configures it otherwise. Deliberately raw: no
# decoding here, just base64(frame bytes) — decoding on the way in would
# risk hiding exactly the kind of malformed-or-unexpected frame this
# exists to catch.
_raw_trace_logger = logging.getLogger("qobuz_proxy.connect.ws_manager.raw")


def _log_raw_frame(direction: str, data: bytes) -> None:
    """Emit one RAW_WS line for `data` — see _raw_trace_logger's own
    comment. `direction` is "SEND" or "RECV". Format:
    ``RAW_WS <direction> <epoch_ms> <base64>`` — parsed back out by
    scripts/qconnect_trace.py's --proxy-log mode via a plain regex, so
    keep this exact shape (three space-separated fields after "RAW_WS")
    if it ever needs to change."""
    epoch_ms = int(time.time() * 1000)
    encoded = base64.b64encode(data).decode("ascii")
    _raw_trace_logger.info(f"RAW_WS {direction} {epoch_ms} {encoded}")


class TokenRefreshRequired(Exception):
    """Raised when the WebSocket must wait for refreshed tokens from the app."""


class WsManager:
    """
    Manages WebSocket connection to Qobuz servers.

    Handles:
    - Connection establishment and authentication
    - Automatic reconnection with exponential backoff
    - Message encoding/decoding via ProtocolCodec
    - Routing incoming messages to registered handlers
    """

    def __init__(
        self,
        config: Config,
        api_client: Optional["QobuzAPIClient"] = None,
        is_worth_keeping_alive: Optional[Callable[[], bool]] = None,
    ):
        """
        Initialize WebSocket manager.

        Args:
            config: Application configuration
            api_client: Shared Qobuz API client, if available — lets this
                manager refresh its own WS token directly (see
                _refresh_token_from_api) instead of only ever waiting for
                the phone app to reconnect and push a new one. None (e.g.
                in tests) simply disables that and falls back to the old
                app-only behavior.
            is_worth_keeping_alive: Queried before every self-refresh (see
                _refresh_token_from_api) — lets the caller (Player, via
                is_worth_keeping_ws_alive) say "don't bother, nothing's
                listening right now" so a long-idle session's token lapses
                naturally instead of being kept alive forever in the
                background. None (e.g. in tests, or when no such policy is
                wired up) always keeps refreshing, matching prior behavior.
        """
        self.config = config
        self._api_client = api_client
        self._is_worth_keeping_alive = is_worth_keeping_alive
        self._device_uuid = self._uuid_to_bytes(config.device.uuid)
        self._codec = ProtocolCodec(self._device_uuid)

        # Connection state
        self._ws: Optional[ClientConnection] = None
        self._ws_token: Optional[WSToken] = None
        self._session_uuid: Optional[bytes] = None
        self._is_connected = False
        self._should_run = False

        # Reconnection state
        self._reconnect_delay = INITIAL_RECONNECT_DELAY

        # Token refresh state
        self._token_update_event = asyncio.Event()
        self._token_version = 0

        # Message handlers: message_type -> handler
        self._handlers: Dict[int, MessageHandler] = {}

        # Outgoing message queue (for messages during disconnect)
        self._pending_messages: list[bytes] = []

        # Tasks
        self._receive_task: Optional[asyncio.Task[None]] = None

        # Callbacks
        self._on_connected: Optional[Callable[[], None]] = None
        self._on_disconnected: Optional[Callable[[], None]] = None

        # Quality setting for join session message
        self._max_audio_quality: int = 27  # Default to Hi-Res 192k

        # Whether the *next* JoinSession should (re)claim active-renderer
        # status — a one-shot override, reset to True right after each
        # join. False only for the join right after force_reconnect(); see
        # its docstring and encode_join_session()'s is_active parameter.
        self._next_join_claims_active: bool = True

    def set_tokens(self, tokens: ConnectTokens) -> None:
        """
        Set connection tokens from discovery service.

        Args:
            tokens: Tokens received from Qobuz app
        """
        previous_token = self._ws_token
        previous_session_uuid = self._session_uuid

        if tokens.ws_token:
            self._ws_token = WSToken.from_connect_token(
                jwt=tokens.ws_token.jwt,
                exp=tokens.ws_token.exp,
                endpoint=tokens.ws_token.endpoint,
            )
        self._session_uuid = self._uuid_to_bytes(tokens.session_id)
        self._token_version += 1
        self._token_update_event.set()

        if self._ws_token:
            logger.debug(f"Tokens set, endpoint: {self._ws_token.endpoint[:50]}...")

        tokens_changed = (
            previous_token != self._ws_token or previous_session_uuid != self._session_uuid
        )
        if tokens_changed and self._ws and self._should_run:
            logger.info("Received refreshed WebSocket tokens, reconnecting")
            asyncio.create_task(self._close_for_token_refresh())

    def set_max_audio_quality(self, quality: int) -> None:
        """
        Set max audio quality for join session message.

        Args:
            quality: Quality ID (5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k)
        """
        self._max_audio_quality = quality

    def on_connected(self, callback: Callable[[], None]) -> None:
        """Register callback for successful connection."""
        self._on_connected = callback

    def on_disconnected(self, callback: Callable[[], None]) -> None:
        """Register callback for disconnection."""
        self._on_disconnected = callback

    def register_handler(self, message_type: int, handler: MessageHandler) -> None:
        """
        Register a handler for a specific QConnect message type.

        Args:
            message_type: QConnectMessage type code (e.g., 41 for SET_STATE)
            handler: Callback function(message_type, message_data)
        """
        self._handlers[message_type] = handler
        logger.debug(f"Registered handler for message type {message_type}")

    async def start(self) -> None:
        """Start WebSocket connection loop."""
        if not self._ws_token or not self._ws_token.is_valid():
            logger.error("Cannot start WsManager: no valid tokens")
            return

        self._should_run = True
        self._receive_task = asyncio.create_task(self._connection_loop())
        logger.info("WebSocket manager started")

    async def stop(self) -> None:
        """Stop WebSocket connection."""
        self._should_run = False
        if self._ws:
            await self._send_disconnect()
            await self._ws.close()
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        logger.info("WebSocket manager stopped")

    async def _raw_send(self, data: bytes) -> None:
        """The one chokepoint every outgoing frame goes through — see
        _log_raw_frame's own comment. Every self._ws.send(...) call site
        in this class goes through here instead of calling send()
        directly, so the raw-trace log is complete: nothing this proxy
        sends is invisible to it. Assumes self._ws is set; callers that
        already check for None (most of them do, for their own reasons)
        keep doing so before calling this."""
        _log_raw_frame("SEND", data)
        await self._ws.send(data)

    async def _send_disconnect(self) -> None:
        """Tell the server this connection is going away for good, right
        before closing it — see ProtocolCodec.encode_disconnect for why
        this matters (stale, permanently-unusable renderer entries).
        Best-effort: a connection already unhealthy enough to be getting
        stopped may not accept one final send either, and that must never
        block or fail the shutdown itself."""
        if not self._ws:
            return
        try:
            await self._raw_send(self._codec.encode_disconnect(reconnect=False))
            logger.info("Sent DISCONNECT (reconnect=False)")
        except Exception as e:
            logger.debug(f"Failed to send DISCONNECT before close: {e}")

    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._is_connected

    async def force_reconnect(
        self, reason: str, hold_seconds: float = HIJACK_RECONNECT_HOLD_SECONDS
    ) -> None:
        """
        Close the current connection and hold off reconnecting for
        hold_seconds before the normal reconnect cycle resumes.

        The Qobuz Connect protocol has no message for a renderer to
        voluntarily give up "active" status (confirmed against two
        independent reverse-engineered implementations — neither
        documents or sends one), and neither does it for a device that
        crashes or is powered off — so the server almost certainly relies
        on its own WS-level heartbeat/timeout to notice a renderer is
        gone. An immediate reconnect is indistinguishable from a network
        blip and likely never crosses that threshold; holding the
        connection closed for a while gives it a real chance to fire.
        Used when this renderer detects it's no longer really in
        control — see Player's external-takeover ("hijack") detection.

        Reuses the existing backoff mechanism rather than adding new
        timing state: setting _reconnect_delay directly makes
        _connection_loop's very next sleep — which runs right after
        _connect_and_run() returns from the close() below — wait exactly
        this long. It resets to normal on the next successful connect,
        same as any other reconnect.

        Confirmed live: holding the connection closed does make the app
        fall back to local playback — but by default a rejoin claims
        isActive again (see encode_join_session()), and the server
        recognizes the *same* renderer coming back (deviceUuid/
        sessionUuid — not the display name, which can change anytime) and
        hands it right back control, evicting whatever the app fell back
        to. So the rejoin that follows this specific reconnect is made to
        *not* reclaim active status.
        """
        if not self._ws:
            return
        logger.info(f"Forcing WebSocket reconnect: {reason} (holding {hold_seconds:.0f}s)")
        self._reconnect_delay = hold_seconds
        self._next_join_claims_active = False
        try:
            await self._ws.close()
        except Exception as e:
            logger.debug(f"Failed to close WebSocket for forced reconnect: {e}")

    async def send_message(self, data: bytes) -> bool:
        """
        Send a pre-encoded message.

        Args:
            data: Encoded frame bytes

        Returns:
            True if sent, False if queued for later
        """
        if self._ws and self._is_connected:
            try:
                await self._raw_send(data)
                return True
            except Exception as e:
                logger.error(f"Failed to send message: {e}")

        # Queue for when connection is restored
        self._pending_messages.append(data)
        return False

    async def send_state_update(
        self,
        playing_state: int,
        buffer_state: int,
        position_ms: int,
        duration_ms: int,
        queue_item_id: int,
        queue_version_major: int,
        queue_version_minor: int,
        position_timestamp_ms: Optional[int] = None,
    ) -> bool:
        """
        Send renderer state update.

        Returns:
            True if sent successfully
        """
        data = self._codec.encode_state_update(
            playing_state=playing_state,
            buffer_state=buffer_state,
            position_ms=position_ms,
            duration_ms=duration_ms,
            queue_item_id=queue_item_id,
            queue_version_major=queue_version_major,
            queue_version_minor=queue_version_minor,
            position_timestamp_ms=position_timestamp_ms,
        )
        return await self.send_message(data)

    async def send_device_info_updated(self, friendly_name: str) -> bool:
        """
        Rename this renderer on an already-joined session, in place.

        Unlike reconnecting with a new JOIN_SESSION, this doesn't drop the
        WebSocket or reset playback state — the app just relabels the
        device. Also updates config.device.name so a future reconnect's
        JOIN_SESSION (if one happens) already carries the new name.

        Args:
            friendly_name: New device display name

        Returns:
            True if sent successfully
        """
        self.config.device.name = friendly_name
        data = self._codec.encode_device_info_updated(
            device_uuid=self._device_uuid,
            friendly_name=friendly_name,
            max_audio_quality=self._max_audio_quality,
        )
        return await self.send_message(data)

    async def send_volume_changed(self, volume: int) -> bool:
        """
        Send volume changed notification.

        Args:
            volume: Volume level 0-100

        Returns:
            True if sent successfully
        """
        data = self._codec.encode_volume_changed(volume)
        return await self.send_message(data)

    async def send_file_audio_quality_changed(
        self,
        quality: int,
        sampling_rate: int = 0,
        bit_depth: int = 0,
        nb_channels: int = 0,
    ) -> bool:
        """
        Send file audio quality changed notification.

        Args:
            quality: Quality ID (5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k)
            sampling_rate: Sample rate in Hz (e.g. 44100, 96000). 0 = derive from quality.
            bit_depth: Bit depth (16 or 24). 0 = derive from quality.
            nb_channels: Number of channels. 0 = derive from quality.

        Returns:
            True if sent successfully
        """
        from qobuz_proxy.connect.protocol import QUALITY_TO_PROTOCOL

        proto_quality = QUALITY_TO_PROTOCOL.get(quality, 4)
        logger.debug(
            f"Sending FILE_AUDIO_QUALITY_CHANGED: qobuz={quality} -> proto={proto_quality}, "
            f"sr={sampling_rate}, bd={bit_depth}, ch={nb_channels}"
        )
        data = self._codec.encode_file_audio_quality_changed(
            quality,
            sampling_rate=sampling_rate,
            bit_depth=bit_depth,
            nb_channels=nb_channels,
        )
        return await self.send_message(data)

    async def send_device_audio_quality_changed(
        self,
        quality: int,
        sampling_rate: int = 0,
        bit_depth: int = 0,
        nb_channels: int = 0,
    ) -> bool:
        """
        Send device audio quality changed notification.

        Args:
            quality: Quality ID (5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k)
            sampling_rate: Max sample rate in Hz. 0 = derive from quality.
            bit_depth: Max bit depth. 0 = derive from quality.
            nb_channels: Number of channels. 0 = derive from quality.

        Returns:
            True if sent successfully
        """
        from qobuz_proxy.connect.protocol import QUALITY_TO_PROTOCOL

        proto_quality = QUALITY_TO_PROTOCOL.get(quality, 4)
        logger.debug(
            f"Sending DEVICE_AUDIO_QUALITY_CHANGED: qobuz={quality} -> proto={proto_quality}, "
            f"sr={sampling_rate}, bd={bit_depth}, ch={nb_channels}"
        )
        data = self._codec.encode_device_audio_quality_changed(
            quality,
            sampling_rate=sampling_rate,
            bit_depth=bit_depth,
            nb_channels=nb_channels,
        )
        return await self.send_message(data)

    async def send_max_audio_quality_changed(self, quality: int, network_type: int = 1) -> bool:
        """
        Send max audio quality changed notification.

        Args:
            quality: Quality ID (5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k)
            network_type: Network type (1=WiFi)

        Returns:
            True if sent successfully
        """
        from qobuz_proxy.connect.protocol import QUALITY_TO_PROTOCOL

        proto_quality = QUALITY_TO_PROTOCOL.get(quality, 4)
        logger.debug(f"Sending MAX_AUDIO_QUALITY_CHANGED: qobuz={quality} -> proto={proto_quality}")
        data = self._codec.encode_max_audio_quality_changed(quality, network_type=network_type)
        return await self.send_message(data)

    # -------------------------------------------------------------------------
    # Connection Loop
    # -------------------------------------------------------------------------

    async def _connection_loop(self) -> None:
        """Main connection loop with reconnection logic."""
        while self._should_run:
            should_backoff = True
            try:
                await self._connect_and_run()
            except TokenRefreshRequired:
                should_backoff = False
            except Exception as e:
                logger.error(f"Connection error: {e}")

            if not self._should_run:
                break

            # Notify disconnection
            if self._on_disconnected:
                self._on_disconnected()

            if not should_backoff:
                continue

            # Exponential backoff
            logger.info(f"Reconnecting in {self._reconnect_delay:.1f}s...")
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * RECONNECT_BACKOFF_MULTIPLIER,
                MAX_RECONNECT_DELAY,
            )

    async def _connect_and_run(self) -> None:
        """Connect, authenticate, and handle messages."""
        if not await self._wait_for_valid_token(buffer_s=TOKEN_REFRESH_BUFFER):
            return

        assert self._ws_token is not None
        endpoint = self._ws_token.endpoint
        logger.info(f"Connecting to {endpoint[:50]}...")

        try:
            async with websockets.connect(
                endpoint,
                origin="https://play.qobuz.com",
                subprotocols=["qws"],
                ping_interval=PING_INTERVAL,
                ping_timeout=PONG_TIMEOUT,
            ) as ws:
                self._ws = ws

                # Authenticate
                if not await self._authenticate():
                    return

                # Subscribe to session
                if not await self._subscribe():
                    return

                # Send join session message
                await self._send_join_session()

                # Also join as Controller — see _send_ctrl_join_session's
                # own docstring for why this is needed at all.
                await self._send_ctrl_join_session()

                self._is_connected = True
                self._reconnect_delay = INITIAL_RECONNECT_DELAY  # Reset backoff
                logger.info("Connected and authenticated")

                # Notify connected callback
                if self._on_connected:
                    self._on_connected()

                # Send any queued messages
                await self._flush_pending_messages()

                # Message receive loop
                await self._receive_loop()

        except TokenRefreshRequired:
            raise
        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e.code} {e.reason}")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
        finally:
            self._is_connected = False
            self._ws = None

    async def _authenticate(self) -> bool:
        """Send AUTHENTICATE message."""
        if not self._ws_token:
            return False
        auth_frame = self._codec.encode_authenticate(self._ws_token.jwt)
        await self._raw_send(auth_frame)
        logger.debug("Sent AUTHENTICATE")
        return True

    async def _subscribe(self) -> bool:
        """Send SUBSCRIBE message."""
        if not self._session_uuid:
            logger.error("No session UUID for subscribe")
            return False

        sub_frame = self._codec.encode_subscribe(self._session_uuid)
        await self._raw_send(sub_frame)
        logger.debug("Sent SUBSCRIBE")
        return True

    async def _send_join_session(self) -> None:
        """Send join session message to register as renderer."""
        if not self._session_uuid:
            logger.error("No session UUID for join session")
            return

        # REVERTED 2026-09-13 — see docs/connect-activation-model-hypotheses.md,
        # "The isActive=false experiment broke real device selection
        # entirely". A brief attempt gated this on Player.has_resumable_playback()
        # to stop a fresh process start from getting fed a stale server-
        # side restore. Confirmed live to be worse: with isActive=false,
        # SRVR_RNDR_SET_ACTIVE never arrives at all — not for a fresh
        # start, and not even for a brand-new device identity the app had
        # just explicitly selected via connect-to-qconnect. Every prior
        # capture that claimed isActive=true got SET_ACTIVE(true) back
        # within ~50ms; this is apparently the actual mechanism by which
        # the app's direct local-network device selection becomes a real
        # activation, not just a courtesy "resume my last state" hint —
        # there's no separate confirmation step to wait for instead.
        claims_active = self._next_join_claims_active
        self._next_join_claims_active = True  # one-shot — restore the default for next time

        # Experiment (2026-09-13, docs/connect-activation-model-hypotheses.md):
        # tell the server explicitly that we have nothing of our own
        # playing, rather than leaving initialState unset and letting the
        # server hand back whatever it last remembers for this deviceUuid
        # (the mechanism behind the stale-restore bug that section
        # describes). Every other RendererState field is optional and
        # deliberately left unset here — only playingState is populated.
        initial_state = common_pb2.RendererState()
        initial_state.playingState = common_pb2.PLAYING_STATE_STOPPED

        join_frame = self._codec.encode_join_session(
            device_uuid=self._device_uuid,
            friendly_name=self.config.device.name,
            session_uuid=self._session_uuid,
            initial_state=initial_state,
            max_audio_quality=self._max_audio_quality,
            is_active=claims_active,
        )
        await self._raw_send(join_frame)
        logger.debug(
            f"Sent JOIN_SESSION with max_quality={self._max_audio_quality}, "
            f"is_active={claims_active}"
        )

    async def _send_ctrl_join_session(self) -> None:
        """Join the Controller role too, alongside the existing Renderer
        join above, on this same WebSocket — confirmed against a captured
        browser session that the Qobuz web player does exactly this, and
        it's why it (unlike a pure-Renderer session) receives SRVR_CTRL_*
        broadcasts, including the full ordered queue via
        SRVR_CTRL_QUEUE_TRACKS_LOADED (see queue_diagnostics.py). Listen-
        only: sends the join and nothing else on the Controller channel.

        sessionUuid is deliberately omitted — passing the id this proxy
        was externally handed for its Renderer join gets it rejected
        outright ("join a known session is not implemented yet"; see
        encode_ctrl_join_session's own docstring). There is only ever one
        Connect session per account, and the server resolves it from the
        authenticated connection itself once no sessionUuid is asserted.

        Uses the *same* deviceUuid as the Renderer join, not a separate
        synthetic identity — a second identity was tried first and
        reasoned to be safer, but it's what the phone app and the web
        player do *not* do (both show up as a single entity in the
        device list, confirmed live) and it's what caused this proxy's
        own two-identities-on-one-socket version to intermittently get
        "Renderer state updated message received from non active
        renderer" from the server and to leave the Controller-role
        identity stuck as a permanently orphaned ghost on every
        disconnect. Sharing the identity fixed both."""
        if not self._ws:
            return

        join_frame = self._codec.encode_ctrl_join_session(
            device_uuid=self._device_uuid,
            friendly_name=self.config.device.name,
            max_audio_quality=self._max_audio_quality,
        )
        await self._raw_send(join_frame)
        logger.info("Sent CTRL_SRVR_JOIN_SESSION with no sessionUuid, same deviceUuid as Renderer")

    async def _receive_loop(self) -> None:
        """Receive and dispatch messages."""
        while self._should_run and self._ws:
            try:
                data = await asyncio.wait_for(
                    self._ws.recv(),
                    timeout=RECV_TIMEOUT,
                )
                _log_raw_frame("RECV", data.encode() if isinstance(data, str) else data)
                await self._handle_message(data)

            except asyncio.TimeoutError:
                # Check token refresh
                if self._ws_token and self._ws_token.is_expired(TOKEN_REFRESH_BUFFER):
                    logger.warning("Token expiring soon, need refresh")
                    raise TokenRefreshRequired()

            except websockets.ConnectionClosed:
                raise

    async def _handle_message(self, data: bytes) -> None:
        """Decode and route incoming message."""
        decoded = self._codec.decode_frame(data)
        if not decoded:
            return

        if decoded.msg_type == MessageType.PAYLOAD:
            await self._handle_payload(decoded)
        elif decoded.msg_type == MessageType.ERROR:
            logger.error(f"Server error {decoded.error_code}: {decoded.error_message}")
        elif decoded.msg_type == MessageType.DISCONNECT:
            logger.warning("Server requested disconnect")
            raise websockets.ConnectionClosed(None, None)

    async def _handle_payload(self, decoded: DecodedMessage) -> None:
        """Handle PAYLOAD message by routing to registered handlers."""
        if not decoded.payload:
            return

        batch = self._codec.decode_qconnect_batch(decoded.payload)
        if not batch:
            return

        for msg in batch.messages:
            msg_type = msg.messageType
            handler = self._handlers.get(msg_type)
            if handler:
                try:
                    handler(msg_type, msg)
                except Exception as e:
                    logger.error(f"Handler error for type {msg_type}: {e}")
            elif msg_type in _QUEUE_POSITION_MESSAGE_TYPES:
                # See _QUEUE_POSITION_MESSAGE_TYPES — logged plainly (info,
                # not warning: expected to arrive, not an error) so these
                # are easy to spot and inspect if/when they show up.
                type_name = _QUEUE_POSITION_MESSAGE_TYPES[msg_type]
                logger.info(f"Observed queue message {type_name} (type {msg_type}): {msg}")
            else:
                logger.warning(f"No handler for message type {msg_type}: {msg}")

    async def _flush_pending_messages(self) -> None:
        """Send any messages queued during disconnect."""
        if not self._pending_messages:
            return

        logger.debug(f"Flushing {len(self._pending_messages)} pending messages")
        for data in self._pending_messages:
            try:
                await self._raw_send(data)
            except Exception as e:
                logger.error(f"Failed to flush message: {e}")

        self._pending_messages.clear()

    async def _wait_for_valid_token(self, buffer_s: int = 0) -> bool:
        """Wait until a non-expired token is available or shutdown is requested.

        Tries to refresh the token itself first, via the same Qobuz API
        credentials used everywhere else (see _refresh_token_from_api) —
        the control WebSocket no longer has to depend on the phone app
        proactively reconnecting to stay alive (see
        docs/gapless-arming-and-token-expiry.md for what used to happen
        when it didn't: nothing here could recover on its own, and a
        gapless transition needing a live SET_STATE to arm the next track
        would silently fail until the app came back — sometimes minutes
        or hours later, e.g. once its own OS stopped suspending it). Falls
        back to waiting on the app — retrying the self-refresh
        periodically in the meantime — when that isn't possible (no
        api_client wired up, or we've never had a session at all yet: the
        very first token can only ever come from the app) or keeps
        failing.
        """
        logged_wait = False

        while self._should_run:
            if (
                self._ws_token
                and self._ws_token.is_valid()
                and not self._ws_token.is_expired(buffer_s)
            ):
                return True

            if await self._refresh_token_from_api(buffer_s):
                continue

            if not logged_wait:
                logger.warning("Token expired, waiting for refreshed token from Qobuz app")
                logged_wait = True

            token_version = self._token_version
            self._token_update_event.clear()
            if self._token_version != token_version:
                continue
            try:
                await asyncio.wait_for(
                    self._token_update_event.wait(), timeout=SELF_REFRESH_RETRY_SECONDS
                )
            except asyncio.TimeoutError:
                pass

        return False

    async def _refresh_token_from_api(self, buffer_s: int) -> bool:
        """Try to refresh the WS token directly from the Qobuz API instead
        of waiting for the app to push one — see QobuzAPIClient.get_ws_token().

        Only attempted once we've had at least one real session from the
        app (self._session_uuid set): the Connect session identity itself
        is still something only the app can hand us; this only ever
        replaces the *token*, keeping that same session alive indefinitely
        instead of needing the app to reconnect every time it expires.

        Also skipped outright when is_worth_keeping_alive says nothing's
        actually listening right now (see __init__) — we simply don't
        renew, letting the current token lapse on its own rather than
        holding the Connect session open forever in the background.
        """
        if self._api_client is None or self._session_uuid is None:
            return False
        if self._is_worth_keeping_alive is not None and not self._is_worth_keeping_alive():
            return False

        previous_jwt = self._ws_token.jwt if self._ws_token else ""
        try:
            result = await self._api_client.get_ws_token(previous_jwt=previous_jwt)
        except Exception as e:
            logger.warning(f"Self-refresh of WS token failed: {e}")
            return False
        if result is None:
            return False

        new_token = WSToken.from_connect_token(
            jwt=result["jwt"], exp=result["exp"], endpoint=result["endpoint"]
        )
        if not new_token.is_valid() or new_token.is_expired(buffer_s):
            logger.warning("Self-refreshed WS token came back invalid or already expiring")
            return False

        self._ws_token = new_token
        self._token_version += 1
        logger.info("Refreshed WebSocket token directly from Qobuz API")
        return True

    async def _close_for_token_refresh(self) -> None:
        """Close the current connection so refreshed tokens are used immediately."""
        if not self._ws:
            return

        try:
            await self._ws.close()
        except Exception as e:
            logger.debug(f"Failed to close WebSocket after token refresh: {e}")

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def _uuid_to_bytes(self, uuid_str: str) -> bytes:
        """Convert UUID string to 16 bytes."""
        try:
            return uuid.UUID(uuid_str).bytes
        except ValueError:
            # Fallback: hash the string
            import hashlib

            return hashlib.md5(uuid_str.encode()).digest()

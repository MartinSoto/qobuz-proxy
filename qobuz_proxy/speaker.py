"""
Speaker — per-speaker lifecycle manager.

Bundles all per-speaker components (discovery, WebSocket, player, backend)
and manages their startup and teardown. Multiple Speaker instances can be
run concurrently, one per physical audio device.
"""

import asyncio
import functools
import logging
from typing import Optional

from aiohttp import web

from qobuz_proxy.config import (
    AUTO_FALLBACK_QUALITY,
    AUTO_QUALITY,
    BackendConfig,
    Config,
    DeviceConfig,
    DLNAConfig,
    LocalConfig,
    LoggingConfig,
    QobuzConfig,
    ServerConfig,
    SpeakerConfig,
)
from qobuz_proxy.auth import QobuzAPIClient
from qobuz_proxy.connect import ConnectTokens, DiscoveryService, WsManager
from qobuz_proxy.playback import (
    MetadataService,
    PlaybackCommandHandler,
    QobuzPlayer,
    QobuzQueue,
    QueueHandler,
    StateReporter,
    VolumeCommandHandler,
)
from qobuz_proxy.playback.command_handler import MSG_TYPE_SET_STATE
from qobuz_proxy.backends import AudioBackend, BackendFactory, PlaybackState
from qobuz_proxy.playback.play_reporter import PlayReporter
from qobuz_proxy.playback.state_reporter import PlaybackStateReport
from qobuz_proxy.backends.dlna import AudioProxyServer, DLNABackend, MetadataServiceURLProvider

logger = logging.getLogger(__name__)


class Speaker:
    """
    Self-contained per-speaker component bundle.

    Accepts a SpeakerConfig and shared resources (api_client, app_id),
    then manages the full lifecycle of all components needed to operate
    one Qobuz Connect device.
    """

    def __init__(
        self,
        config: SpeakerConfig,
        api_client: QobuzAPIClient,
        app_id: str,
        web_app: Optional[web.Application] = None,
    ) -> None:
        """
        Initialize Speaker.

        Args:
            config: Per-speaker configuration
            api_client: Authenticated Qobuz API client (shared across speakers)
            app_id: Qobuz application ID (shared across speakers)
            web_app: Optional shared aiohttp Application for discovery routes
        """
        self._config = config
        self._api_client = api_client
        self._app_id = app_id
        self._web_app = web_app

        self._is_running: bool = False
        self._ws_connected_event: asyncio.Event = asyncio.Event()
        self._ws_setup_lock: asyncio.Lock = asyncio.Lock()

        # Effective quality (may differ from config when AUTO_QUALITY is resolved)
        self._effective_quality: int = config.max_quality
        # Where the effective quality came from: "manual", "auto" (detected from
        # the device), or "auto_fallback" (device never said — conservative CD)
        self._quality_source: str = (
            "manual" if config.max_quality != AUTO_QUALITY else "auto_fallback"
        )

        # Component slots — populated during start()
        self._discovery: Optional[DiscoveryService] = None
        self._ws_manager: Optional[WsManager] = None
        self._metadata_service: Optional[MetadataService] = None
        self._queue: Optional[QobuzQueue] = None
        self._player: Optional[QobuzPlayer] = None
        self._backend: Optional[AudioBackend] = None
        self._proxy_server: Optional[AudioProxyServer] = None
        self._state_reporter: Optional[StateReporter] = None

        # Command handlers
        self._queue_handler: Optional[QueueHandler] = None
        self._playback_handler: Optional[PlaybackCommandHandler] = None
        self._volume_handler: Optional[VolumeCommandHandler] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Human-readable speaker name (used as Qobuz Connect device name)."""
        return self._config.name

    @property
    def is_active(self) -> bool:
        """Whether the Qobuz server currently considers this Speaker the
        active playback target (SrvrRndrSetActive) — the authoritative
        signal for "is this the renderer actually being played to right
        now", as opposed to one that's merely discovered and available. At
        most one Speaker is ever active at a time."""
        return self._player is not None and self._player.is_active_renderer

    async def rename(self, new_name: str) -> bool:
        """
        Rename this speaker in place, without restarting playback.

        Updates the mDNS advertisement and, if a Qobuz Connect session is
        currently joined, sends DEVICE_INFO_UPDATED so the app relabels the
        device without a reconnect — unlike a full stop/start, this never
        drops the WebSocket or resets playback state/position. Safe to call
        repeatedly with the same result (e.g. on a retried failure).

        Returns:
            True once applied (or already matching — a no-op).
        """
        if new_name == self._config.name:
            return True

        self._config.name = new_name

        # Independent steps: a failure in one (e.g. mDNS) must not skip the
        # other (e.g. the live app-visible WS rename) — both are safe to
        # retry, so on any failure the caller retries the whole call, but
        # each attempt still does everything it can this time around.
        ok = True

        if self._discovery:
            try:
                await self._discovery.update_name(new_name)
            except Exception as e:
                logger.warning(f"Failed to update mDNS name to '{new_name}': {e}")
                ok = False

        if self._ws_manager and self._ws_manager.is_connected:
            try:
                await self._ws_manager.send_device_info_updated(new_name)
            except Exception as e:
                logger.warning(f"Failed to send device info update for '{new_name}': {e}")
                ok = False

        if ok:
            logger.info(f"[{new_name}] Renamed")
        return ok

    async def detach(self) -> None:
        """
        Give up the current physical target without tearing anything else
        down — the Qobuz Connect session (WebSocket, mDNS registration),
        Player, and queue all stay alive. Used when this speaker's Sonos
        group_id has gone pending (SonosDiscoveryManager can't yet tell
        whether it's a real loss or a handoff in progress): if this speaker
        is the one actually being driven, leaving it pointed at a
        coordinator that may already be transitioning out of the group
        risks two physical devices both thinking they're in charge at once.
        A later retarget() (see AudioBackend.retarget()) reconnects
        cleanly from this state, even to the same ip/port as before.

        Delegates to Player.detach(), which runs the actual disconnect +
        set_backend_attached(False) as an item on Player's own command
        queue — so it can never overlap a play/pause/seek/etc. call the
        queue's consumer is already in the middle of (see there for why
        that matters). Safe to call before the speaker has finished
        starting (no player yet) — a no-op in that case.
        """
        if self._player:
            await self._player.detach()

    async def retarget(self, ip: str, port: int) -> bool:
        """
        Repoint this speaker's backend at a different physical device, in
        place — the Qobuz Connect session (WebSocket, mDNS registration)
        is untouched. Delegates to Player.retarget(), which runs the
        actual AudioBackend.retarget() call as an item on Player's own
        command queue (see detach() above for why), itself a no-op
        returning False for backends without a meaningful notion of this
        (e.g. local output).

        Returns:
            True if the retarget succeeded.
        """
        if not self._player:
            return False

        ok = await self._player.retarget(ip, port)
        if ok:
            self._config.dlna_ip = ip
            self._config.dlna_port = port
        return ok

    def get_status(self) -> dict:
        """Return rich status dict for API responses."""
        from qobuz_proxy.config import slugify_name

        # Determine playback status
        if not self._is_running:
            playback_status = "disconnected"
        elif self._player and self._player.state == PlaybackState.PLAYING:
            playback_status = "playing"
        elif self._player and self._player.state == PlaybackState.PAUSED:
            playback_status = "paused"
        else:
            playback_status = "idle"

        # Build now_playing if there's a current track with metadata
        now_playing = None
        if self._player and self._player.current_track and playback_status in ("playing", "paused"):
            track = self._player.current_track
            meta = track.metadata
            now_playing = {
                "title": meta.get("title", ""),
                "artist": meta.get("artist", ""),
                "album": meta.get("album", ""),
                "album_art_url": meta.get("artwork_url", ""),
                "quality": meta.get("quality_name", ""),
                "volume": self._player._volume,
            }

        # Build config section
        config_dict: dict = {
            "max_quality": (
                "auto" if self._config.max_quality == AUTO_QUALITY else self._config.max_quality
            ),
            "effective_quality": self._effective_quality,
            "quality_source": self._quality_source,
        }
        if self._config.backend_type == "dlna":
            config_dict["dlna_ip"] = self._config.dlna_ip
            config_dict["dlna_port"] = self._config.dlna_port
            config_dict["description_url"] = self._config.dlna_description_url
            config_dict["fixed_volume"] = self._config.dlna_fixed_volume
        elif self._config.backend_type == "local":
            config_dict["audio_device"] = self._config.audio_device
            config_dict["buffer_size"] = self._config.audio_buffer_size

        return {
            "id": slugify_name(self._config.name),
            "name": self._config.name,
            "backend": self._config.backend_type,
            "status": playback_status,
            "config": config_dict,
            "now_playing": now_playing,
            "auto_managed": self._config.auto_managed,
        }

    def _build_component_config(self) -> Config:
        """
        Synthesize a Config object from this speaker's SpeakerConfig.

        Existing components (DiscoveryService, WsManager, BackendFactory) all
        accept a Config, so we map SpeakerConfig fields into one to keep those
        components unchanged.
        """
        cfg = Config(
            # Qobuz config carries max_quality only; credentials are on api_client
            qobuz=QobuzConfig(
                max_quality=self._config.max_quality,
            ),
            device=DeviceConfig(
                name=self._config.name,
                uuid=self._config.uuid,
            ),
            backend=BackendConfig(
                type=self._config.backend_type,
                dlna=DLNAConfig(
                    ip=self._config.dlna_ip,
                    port=self._config.dlna_port,
                    fixed_volume=self._config.dlna_fixed_volume,
                    proxy_port=self._config.proxy_port,
                    description_url=self._config.dlna_description_url,
                    hires_downsampling=self._config.dlna_hires_downsampling,
                ),
                local=LocalConfig(
                    device=self._config.audio_device,
                    buffer_size=self._config.audio_buffer_size,
                ),
            ),
            server=ServerConfig(
                http_port=self._config.http_port,
                bind_address=self._config.bind_address,
            ),
            logging=LoggingConfig(),
        )
        return cfg

    async def start(self) -> bool:
        """
        Start the speaker and all its components.

        Returns:
            True on success, False if any component fails to start.
        """
        try:
            logger.info(f"[{self.name}] Starting speaker...")

            # 1. Build a per-speaker Config for component factories
            component_config = self._build_component_config()

            # 2. Create and start discovery service — its constructor is
            # synchronous, but its actual .start() (mDNS registration: a
            # fresh Zeroconf() engine plus RFC 6762 probing, typically the
            # single most expensive step here) touches nothing the backend
            # chain below builds. quality_getter is a bound method invoked
            # lazily by an HTTP handler, not resolved now, so the two have
            # no real dependency — run them concurrently instead of paying
            # for mDNS registration and DLNA connect back to back.
            logger.debug(f"[{self.name}] Starting discovery service...")
            self._discovery = DiscoveryService(
                config=component_config,
                app_id=self._app_id,
                on_connect=self._on_app_connected,
                quality_getter=self._get_effective_quality,
                web_app=self._web_app,
            )

            await asyncio.gather(
                self._discovery.start(),
                self._connect_backend_and_build_player(component_config),
            )
            logger.info(f"[{self.name}] Discovery service started on port {self._config.http_port}")

            self._is_running = True
            logger.info(
                f"[{self.name}] Ready — device '{self._config.name}' is now visible in Qobuz app"
            )
            return True

        except Exception as e:
            logger.error(f"[{self.name}] Failed to start: {e}", exc_info=True)
            await self.stop()
            return False

    async def _connect_backend_and_build_player(self, component_config: Config) -> None:
        """Connect the audio backend and build the queue/player on top of it.

        Split out of start() so it can run concurrently with
        DiscoveryService.start() (see there) — sets self._backend,
        self._effective_quality, self._metadata_service, self._proxy_server,
        self._queue and self._player.
        """
        # Create audio backend
        logger.debug(f"[{self.name}] Creating audio backend...")
        backend = await BackendFactory.create_from_config(component_config)
        self._backend = backend
        logger.info(f"[{self.name}] Connected to backend: {backend.name}")

        # Resolve effective quality (handle AUTO_QUALITY)
        self._effective_quality = self._config.max_quality
        if self._effective_quality == AUTO_QUALITY:
            if isinstance(backend, DLNABackend):
                recommended = backend.get_recommended_quality()
                if recommended:
                    self._effective_quality = recommended
                    quality_names = {
                        5: "MP3",
                        6: "CD (FLAC 16/44)",
                        7: "Hi-Res (24/96)",
                        27: "Hi-Res (24/192)",
                    }
                    if backend.quality_detection_confirmed:
                        self._quality_source = "auto"
                        logger.info(
                            f"[{self.name}] Auto-detected max quality: "
                            f"{quality_names.get(self._effective_quality, self._effective_quality)}"
                        )
                    else:
                        self._quality_source = "auto_fallback"
                        logger.info(
                            f"[{self.name}] Device did not report its supported "
                            f"formats; using conservative quality: "
                            f"{quality_names.get(self._effective_quality, self._effective_quality)}. "
                            f"Set max_quality manually if the device supports hi-res."
                        )
                else:
                    self._effective_quality = AUTO_FALLBACK_QUALITY
                    self._quality_source = "auto_fallback"
                    logger.info(
                        f"[{self.name}] Capability discovery unavailable, "
                        f"using fallback quality: CD (FLAC 16/44)"
                    )
            else:
                # Local backend: default to Hi-Res 192k
                self._effective_quality = 27
                self._quality_source = "auto"
                logger.info(f"[{self.name}] Local backend, using max quality: Hi-Res (24/192)")

        # Create metadata service
        logger.debug(f"[{self.name}] Creating metadata service...")
        self._metadata_service = MetadataService(
            api_client=self._api_client,
            max_quality=self._effective_quality,
        )

        # Create and start audio proxy server (DLNA only)
        if isinstance(backend, DLNABackend):
            logger.debug(f"[{self.name}] Starting audio proxy server...")
            url_provider = MetadataServiceURLProvider(self._metadata_service)
            self._proxy_server = AudioProxyServer(
                url_provider=url_provider,
                host=self._config.bind_address,
                port=self._config.proxy_port,
            )
            await self._proxy_server.start()
            logger.info(
                f"[{self.name}] Audio proxy listening on "
                f"{self._config.bind_address}:{self._config.proxy_port}"
            )
            backend.set_proxy_server(self._proxy_server)

        # Create queue and player
        logger.debug(f"[{self.name}] Creating queue and player...")
        self._queue = QobuzQueue()
        self._player = QobuzPlayer(
            queue=self._queue,
            metadata_service=self._metadata_service,
            backend=backend,
            play_reporter=PlayReporter(self._api_client),
        )
        if isinstance(backend, DLNABackend):
            self._player.set_fixed_volume_mode(self._config.dlna_fixed_volume)

    async def stop(self, send_device_stop: bool = True) -> None:
        """
        Stop the speaker and all its components.

        Shutdown order (reverse of startup):
        1. Stop state reporter
        2. Stop player
        3. Disconnect WebSocket
        4. Stop discovery service
        5. Stop audio proxy
        6. Disconnect backend

        Args:
            send_device_stop: Whether the backend should send the physical
                device an explicit stop command. Set False when the device
                isn't actually going anywhere — e.g. it's already being
                driven by something else and a Stop here would just
                interrupt that.
        """
        logger.info(f"[{self.name}] Stopping speaker...")
        self._is_running = False

        # 1. Stop state reporter
        if self._state_reporter:
            try:
                await self._state_reporter.stop()
            except Exception as e:
                logger.warning(f"[{self.name}] Error stopping state reporter: {e}")

        # 2. Stop player
        if self._player:
            try:
                await self._player.stop(send_device_stop=send_device_stop)
            except Exception as e:
                logger.warning(f"[{self.name}] Error stopping player: {e}")

        # 3. Disconnect WebSocket
        if self._ws_manager:
            try:
                await self._ws_manager.stop()
            except Exception as e:
                logger.warning(f"[{self.name}] Error disconnecting WebSocket: {e}")

        # 4. Stop discovery service
        if self._discovery:
            try:
                await self._discovery.stop()
            except Exception as e:
                logger.warning(f"[{self.name}] Error stopping discovery service: {e}")

        # 5. Stop audio proxy
        if self._proxy_server:
            try:
                await self._proxy_server.stop()
            except Exception as e:
                logger.warning(f"[{self.name}] Error stopping proxy server: {e}")

        # 6. Disconnect backend
        if self._backend:
            try:
                await self._backend.disconnect(send_device_stop=send_device_stop)
            except Exception as e:
                logger.warning(f"[{self.name}] Error disconnecting backend: {e}")

        logger.info(f"[{self.name}] Stopped")

    # ------------------------------------------------------------------
    # Internal callbacks and helpers
    # ------------------------------------------------------------------

    def _get_effective_quality(self) -> int:
        """Return current effective quality (may change after auto-detection or app request)."""
        return self._effective_quality

    def _on_app_connected(self, tokens: ConnectTokens) -> None:
        """Callback invoked by DiscoveryService when the Qobuz app provides tokens."""
        logger.info(f"[{self.name}] Qobuz app connected, setting up WebSocket...")
        asyncio.create_task(self._setup_websocket(tokens))

    async def _setup_websocket(self, tokens: ConnectTokens) -> None:
        """Set up (or refresh) the WebSocket connection after receiving tokens."""
        assert self._queue is not None
        assert self._player is not None

        async with self._ws_setup_lock:
            try:
                if self._ws_manager is not None:
                    # Already connected — just refresh the tokens
                    self._ws_manager.set_tokens(tokens)
                    logger.info(f"[{self.name}] Refreshed WebSocket tokens from Qobuz app")
                    self._ws_connected_event.set()
                    return

                # Build the per-speaker Config so WsManager knows device identity / quality
                component_config = self._build_component_config()

                # Create WebSocket manager
                self._ws_manager = WsManager(config=component_config)
                self._ws_manager.set_tokens(tokens)
                self._ws_manager.set_max_audio_quality(self._effective_quality)

                # Create handlers
                self._queue_handler = QueueHandler(self._queue)
                self._playback_handler = PlaybackCommandHandler(
                    self._player,
                    on_quality_change=self._on_quality_change,
                )
                self._volume_handler = VolumeCommandHandler(self._player)

                # Wire next-track callbacks for auto-advance
                self._player.set_next_track_callbacks(
                    get_callback=self._playback_handler.get_next_track_info,
                    clear_callback=self._playback_handler.clear_next_track_info,
                )

                # Re-arm gapless when the app changes the next queue item mid-track
                # (e.g. "play next" insertions), otherwise the stale armed track
                # plays and the inserted track is skipped.
                self._playback_handler.set_on_next_track_changed(
                    self._player.on_next_track_info_changed
                )

                # Register all message-type handlers. Each dispatch is
                # enqueued on the player's command queue rather than
                # spawned as its own bare task — a single consumer running
                # these strictly one at a time is what gives WS-driven
                # commands (and the backend-driven natural-track-end
                # continuation, on_track_ended) real ordering/mutual
                # exclusion, instead of every inbound message racing every
                # other one with no guarantee at all. coalesce=True on
                # SET_STATE specifically reproduces the old generation-
                # based supersede behavior for the one message type that
                # actually arrives in aggressive bursts (e.g. a seek-bar
                # scrub) — see QobuzPlayer.enqueue().
                #
                # Bound to plain local variables (rather than referencing
                # self._player/self._x_handler from inside the lambdas)
                # so mypy can narrow them past their Optional[...]
                # declared type — narrowing an attribute doesn't survive
                # into a closure that might run later, a local variable's
                # does.
                player = self._player
                queue_handler = self._queue_handler
                playback_handler = self._playback_handler
                volume_handler = self._volume_handler

                for msg_type in queue_handler.get_message_types():
                    self._ws_manager.register_handler(
                        msg_type,
                        lambda mt, msg: player.enqueue(
                            functools.partial(queue_handler.handle_message, mt, msg)
                        ),
                    )

                for msg_type in playback_handler.get_message_types():
                    self._ws_manager.register_handler(
                        msg_type,
                        lambda mt, msg: player.enqueue(
                            functools.partial(playback_handler.handle_message, mt, msg),
                            coalesce=(mt == MSG_TYPE_SET_STATE),
                        ),
                    )

                for msg_type in volume_handler.get_message_types():
                    self._ws_manager.register_handler(
                        msg_type,
                        lambda mt, msg: player.enqueue(
                            functools.partial(volume_handler.handle_message, mt, msg)
                        ),
                    )

                # Register error handler (message type 1 = MESSAGE_TYPE_ERROR)
                self._ws_manager.register_handler(
                    1,
                    self._handle_protocol_error,
                )

                # Create state reporter and wire it into the player
                self._state_reporter = StateReporter(
                    player=self._player,
                    queue=self._queue,
                    send_callback=self._send_state_report,
                )
                self._player.set_state_reporter(self._state_reporter)

                # Wire volume and file-quality reporting callbacks
                self._player.set_volume_report_callback(self._ws_manager.send_volume_changed)
                self._player.set_file_quality_report_callback(
                    self._ws_manager.send_file_audio_quality_changed
                )
                self._player.set_hijack_detected_callback(self._ws_manager.force_reconnect)

                # Start WebSocket, state reporter, and player
                await self._ws_manager.start()
                logger.info(f"[{self.name}] WebSocket connected to Qobuz servers")

                await self._state_reporter.start()
                await self._player.start()
                logger.info(f"[{self.name}] Player started")

                # Send initial volume so the app shows the accurate value immediately
                try:
                    initial_volume = await self._player.get_volume()
                    await self._ws_manager.send_volume_changed(initial_volume)
                    logger.info(f"[{self.name}] Sent initial volume to app: {initial_volume}%")
                except Exception as e:
                    logger.warning(f"[{self.name}] Failed to send initial volume: {e}")

                # Signal that the WebSocket setup is complete
                self._ws_connected_event.set()

            except Exception as e:
                logger.error(f"[{self.name}] Failed to set up WebSocket: {e}", exc_info=True)

    async def _on_quality_change(self, new_quality: int) -> None:
        """
        Handle a quality-change request from the Qobuz app.

        Args:
            new_quality: New quality ID (5=MP3, 6=CD, 7=Hi-Res 96k, 27=Hi-Res 192k)
        """
        if new_quality == self._effective_quality:
            logger.debug(f"[{self.name}] Quality unchanged: {new_quality}")
            return

        logger.info(f"[{self.name}] Quality changed: {self._effective_quality} -> {new_quality}")
        self._effective_quality = new_quality

        if self._metadata_service:
            self._metadata_service.set_max_quality(new_quality)

        if self._player:
            await self._player.reload_current_track()

    def _handle_protocol_error(self, msg_type: int, msg: object) -> None:
        """Handle protocol error messages received from the Qobuz server."""
        # msg is a protobuf message; use hasattr for safe access in tests
        error = getattr(msg, "error", None)
        has_error = callable(getattr(msg, "HasField", None)) and msg.HasField("error")  # type: ignore[attr-defined]
        if has_error and error:
            logger.error(
                f"[{self.name}] Protocol error: code={error.code}, message={error.message}"
            )
        else:
            logger.error(f"[{self.name}] Protocol error message received (type {msg_type})")

    async def _send_state_report(self, report: PlaybackStateReport) -> None:
        """Forward a state report to the Qobuz servers via WebSocket."""
        if not self._ws_manager:
            return

        playing_state = report.playing_state
        if playing_state == PlaybackState.LOADING:
            playing_state = PlaybackState.STOPPED
        elif playing_state == PlaybackState.ERROR:
            playing_state = PlaybackState.STOPPED

        # TEMP: dump every outbound state report as-is — remove once the
        # next/previous investigation is done.
        logger.info(
            f"TEMP sending state report: playing_state={playing_state!r} "
            f"(raw={report.playing_state!r}), position={report.position_value_ms}ms "
            f"@ts={report.position_timestamp_ms}, duration={report.duration_ms}ms, "
            f"queue_item_id={report.current_queue_item_id}, "
            f"queue_version={report.queue_version_major}.{report.queue_version_minor}, "
            f"buffer={report.buffer_state!r}"
        )

        await self._ws_manager.send_state_update(
            playing_state=int(playing_state),
            buffer_state=int(report.buffer_state),
            position_ms=report.position_value_ms,
            position_timestamp_ms=report.position_timestamp_ms,
            duration_ms=report.duration_ms,
            queue_item_id=report.current_queue_item_id,
            queue_version_major=report.queue_version_major,
            queue_version_minor=report.queue_version_minor,
        )

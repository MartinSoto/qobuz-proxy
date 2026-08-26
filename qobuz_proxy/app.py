"""
QobuzProxy Application.

Orchestrates authentication, the web UI, and per-speaker lifecycle.
The HTTP server starts first so users can submit credentials through the
web UI even before a valid Qobuz token is available.
"""

import asyncio
import logging
import os
import signal
from typing import Optional

from aiohttp import web

from qobuz_proxy import __commit__, __version__
from qobuz_proxy.auth import (
    QobuzAPIClient,
    clear_user_token,
    load_user_token,
    save_user_token,
)
from qobuz_proxy.auth.oauth import OAUTH_APP_ID, OAUTH_APP_SECRET
from qobuz_proxy.config import (
    AUTO_QUALITY,
    Config,
    SpeakerConfig,
    _assign_ports,
    _generate_uuids,
    generate_sonos_speaker_uuid,
    slugify_name,
)
from qobuz_proxy.backends.dlna.client import DLNAClient
from qobuz_proxy.backends.dlna.sonos_discovery_manager import (
    DepartedMember,
    SonosDiscoveryManager,
    SonosRoom,
)
from qobuz_proxy.backends.dlna.sonos_events import SonosEventSubscriber
from qobuz_proxy.speaker import Speaker
from qobuz_proxy.webui.config_writer import save_config
from qobuz_proxy.webui.routes import register_routes

logger = logging.getLogger(__name__)

# Backoff schedule for speakers that fail to start (renderer offline, boot
# races between containers). After the ramp, keep trying at a steady pace.
SPEAKER_RETRY_DELAYS_SECONDS: tuple[float, ...] = (5.0, 10.0, 20.0, 40.0, 60.0)
SPEAKER_RETRY_STEADY_DELAY_SECONDS: float = 300.0


class QobuzProxy:
    """
    Main QobuzProxy application.

    Starts the shared HTTP server (web UI + discovery routes) first, then
    attempts automatic authentication from config or cached tokens. If no
    valid credentials are available the app stays running in a
    "waiting-for-auth" state so the user can provide a token through the
    web UI.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._is_running = False
        self._shutdown_event = asyncio.Event()

        # Auth / API
        self._api_client: Optional[QobuzAPIClient] = None
        self._app_id: str = ""
        self._app_secret: str = ""

        # Auth state — shared with the web UI status endpoint via _web_app["auth_state"].
        # Always the *same* dict object so route handlers see live updates.
        self._auth_state: dict[str, object] = {
            "authenticated": False,
            "user_id": "",
            "email": "",
            "name": "",
            "avatar": "",
        }

        # Shared aiohttp application (web UI + per-speaker discovery routes)
        self._web_app: Optional[web.Application] = None
        self._web_runner: Optional[web.AppRunner] = None
        self._web_site: Optional[web.TCPSite] = None
        # Its GENA NOTIFY route must be registered before the app starts
        # serving (aiohttp freezes the router afterwards), but a
        # SonosDiscoveryManager to actually use it only exists post-login —
        # so this is created unconditionally in _start_web_server(), and
        # SonosDiscoveryManager attaches/detaches as it starts/stops.
        self._sonos_event_subscriber: Optional[SonosEventSubscriber] = None

        # Speakers
        self._speakers: list[Speaker] = []
        # Background retry tasks for speakers that failed to start, keyed by
        # slugified speaker name. Also keeps strong references to the tasks.
        self._speaker_retry_tasks: dict[str, asyncio.Task[None]] = {}

        # Sonos auto-discovery (mutually exclusive with config.speakers —
        # see _start_speakers). Running speakers it created are still just
        # entries in self._speakers; this index is only for matching a
        # SonosRoom.tracking_key (its group_id) back to its Speaker on
        # lost/renamed/retargeted — the key never changes across a
        # coordinator handoff, only what it points at does.
        self._sonos_discovery: Optional[SonosDiscoveryManager] = None
        self._sonos_speakers_by_group_id: dict[str, Speaker] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start web server, fetch app credentials, attempt auto-auth."""
        logger.info("Starting qobuz-proxy...")

        # 1. Start the HTTP server so the web UI is reachable immediately
        await self._start_web_server()

        # 2. Set Qobuz app credentials (desktop app OAuth + signing secret)
        self._app_id = OAUTH_APP_ID
        self._app_secret = OAUTH_APP_SECRET

        # 3. Attempt auto-auth from config or cache
        token_info = self._get_token_from_config_or_cache()
        if token_info:
            user_id = token_info["user_id"]
            auth_token = token_info["user_auth_token"]
            email = token_info.get("email", "")

            if await self._authenticate(user_id, auth_token):
                self._auth_state["user_id"] = user_id
                self._auth_state["email"] = email
                self._auth_state["authenticated"] = True
                await self._start_speakers()
            else:
                logger.warning("Cached/config token is invalid — waiting for auth via web UI")

        if not self._auth_state["authenticated"]:
            port = self._config.server.http_port
            logger.info(f"No valid credentials — visit http://localhost:{port} to authenticate")

        self._is_running = True

    async def stop(self) -> None:
        """Stop speakers, then the web server."""
        if not self._is_running:
            return

        self._is_running = False
        await self._stop_speakers()
        await self._stop_web_server()
        logger.info("qobuz-proxy stopped")

    async def run(self) -> None:
        """Run until SIGINT / SIGTERM."""
        loop = asyncio.get_running_loop()

        def handle_signal() -> None:
            logger.info("Shutdown signal received")
            self._shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_signal)

        try:
            await self.start()
            await self._shutdown_event.wait()
        finally:
            await self.stop()

    @property
    def is_running(self) -> bool:
        """Return True if the application event loop is active."""
        return self._is_running

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def _get_token_from_config_or_cache(self) -> Optional[dict[str, str]]:
        """Return user credentials from config (highest priority) or cache."""
        # Config values take precedence
        if self._config.qobuz.auth_token and self._config.qobuz.user_id:
            return {
                "user_id": self._config.qobuz.user_id,
                "user_auth_token": self._config.qobuz.auth_token,
                "email": self._config.qobuz.email,
            }

        # Fall back to cached token
        cached = load_user_token()
        if cached:
            logger.info("Found cached user token")
            return cached

        return None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def _authenticate(self, user_id: str, auth_token: str) -> bool:
        """Validate credentials against the Qobuz API. Returns True on success."""
        if not self._app_id:
            logger.error("Cannot authenticate — app credentials not available")
            return False

        self._api_client = QobuzAPIClient(self._app_id, self._app_secret)
        logger.info(f"Authenticating user {user_id}...")
        if await self._api_client.login_with_token(user_id=user_id, auth_token=auth_token):
            logger.info("Authentication successful")
            return True

        logger.warning("Authentication failed — invalid credentials")
        self._api_client = None
        return False

    # ------------------------------------------------------------------
    # Web UI callbacks
    # ------------------------------------------------------------------

    async def _on_auth_token(
        self,
        user_id: str,
        auth_token: str,
        profile: dict[str, str] | None = None,
        **_kwargs: object,
    ) -> bool:
        """Called by the web UI when the user submits a token.

        Validates credentials, persists them to cache, and starts speakers
        if they are not already running.
        """
        if profile is None:
            profile = {}

        # Set up API client directly — token is pre-validated by OAuth
        # and scoped to OAUTH_APP_ID which we use for all requests.
        self._api_client = QobuzAPIClient(self._app_id, self._app_secret)
        self._api_client.user_auth_token = auth_token
        self._api_client.user_id = user_id

        email = profile.get("email", "")
        name = profile.get("name", "")
        avatar = profile.get("avatar", "")

        # Persist to cache
        save_user_token(user_id=user_id, auth_token=auth_token, email=email)

        # Update shared auth state
        self._auth_state["authenticated"] = True
        self._auth_state["user_id"] = user_id
        self._auth_state["email"] = email
        self._auth_state["name"] = name
        self._auth_state["avatar"] = avatar

        # Start speakers if not already running
        if not self._speakers:
            await self._start_speakers()

        return True

    async def _on_logout(self) -> None:
        """Called by the web UI when the user requests logout."""
        logger.info("Logout requested — stopping speakers and clearing token")
        await self._stop_speakers()

        self._auth_state["authenticated"] = False
        self._auth_state["user_id"] = ""
        self._auth_state["email"] = ""
        self._api_client = None

        clear_user_token()

    # ------------------------------------------------------------------
    # Speaker management (hot add / edit / remove)
    # ------------------------------------------------------------------

    async def _on_add_speaker(self, body: dict) -> dict:
        """Add a new speaker at runtime."""
        if self._config.sonos_auto_discover:
            # A manually-added speaker would run until the next restart, then
            # silently vanish — auto-discovery skips config.speakers on boot,
            # so it's never read back. Reject rather than trap the user.
            raise ValueError(
                "Manual speaker configuration is disabled while sonos_auto_discover is enabled"
            )

        name = body["name"].strip()
        backend_type = body.get("backend", "dlna")

        # Check for duplicate names against the config — a speaker can exist in
        # config without running (e.g. its device is offline), and a duplicate
        # name in the saved config prevents the app from booting. Also check
        # currently running speakers, which can include ones not in config.
        new_id = slugify_name(name)
        for sc_existing in self._config.speakers:
            if slugify_name(sc_existing.name) == new_id:
                raise ValueError(f"Speaker '{name}' already exists")
        if any(slugify_name(s.name) == new_id for s in self._speakers):
            raise ValueError(f"Speaker '{name}' already exists")

        # Build SpeakerConfig
        quality_raw = body.get("max_quality", "auto")
        if isinstance(quality_raw, str) and quality_raw.lower() == "auto":
            max_quality = AUTO_QUALITY
        else:
            max_quality = int(quality_raw)

        sc = SpeakerConfig(
            name=name,
            backend_type=backend_type,
            max_quality=max_quality,
            dlna_ip=body.get("dlna_ip", ""),
            dlna_port=int(body.get("dlna_port", 1400)),
            dlna_fixed_volume=bool(body.get("fixed_volume", False)),
            dlna_description_url=body.get("description_url", ""),
            audio_device=body.get("audio_device", "default"),
            audio_buffer_size=int(body.get("buffer_size", 2048)),
        )

        # Assign ports and UUID
        all_configs = [s._config for s in self._speakers] + [sc]
        _assign_ports(all_configs, webui_port=self._config.server.http_port)
        _generate_uuids([sc])

        # Create and start speaker
        assert self._api_client is not None
        speaker = Speaker(config=sc, api_client=self._api_client, app_id=self._app_id)
        started = await speaker.start()
        if not started:
            raise ValueError(f"Speaker '{name}' failed to start")

        self._speakers.append(speaker)

        # Update config and persist
        self._config.speakers.append(sc)
        self._save_config()

        return speaker.get_status()

    async def _on_edit_speaker(self, speaker_id: str, body: dict) -> dict:
        """Edit a speaker at runtime (stop, reconfigure, restart).

        The new config is persisted even when the speaker fails to restart —
        edits are often the fix for connectivity (e.g. a changed device IP),
        so refusing to save them would lock the user out of recovering.
        """
        # Match the config entry by name: the running list and the config list
        # can be misaligned when some speakers failed to start.
        config_idx = None
        for i, sc in enumerate(self._config.speakers):
            if slugify_name(sc.name) == speaker_id:
                config_idx = i
                break
        if config_idx is None:
            raise KeyError(speaker_id)

        speaker_idx = None
        for i, s in enumerate(self._speakers):
            if slugify_name(s.name) == speaker_id:
                speaker_idx = i
                break

        old_config = self._config.speakers[config_idx]

        new_name = body.get("name", old_config.name).strip()
        if slugify_name(new_name) != speaker_id:
            for i, sc in enumerate(self._config.speakers):
                if i != config_idx and slugify_name(sc.name) == slugify_name(new_name):
                    raise ValueError(f"Speaker '{new_name}' already exists")

        quality_raw = body.get("max_quality", old_config.max_quality)
        if isinstance(quality_raw, str) and quality_raw.lower() == "auto":
            max_quality = AUTO_QUALITY
        else:
            max_quality = int(quality_raw)

        new_config = SpeakerConfig(
            name=new_name,
            uuid=old_config.uuid,
            backend_type=old_config.backend_type,  # Immutable
            max_quality=max_quality,
            http_port=old_config.http_port,
            bind_address=old_config.bind_address,
            dlna_ip=body.get("dlna_ip", old_config.dlna_ip),
            dlna_port=int(body.get("dlna_port", old_config.dlna_port)),
            dlna_fixed_volume=bool(body.get("fixed_volume", old_config.dlna_fixed_volume)),
            dlna_description_url=body.get("description_url", old_config.dlna_description_url),
            proxy_port=old_config.proxy_port,
            audio_device=body.get("audio_device", old_config.audio_device),
            audio_buffer_size=int(body.get("buffer_size", old_config.audio_buffer_size)),
        )

        # Persist first: the edit is saved even if the restart below fails
        self._config.speakers[config_idx] = new_config
        self._save_config()

        # The edited speaker replaces any pending boot retry of the old config
        self._cancel_speaker_retry(speaker_id)

        if speaker_idx is not None:
            await self._speakers[speaker_idx].stop()

        assert self._api_client is not None
        new_speaker = Speaker(config=new_config, api_client=self._api_client, app_id=self._app_id)
        started = await new_speaker.start()
        if speaker_idx is not None:
            self._speakers[speaker_idx] = new_speaker
        else:
            self._speakers.append(new_speaker)

        status = new_speaker.get_status()
        if not started:
            logger.warning(
                f"Speaker '{new_config.name}' failed to start with new config "
                "(configuration saved anyway)"
            )
            status["warning"] = "Configuration saved, but the speaker failed to start"
        return status

    async def _on_remove_speaker(self, speaker_id: str) -> None:
        """Remove a speaker at runtime."""
        # Match the config entry by name: the running list and the config list
        # can be misaligned when some speakers failed to start, so the running
        # index must not be used to pop from the config list.
        config_idx = None
        for i, sc in enumerate(self._config.speakers):
            if slugify_name(sc.name) == speaker_id:
                config_idx = i
                break
        if config_idx is None:
            raise KeyError(speaker_id)

        speaker_idx = None
        for i, s in enumerate(self._speakers):
            if slugify_name(s.name) == speaker_id:
                speaker_idx = i
                break

        self._config.speakers.pop(config_idx)
        self._cancel_speaker_retry(speaker_id)
        if speaker_idx is not None:
            speaker = self._speakers.pop(speaker_idx)
            await speaker.stop()

        self._save_config()

    def _save_config(self) -> None:
        """Persist current config to YAML file."""
        if self._config.config_path:
            try:
                save_config(self._config, self._config.config_path)
            except Exception as e:
                logger.error(f"Failed to save config: {e}")

    # ------------------------------------------------------------------
    # Web server
    # ------------------------------------------------------------------

    async def _start_web_server(self) -> None:
        """Create the shared aiohttp app and start listening."""
        self._web_app = web.Application()

        # Expose state for route handlers
        self._web_app["auth_state"] = self._auth_state
        self._web_app["get_speakers"] = lambda: [s.get_status() for s in self._speakers]
        self._web_app["version"] = __version__
        self._web_app["commit"] = __commit__
        self._web_app["http_port"] = self._config.server.http_port
        self._web_app["on_auth_token"] = self._on_auth_token
        self._web_app["on_logout"] = self._on_logout
        self._web_app["on_add_speaker"] = self._on_add_speaker
        self._web_app["on_edit_speaker"] = self._on_edit_speaker
        self._web_app["on_remove_speaker"] = self._on_remove_speaker
        self._web_app["local_audio_enabled"] = os.environ.get(
            "QOBUZPROXY_LOCAL_AUDIO_UI", ""
        ).lower() in ("true", "1", "yes")

        register_routes(self._web_app)

        # Registered unconditionally (cheap — an idle route that 412s until
        # a SonosDiscoveryManager claims it) since routes can't be added
        # once the runner below freezes the router.
        self._sonos_event_subscriber = SonosEventSubscriber()
        self._sonos_event_subscriber.register_route(self._web_app)

        self._web_runner = web.AppRunner(self._web_app, access_log=None)
        await self._web_runner.setup()
        self._web_site = web.TCPSite(
            self._web_runner,
            self._config.server.bind_address,
            self._config.server.http_port,
        )
        await self._web_site.start()
        logger.info(
            f"Web server listening on "
            f"{self._config.server.bind_address}:{self._config.server.http_port}"
        )

    async def _stop_web_server(self) -> None:
        """Shut down the shared aiohttp app."""
        if self._web_site:
            await self._web_site.stop()
        if self._web_runner:
            await self._web_runner.cleanup()

    # ------------------------------------------------------------------
    # Speaker lifecycle
    # ------------------------------------------------------------------

    async def _start_speakers(self) -> None:
        """Create and start Speaker instances from config.

        Speakers that fail to start (e.g. the renderer is still booting or
        temporarily unreachable) are retried in the background instead of
        being dropped until the next restart.
        """
        assert self._api_client is not None

        if self._config.sonos_auto_discover:
            if self._config.speakers:
                logger.warning(
                    "sonos_auto_discover is enabled — ignoring the configured 'speakers' list"
                )
            await self._start_sonos_auto_discovery()
            return

        if not self._config.speakers:
            port = self._config.server.http_port
            logger.info(f"No speakers configured — add speakers at http://localhost:{port}")
            return

        running_ids = {slugify_name(s.name) for s in self._speakers}
        configs = [sc for sc in self._config.speakers if slugify_name(sc.name) not in running_ids]

        speakers = [
            Speaker(
                config=sc,
                api_client=self._api_client,
                app_id=self._app_id,
            )
            for sc in configs
        ]

        results = await asyncio.gather(*[s.start() for s in speakers], return_exceptions=True)

        for sc, speaker, result in zip(configs, speakers, results):
            if isinstance(result, BaseException) or result is False:
                reason = f": {result}" if isinstance(result, BaseException) else ""
                logger.warning(
                    f"Speaker '{speaker.name}' failed to start{reason} — retrying in background"
                )
                self._schedule_speaker_retry(sc)
            else:
                self._speakers.append(speaker)

        if not self._speakers:
            logger.error(
                "No speakers started successfully — retrying in background; "
                "check configuration and logs"
            )
            return

        names = ", ".join(s.name for s in self._speakers)
        port = self._config.server.http_port
        logger.info(f"qobuz-proxy ready — {len(self._speakers)} speaker(s): {names}")
        logger.info(f"Web UI: http://localhost:{port}")

    # ------------------------------------------------------------------
    # Sonos auto-discovery
    # ------------------------------------------------------------------

    async def _start_sonos_auto_discovery(self) -> None:
        """Start continuous Sonos household discovery in place of config.speakers."""
        if self._sonos_discovery is not None:
            return  # already running (e.g. re-login after logout)

        self._sonos_discovery = SonosDiscoveryManager(
            on_room_found=self._on_sonos_room_found,
            on_room_lost=self._on_sonos_room_lost,
            on_room_renamed=self._on_sonos_room_renamed,
            on_room_retargeted=self._on_sonos_room_retargeted,
            on_room_rekeyed=self._on_sonos_room_rekeyed,
            on_room_members_departed=self._on_sonos_room_members_departed,
            # Enables GENA event subscription (near-instant topology change
            # detection) on top of the polling fallback, via the route
            # _start_web_server already registered on the shared app.
            event_subscriber=self._sonos_event_subscriber,
            http_port=self._config.server.http_port,
        )
        await self._sonos_discovery.start()

        port = self._config.server.http_port
        if self._speakers:
            names = ", ".join(s.name for s in self._speakers)
            logger.info(f"Sonos auto-discovery ready — {len(self._speakers)} room(s): {names}")
        else:
            logger.info("Sonos auto-discovery started — no rooms found yet, will keep polling")
        logger.info(f"Web UI: http://localhost:{port}")

    async def _on_sonos_room_found(self, room: SonosRoom) -> bool:
        """Called by SonosDiscoveryManager when a new group coordinator appears.

        Returns True only once the Speaker is actually running — the caller
        treats False as "not yet known", so a failure here is naturally
        retried on the next poll with no extra bookkeeping.
        """
        if self._api_client is None:
            return False  # not logged in yet

        # display_name is comma-joined member room names for an active
        # group ("Kitchen, Living Room"), matching how the Sonos app itself
        # labels a group — just the room name when playing solo.
        display_name = room.display_name
        new_id = slugify_name(display_name)
        if any(slugify_name(s.name) == new_id for s in self._speakers):
            logger.warning(
                f"Sonos discovery: room '{display_name}' name collides with an "
                "existing speaker — skipping"
            )
            return False

        # Identity is derived from the group's own id, not the coordinator's
        # physical uuid: group_id is confirmed stable across a coordinator
        # handoff, so a promoted coordinator taking over a continuing group
        # computes the *same* device identity its predecessor had — the app
        # sees a device it already knows reconnect. Falls back to the
        # coordinator's own uuid if group_id is ever unavailable.
        sc = SpeakerConfig(
            name=display_name,
            uuid=generate_sonos_speaker_uuid(room.group_id or room.uuid),
            backend_type="dlna",
            max_quality=AUTO_QUALITY,
            dlna_ip=room.ip,
            dlna_port=room.port,
            auto_managed=True,
        )
        all_configs = [s._config for s in self._speakers] + [sc]
        _assign_ports(all_configs, webui_port=self._config.server.http_port)

        speaker = Speaker(config=sc, api_client=self._api_client, app_id=self._app_id)
        started = await speaker.start()
        if not started:
            logger.warning(f"Sonos discovery: '{display_name}' failed to start")
            return False

        self._speakers.append(speaker)
        self._sonos_speakers_by_group_id[room.tracking_key] = speaker
        logger.info(f"Sonos discovery: added speaker '{display_name}' ({room.ip})")
        return True

    async def _on_sonos_room_lost(self, tracking_key: str, still_present: bool) -> None:
        """Called by SonosDiscoveryManager when a group's coordinator stops
        being a coordinator at all — either its last device went offline
        (still_present=False), or it was absorbed as a plain member into
        another group (still_present=True). Either way we give up *our*
        Speaker for it, but only send the device an explicit stop in the
        offline case — when it's still present, Sonos itself is already
        directing its audio as part of the other group, and a stop here
        would just interrupt that."""
        speaker = self._sonos_speakers_by_group_id.pop(tracking_key, None)
        if speaker is None:
            return
        if speaker in self._speakers:
            self._speakers.remove(speaker)
        logger.info(f"Sonos discovery: removing speaker '{speaker.name}'")
        await speaker.stop(send_device_stop=not still_present)

    async def _on_sonos_room_renamed(self, room: SonosRoom) -> bool:
        """Called by SonosDiscoveryManager when a group's display name
        changes (regrouping, or a room renamed in the Sonos app) but its
        coordinator/network identity didn't. Renames the existing Speaker in
        place instead of restarting it, so playback and position survive."""
        speaker = self._sonos_speakers_by_group_id.get(room.tracking_key)
        if speaker is None:
            return False  # not currently running; a found/lost pair will handle it

        new_name = room.display_name
        new_id = slugify_name(new_name)
        if any(slugify_name(s.name) == new_id and s is not speaker for s in self._speakers):
            logger.warning(
                f"Sonos discovery: rename to '{new_name}' collides with an "
                "existing speaker — skipping"
            )
            return False

        return await speaker.rename(new_name)

    async def _on_sonos_room_retargeted(self, room: SonosRoom) -> bool:
        """Called by SonosDiscoveryManager when a group's coordinator (and/or
        its address) changed — repoints the existing Speaker's DLNA backend
        at the new target instead of tearing it down, so the Qobuz Connect
        session (WebSocket, mDNS registration) survives untouched. Sonos
        already migrates the audio itself on a handoff; this just moves
        where future commands go and where state is polled from. The
        group's tracking_key never changes, so there's no re-keying to do."""
        speaker = self._sonos_speakers_by_group_id.get(room.tracking_key)
        if speaker is None:
            return False  # not currently running; a found/lost pair will handle it

        if not await speaker.retarget(room.ip, room.port):
            return False

        new_name = room.display_name
        if new_name != speaker.name:
            await speaker.rename(new_name)

        logger.info(f"Sonos discovery: retargeted speaker '{speaker.name}' to {room.ip}")
        return True

    async def _on_sonos_room_rekeyed(self, old_key: str, room: SonosRoom) -> bool:
        """Called by SonosDiscoveryManager when the *same* physical
        coordinator (uuid unchanged) is now tracked under a different key —
        its group_id changed for some reason other than an actual handoff
        (most likely a plain membership change to a group that otherwise
        didn't move). Moves the existing Speaker to live under the new key
        and applies any retarget/rename needed, instead of tearing it down
        and starting a fresh Qobuz Connect session for a coordinator that
        never actually went anywhere."""
        speaker = self._sonos_speakers_by_group_id.get(old_key)
        if speaker is None:
            return False  # not currently running; a found/lost pair will handle it

        # ip/port are almost always unchanged here (retarget() no-ops in
        # that case) — this is mostly about moving the dict entry, but a
        # coincidental address change (e.g. DHCP) rides along for free.
        if not await speaker.retarget(room.ip, room.port):
            return False

        new_name = room.display_name
        if new_name != speaker.name:
            await speaker.rename(new_name)

        del self._sonos_speakers_by_group_id[old_key]
        self._sonos_speakers_by_group_id[room.tracking_key] = speaker
        logger.info(f"Sonos discovery: re-keyed speaker '{speaker.name}'")
        return True

    async def _on_sonos_room_members_departed(
        self, tracking_key: str, departed: tuple[DepartedMember, ...]
    ) -> None:
        """Called by SonosDiscoveryManager whenever a still-tracked group's
        membership shrinks — for *any* group, whether we're playing to it
        or not (SonosDiscoveryManager has no notion of Qobuz playback
        state at all). Only the group we're actively playing to is our
        business: a device leaving it needs to be told to stop, since it's
        no longer part of what the Qobuz app thinks it's driving and
        nothing else will ever tell it to stop. A device leaving any other
        (merely discovered, not playing) group is Sonos's own business —
        touching it risks exactly the play/mute race fixed for the
        absorbed-into-another-group case (see _on_sonos_room_lost)."""
        speaker = self._sonos_speakers_by_group_id.get(tracking_key)
        if speaker is None or not speaker.is_active:
            return

        for member in departed:
            logger.info(
                f"Sonos discovery: '{member.uuid}' left the active group "
                f"'{speaker.name}' — stopping it directly"
            )
            client = DLNAClient(member.ip, member.port)
            try:
                await client.connect()
                await client.stop()
            except Exception as e:
                logger.debug(f"Could not stop departed device {member.ip}: {e}")
            finally:
                await client.disconnect()

    def _schedule_speaker_retry(self, config: SpeakerConfig) -> None:
        """Start (or keep) a background task retrying a failed speaker."""
        speaker_id = slugify_name(config.name)
        existing = self._speaker_retry_tasks.get(speaker_id)
        if existing and not existing.done():
            return
        self._speaker_retry_tasks[speaker_id] = asyncio.create_task(self._retry_speaker(config))

    def _cancel_speaker_retry(self, speaker_id: str) -> None:
        """Stop retrying a speaker (it was edited, removed, or is shutting down)."""
        task = self._speaker_retry_tasks.pop(speaker_id, None)
        if task:
            task.cancel()

    async def _retry_speaker(self, config: SpeakerConfig) -> None:
        """Retry starting a speaker with backoff until it comes up.

        Renderers routinely boot slower than qobuz-proxy (sibling Docker
        containers, powered-off devices), so a failed start must not drop
        the speaker until the next restart.
        """

        def retry_delay(attempt: int) -> float:
            if attempt < len(SPEAKER_RETRY_DELAYS_SECONDS):
                return SPEAKER_RETRY_DELAYS_SECONDS[attempt]
            return SPEAKER_RETRY_STEADY_DELAY_SECONDS

        speaker_id = slugify_name(config.name)
        attempt = 0
        try:
            while True:
                await asyncio.sleep(retry_delay(attempt))
                attempt += 1

                if self._api_client is None:
                    return  # Logged out — speakers restart on the next login

                speaker = Speaker(config=config, api_client=self._api_client, app_id=self._app_id)
                try:
                    started = await speaker.start()
                except asyncio.CancelledError:
                    await speaker.stop()
                    raise
                except Exception as e:
                    logger.debug(f"Speaker '{config.name}' retry raised: {e}")
                    started = False

                if started:
                    self._speakers.append(speaker)
                    logger.info(f"Speaker '{config.name}' started after {attempt} retry attempt(s)")
                    return

                logger.info(
                    f"Speaker '{config.name}' still failing to start "
                    f"(attempt {attempt}) — next retry in {retry_delay(attempt):.0f}s"
                )
        finally:
            self._speaker_retry_tasks.pop(speaker_id, None)

    async def _stop_speakers(self) -> None:
        """Stop all running speakers and any pending start retries."""
        for task in list(self._speaker_retry_tasks.values()):
            task.cancel()
        self._speaker_retry_tasks.clear()

        if self._sonos_discovery is not None:
            await self._sonos_discovery.stop()
            self._sonos_discovery = None
        self._sonos_speakers_by_group_id.clear()

        if self._speakers:
            # Only send a live device Stop to speakers we're actually
            # driving — shutting down (or logging out) must not interrupt
            # a merely-discovered, idle Sonos room, the same principle as
            # _on_sonos_room_lost/_on_sonos_room_members_departed.
            await asyncio.gather(
                *[s.stop(send_device_stop=s.is_active) for s in self._speakers],
                return_exceptions=True,
            )
            self._speakers = []

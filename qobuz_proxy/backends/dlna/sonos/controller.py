"""
Continuous Sonos household auto-discovery, wired up as Speaker lifecycle.

SonosController is the consumer that turns SonosDiscoveryManager's
Sonos-topology-only callbacks (found/lost/renamed/retargeted/pending/
members-departed) into actual Speaker/Qobuz-Connect-session lifecycle
actions. It owns every Speaker it creates end to end — the app only ever
starts/stops this controller and reads .speakers for cross-cutting
concerns (the web UI's speaker list, an app-wide shutdown), the same
shape as any other top-level component (DiscoveryService, Speaker itself).

A group_id's Speaker is never torn down just because its group_id stopped
being reported for one topology update — see SonosDiscoveryManager's
pending state. _on_room_pending is where this controller reacts to that:
if the affected group is the one actually being played to, detach its
Speaker's backend (stop the device, stop polling it) rather than risk
commands continuing to go to a coordinator that may already be
transitioning out of the group — see split-brain note on _on_room_pending
itself. A room that's merely discovered, not the one Qobuz is driving, is
left running untouched either way; if the pending period resolves as a
real loss, _on_room_lost tears the Speaker down for good.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Optional

from qobuz_proxy.config import (
    AUTO_QUALITY,
    SpeakerConfig,
    _assign_ports,
    generate_sonos_speaker_uuid,
    slugify_name,
)

from ..client import DLNAClient
from .discovery_manager import DepartedMember, SonosDiscoveryManager, SonosRoom
from .events import SonosEventSubscriber

if TYPE_CHECKING:
    # Deferred to runtime (see start()/_on_room_found()) — qobuz_proxy.speaker
    # imports backends.dlna at module level (for its own generic DLNA-vs-
    # local isinstance checks), which imports this package, which would
    # otherwise import speaker.py right back before it's finished loading.
    from qobuz_proxy.auth import QobuzAPIClient
    from qobuz_proxy.speaker import Speaker

logger = logging.getLogger(__name__)

# A departed member keeps pulling audio from our proxy over its own HTTP
# connection — Sonos's group-leave doesn't reliably tear that down on its
# own (observed: a device still mid-download 35s after being told to Stop
# and after leaving its group). Nothing else will ever stop it, and
# _on_room_found for this same device — creating a brand-new Speaker on
# top of what it thinks is a fresh, idle room — runs right after this in
# the same topology pass, so we verify (and retry) rather than fire-and-
# forget: a still-playing device would otherwise hand that new Speaker a
# transport state it never asked for, read by it as an external takeover.
_STOP_VERIFY_ATTEMPTS = 5
_STOP_VERIFY_INTERVAL_SECONDS = 0.4
_STOPPED_TRANSPORT_STATES = {None, "STOPPED", "NO_MEDIA_PRESENT"}


async def _stop_and_verify(client: DLNAClient, ip: str) -> None:
    """Send Stop and confirm the device actually stopped, re-sending it if
    not — a single Stop can silently not take (see module note above)."""
    for attempt in range(1, _STOP_VERIFY_ATTEMPTS + 1):
        await client.stop()
        state = await client.get_transport_info()
        if state in _STOPPED_TRANSPORT_STATES:
            return
        if attempt < _STOP_VERIFY_ATTEMPTS:
            await asyncio.sleep(_STOP_VERIFY_INTERVAL_SECONDS)
    logger.warning(f"Sonos discovery: could not confirm {ip} stopped after leaving its group")


class SonosController:
    """Owns continuous Sonos household discovery and the Speaker/Qobuz
    Connect session for every room it finds."""

    def __init__(
        self,
        *,
        api_client: "QobuzAPIClient",
        app_id: str,
        webui_http_port: int,
        event_subscriber: SonosEventSubscriber,
        hires_downsampling: bool = False,
    ) -> None:
        """
        Args:
            api_client: Authenticated Qobuz API client, shared with every
                room's Speaker.
            app_id: Qobuz app ID (from credential scraper), passed to
                every Speaker for its own Qobuz Connect session.
            webui_http_port: The shared web UI's own port — reserved so
                per-room speakers never collide with it (see
                config._assign_ports) and used to build the GENA callback
                URL (event_subscriber's route lives on the same shared
                app).
            event_subscriber: Shared SonosEventSubscriber whose aiohttp
                route was already registered before the app started
                serving — this controller claims it for as long as it's
                running, releasing it on stop().
            hires_downsampling: Forwarded uniformly to every auto-
                discovered room's DLNAConfig — auto-discovered rooms have
                no per-room config of their own to set this individually.
        """
        self._api_client = api_client
        self._app_id = app_id
        self._webui_http_port = webui_http_port
        self._event_subscriber = event_subscriber
        self._hires_downsampling = hires_downsampling

        self._discovery: Optional[SonosDiscoveryManager] = None
        # Keyed by SonosRoom.tracking_key (a group's own group_id) — the
        # key never changes across a coordinator handoff, only what it
        # points at does. See SonosDiscoveryManager's own docstring.
        self._speakers_by_group_id: dict[str, Speaker] = {}

    @property
    def speakers(self) -> list[Speaker]:
        """Every currently-running auto-discovered Speaker."""
        return list(self._speakers_by_group_id.values())

    async def start(self) -> None:
        """Start continuous Sonos household discovery."""
        self._discovery = SonosDiscoveryManager(
            on_room_found=self._on_room_found,
            on_room_lost=self._on_room_lost,
            on_room_renamed=self._on_room_renamed,
            on_room_retargeted=self._on_room_retargeted,
            on_room_members_departed=self._on_room_members_departed,
            on_room_pending=self._on_room_pending,
            # Enables GENA event subscription (near-instant topology change
            # detection) on top of the polling fallback, via the route
            # already registered on the shared app.
            event_subscriber=self._event_subscriber,
            http_port=self._webui_http_port,
        )
        await self._discovery.start()

        if self._speakers_by_group_id:
            names = ", ".join(s.name for s in self.speakers)
            logger.info(
                f"Sonos auto-discovery ready — {len(self._speakers_by_group_id)} room(s): {names}"
            )
        else:
            logger.info("Sonos auto-discovery started — no rooms found yet, will keep polling")

    async def stop(self) -> None:
        """Stop discovery and every Speaker/Qobuz Connect session it started.

        Only sends a live device Stop to a room actually being driven
        (Speaker.is_active) — shutting down (or logging out) must not
        interrupt a merely-discovered, idle Sonos room, the same
        principle applied throughout the individual callbacks below.
        """
        if self._discovery is not None:
            await self._discovery.stop()
            self._discovery = None

        if self._speakers_by_group_id:
            await asyncio.gather(
                *[s.stop(send_device_stop=s.is_active) for s in self.speakers],
                return_exceptions=True,
            )
        self._speakers_by_group_id.clear()

    # ------------------------------------------------------------------
    # SonosDiscoveryManager callbacks
    # ------------------------------------------------------------------

    async def _on_room_found(self, room: SonosRoom) -> bool:
        """Called when a new group coordinator appears.

        Returns True only once the Speaker is actually running — the
        caller treats False as "not yet known", so a failure here is
        naturally retried on the next poll with no extra bookkeeping.
        """
        # display_name is comma-joined member room names for an active
        # group ("Kitchen, Living Room"), matching how the Sonos app itself
        # labels a group — just the room name when playing solo.
        display_name = room.display_name
        new_id = slugify_name(display_name)
        if any(slugify_name(s.name) == new_id for s in self.speakers):
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
            dlna_hires_downsampling=self._hires_downsampling,
            auto_managed=True,
        )
        all_configs = [s._config for s in self.speakers] + [sc]
        _assign_ports(all_configs, webui_port=self._webui_http_port)

        # Deferred import — see the module docstring's note by the
        # TYPE_CHECKING block on why this can't be a top-level import.
        from qobuz_proxy.speaker import Speaker

        speaker = Speaker(config=sc, api_client=self._api_client, app_id=self._app_id)

        # Register (tracking key) before the slow part (speaker.start(): DLNA
        # connect, mDNS registration, ...) rather than after. Multiple newly
        # found rooms are started concurrently (see SonosDiscoveryManager),
        # and this whole method runs to this point with no `await` in
        # between — so by the time another concurrently-scheduled call
        # reaches its own name check / _assign_ports above, it already sees
        # this one reserved and can't compute a colliding name or port.
        # Rolled back below on failure.
        self._speakers_by_group_id[room.tracking_key] = speaker
        try:
            started = await speaker.start()
        except Exception:
            del self._speakers_by_group_id[room.tracking_key]
            raise
        if not started:
            del self._speakers_by_group_id[room.tracking_key]
            logger.warning(f"Sonos discovery: '{display_name}' failed to start")
            return False

        logger.info(f"Sonos discovery: added speaker '{display_name}' ({room.ip})")
        return True

    async def _on_room_lost(self, tracking_key: str) -> None:
        """Called once a group_id's pending grace period resolves as a
        real loss (see SonosDiscoveryManager's pending state) — every
        former member turned up in some other group, or the grace period
        simply ran out. Tears the Speaker down for good.

        Never sends a live device stop here: if this was the active
        speaker, _on_room_pending already stopped its coordinator the
        moment the group_id first went pending — there is nothing left to
        interrupt. If it wasn't active, there was never anything playing
        through us to stop in the first place."""
        speaker = self._speakers_by_group_id.pop(tracking_key, None)
        if speaker is None:
            return
        logger.info(f"Sonos discovery: removing speaker '{speaker.name}'")
        await speaker.stop(send_device_stop=False)

    async def _on_room_pending(self, tracking_key: str) -> None:
        """Called the moment a still-tracked group_id first stops being
        reported — see SonosDiscoveryManager's pending state for why this
        isn't yet treated as a real loss. Only matters for the speaker
        Qobuz is actually driving: if it's not active, leave it exactly as
        it is (still connected, still polling) — the pending period
        resolving either way costs it nothing.

        For the active one, detach immediately. The group's own
        coordinator may already be transitioning out of it — Sonos's own
        handoff mechanism doesn't reliably stop it for us (see the
        _stop_and_verify note above), and leaving this speaker's backend
        pointed at it risks a real split-brain: our backend still polling/
        commanding a device that a *different*, not-yet-recognized room is
        about to also claim as its own coordinator, with both ends
        thinking they're in charge. Detaching first, then retargeting once
        the group_id resolves (see _on_room_retargeted), makes the
        handoff strictly two-step: off, then onto whatever's confirmed
        next — never both at once."""
        speaker = self._speakers_by_group_id.get(tracking_key)
        if speaker is None or not speaker.is_active:
            return
        logger.info(f"Sonos discovery: '{speaker.name}' pending — detaching")
        await speaker.detach()

    async def _on_room_renamed(self, room: SonosRoom) -> bool:
        """Called when a group's display name changes (regrouping, or a
        room renamed in the Sonos app) but its coordinator/network identity
        didn't. Renames the existing Speaker in place instead of
        restarting it, so playback and position survive."""
        speaker = self._speakers_by_group_id.get(room.tracking_key)
        if speaker is None:
            return False  # not currently running; a found/lost pair will handle it

        new_name = room.display_name
        new_id = slugify_name(new_name)
        if any(slugify_name(s.name) == new_id and s is not speaker for s in self.speakers):
            logger.warning(
                f"Sonos discovery: rename to '{new_name}' collides with an "
                "existing speaker — skipping"
            )
            return False

        return await speaker.rename(new_name)

    async def _on_room_retargeted(self, room: SonosRoom) -> bool:
        """Called when a still-tracked group's coordinator (and/or its
        address) changed — repoints the existing Speaker's DLNA backend at
        the new target instead of tearing it down, so the Qobuz Connect
        session (WebSocket, mDNS registration) survives untouched. Also
        how a group_id coming back out of pending gets resolved (see
        _on_room_pending): Speaker.retarget() reconnects cleanly whether
        the backend is still attached to its old target or was detached
        while pending, even to the same ip/port as before. The group's
        tracking_key never changes, so there's nothing else to move."""
        speaker = self._speakers_by_group_id.get(room.tracking_key)
        if speaker is None:
            return False  # not currently running; a found/lost pair will handle it

        if not await speaker.retarget(room.ip, room.port):
            return False

        new_name = room.display_name
        if new_name != speaker.name:
            await speaker.rename(new_name)

        logger.info(f"Sonos discovery: retargeted speaker '{speaker.name}' to {room.ip}")
        return True

    async def _on_room_members_departed(
        self, tracking_key: str, departed: tuple[DepartedMember, ...]
    ) -> None:
        """Called whenever a still-tracked group's membership shrinks —
        for *any* group, whether we're playing to it or not
        (SonosDiscoveryManager has no notion of Qobuz playback state at
        all). Only the group we're actively playing to is our business: a
        device leaving it needs to be told to stop, since it's no longer
        part of what the Qobuz app thinks it's driving and nothing else
        will ever tell it to stop. A device leaving any other (merely
        discovered, not playing) group is Sonos's own business to
        (re)direct — touching it risks interrupting playback Sonos itself
        just set up as part of moving it there."""
        speaker = self._speakers_by_group_id.get(tracking_key)
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
                await _stop_and_verify(client, member.ip)
            except Exception as e:
                logger.debug(f"Could not stop departed device {member.ip}: {e}")
            finally:
                await client.disconnect()


__all__ = ["SonosController"]

"""
Continuous Sonos household auto-discovery, wired up as Speaker lifecycle.

SonosController is the consumer that turns SonosDiscoveryManager's
Sonos-topology-only callbacks (found/lost/renamed/retargeted/rekeyed/
members-departed) into actual Speaker/Qobuz-Connect-session lifecycle
actions. It owns every Speaker it creates end to end — the app only ever
starts/stops this controller and reads .speakers for cross-cutting
concerns (the web UI's speaker list, an app-wide shutdown), the same
shape as any other top-level component (DiscoveryService, Speaker itself).
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
            on_room_rekeyed=self._on_room_rekeyed,
            on_room_members_departed=self._on_room_members_departed,
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

    async def _on_room_lost(self, tracking_key: str, still_present: bool) -> None:
        """Called when a group's coordinator stops being a coordinator at
        all — either its last device went offline (still_present=False),
        or it was absorbed as a plain member into another group
        (still_present=True). Either way we give up *our* Speaker for it,
        but only send the device an explicit stop in the offline case —
        when it's still present, Sonos itself is already directing its
        audio as part of the other group, and a stop here would just
        interrupt that."""
        speaker = self._speakers_by_group_id.pop(tracking_key, None)
        if speaker is None:
            return
        logger.info(f"Sonos discovery: removing speaker '{speaker.name}'")
        await speaker.stop(send_device_stop=not still_present)

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
        """Called when a group's coordinator (and/or its address) changed
        — repoints the existing Speaker's DLNA backend at the new target
        instead of tearing it down, so the Qobuz Connect session
        (WebSocket, mDNS registration) survives untouched. Sonos already
        migrates the audio itself on a handoff; this just moves where
        future commands go and where state is polled from. The group's
        tracking_key never changes, so there's no re-keying to do."""
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

    async def _on_room_rekeyed(self, old_key: str, room: SonosRoom) -> bool:
        """Called when the *same* physical coordinator (uuid unchanged) is
        now tracked under a different key — its group_id changed for some
        reason other than an actual handoff (most likely a plain
        membership change to a group that otherwise didn't move). Moves
        the existing Speaker to live under the new key and applies any
        retarget/rename needed, instead of tearing it down and starting a
        fresh Qobuz Connect session for a coordinator that never actually
        went anywhere."""
        speaker = self._speakers_by_group_id.get(old_key)
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

        del self._speakers_by_group_id[old_key]
        self._speakers_by_group_id[room.tracking_key] = speaker
        logger.info(f"Sonos discovery: re-keyed speaker '{speaker.name}'")
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
        discovered, not playing) group is Sonos's own business — touching
        it risks exactly the play/mute race fixed for the
        absorbed-into-another-group case (see _on_room_lost)."""
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
                await client.stop()
            except Exception as e:
                logger.debug(f"Could not stop departed device {member.ip}: {e}")
            finally:
                await client.disconnect()


__all__ = ["SonosController"]

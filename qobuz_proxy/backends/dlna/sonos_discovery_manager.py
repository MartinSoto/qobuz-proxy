"""
Continuous Sonos household discovery.

Polls SSDP + the Sonos ZoneGroupTopology service on a fixed interval and
reports group coordinators as they appear/disappear, so a caller (app.py)
can start/stop a Speaker per room without any manual per-room
configuration. Deliberately simple for a first cut: polling rather than
UPnP (GENA) eventing, matching how the rest of this codebase watches
device state (see DLNABackend's own state-poll loop).

Only group *coordinators* are reported — a non-coordinator group member's
AVTransport doesn't accept new sources while grouped (see sonos_topology's
module docstring), so it isn't a valid independent playback target.
Coordinator-only reporting also means dynamic regrouping (a room becomes a
group's coordinator, or stops being one) is expressed as a plain
lost+found pair rather than an in-place update — simple, and safe, since
Speaker start/stop is already idempotent lifecycle machinery.

Resilience is intentionally conservative: a poll that finds no Sonos
device, or whose topology can't be parsed, changes nothing — the previous
snapshot is kept as-is rather than tearing every speaker down on a
transient network hiccup. Similarly, a room whose Speaker fails to start
is not recorded as "known", so it naturally reappears as newly-found on
the next poll — a free retry loop with no separate backoff bookkeeping.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from .discovery import discover_dlna_devices
from .sonos_topology import fetch_sonos_groups, fetch_sonos_topology

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 30.0
DEFAULT_SSDP_TIMEOUT_SECONDS = 3.0


@dataclass(frozen=True)
class SonosRoom:
    """One group coordinator, ready to become a Speaker."""

    uuid: str
    name: str
    ip: str
    port: int
    is_stereo_pair: bool
    # Visible member room names in topology order (coordinator first),
    # e.g. ("Kitchen", "Living Room") for an active dynamic group, or just
    # (name,) when playing solo. Bonded stereo pair / HT satellite members
    # are invisible and never appear here, so a stereo pair still yields a
    # single-element tuple.
    member_names: tuple[str, ...] = ()

    @property
    def display_name(self) -> str:
        """Name for the Qobuz Connect device — comma-joined room names like
        the Sonos app shows a group ("Kitchen, Living Room"), or just the
        room name when not grouped with anything else."""
        return ", ".join(self.member_names) if self.member_names else self.name


# Returns True if the room was successfully turned into a running Speaker —
# only then is it considered "known" until it's reported lost.
RoomFoundCallback = Callable[[SonosRoom], Awaitable[bool]]
RoomLostCallback = Callable[[str], Awaitable[None]]  # sonos uuid


class SonosDiscoveryManager:
    """Continuously discovers Sonos group coordinators and reports changes."""

    def __init__(
        self,
        on_room_found: RoomFoundCallback,
        on_room_lost: RoomLostCallback,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        ssdp_timeout: float = DEFAULT_SSDP_TIMEOUT_SECONDS,
    ) -> None:
        self._on_room_found = on_room_found
        self._on_room_lost = on_room_lost
        self._poll_interval = poll_interval
        self._ssdp_timeout = ssdp_timeout

        self._known: dict[str, SonosRoom] = {}
        self._task: Optional[asyncio.Task[None]] = None
        self._running = False

    async def start(self) -> None:
        """Run one poll synchronously (so speakers exist immediately), then
        keep polling in the background."""
        self._running = True
        await self._poll_once()
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Stop polling. Does not tear down any speakers it created — the
        caller owns that lifecycle."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._known.clear()

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._poll_interval)
                if not self._running:
                    break
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Sonos discovery poll failed: {e}")

    async def _poll_once(self) -> None:
        try:
            devices = await discover_dlna_devices(timeout=self._ssdp_timeout)
        except Exception as e:
            logger.debug(f"Sonos discovery: SSDP scan failed: {e}")
            return

        sonos_devices = [d for d in devices if "sonos" in d.manufacturer.lower()]
        if not sonos_devices:
            logger.debug("Sonos discovery: no Sonos devices found this cycle")
            return

        members = await fetch_sonos_topology(sonos_devices)
        groups = await fetch_sonos_groups(sonos_devices)
        if not members or not groups:
            logger.debug("Sonos discovery: topology unavailable this cycle")
            return

        current: dict[str, SonosRoom] = {}
        for g in groups:
            member = members.get(g.coordinator_uuid)
            if member is None or member.invisible or not member.ip:
                continue

            room_name = member.zone_name or g.coordinator_uuid
            # Visible members only — a bonded stereo pair's secondary (or an
            # HT satellite) is part of g.member_uuids too, but Invisible, so
            # it must not turn a solo room's display name into "X, X".
            # Coordinator first (matches how the Sonos app itself orders a
            # group's name), remaining rooms alphabetical.
            other_names = sorted(
                m.zone_name
                for uuid in g.member_uuids
                if uuid != g.coordinator_uuid
                and (m := members.get(uuid)) is not None
                and not m.invisible
                and m.zone_name
            )
            member_names = (room_name, *other_names)

            current[g.coordinator_uuid] = SonosRoom(
                uuid=g.coordinator_uuid,
                name=room_name,
                ip=member.ip,
                port=member.port,
                is_stereo_pair=member.is_stereo_pair,
                member_names=member_names,
            )

        removed = [uuid for uuid in self._known if uuid not in current]
        added = [room for uuid, room in current.items() if uuid not in self._known]
        changed = [
            room
            for uuid, room in current.items()
            if uuid in self._known and self._known[uuid] != room
        ]

        for uuid in removed:
            await self._report_lost(uuid)
        for room in changed:
            # Coordinator moved IP or was renamed — treat as lost+found
            # rather than patching a live Speaker in place.
            await self._report_lost(room.uuid)
        for room in added + changed:
            await self._report_found(room)

    async def _report_found(self, room: SonosRoom) -> None:
        try:
            started = await self._on_room_found(room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error starting speaker for '{room.name}': {e}")
            started = False
        if started:
            self._known[room.uuid] = room
        # else: left out of _known, so it's retried as "newly found" next poll

    async def _report_lost(self, uuid: str) -> None:
        self._known.pop(uuid, None)
        try:
            await self._on_room_lost(uuid)
        except Exception as e:
            logger.warning(f"Sonos discovery: error stopping speaker for {uuid}: {e}")


__all__ = ["SonosRoom", "SonosDiscoveryManager"]

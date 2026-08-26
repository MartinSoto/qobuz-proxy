"""
Continuous Sonos household discovery.

Watches SSDP + the Sonos ZoneGroupTopology service and reports group
coordinators as they appear/disappear, so a caller (app.py) can start/stop
a Speaker per room without any manual per-room configuration.

Two complementary mechanisms feed the same topology-diffing logic
(`_apply_topology`):

- **Polling** (always active): a full SSDP scan + GetZoneGroupState fetch on
  a fixed interval. Simple, matches how the rest of this codebase watches
  device state (DLNABackend's own state-poll loop), and is the sole
  mechanism if event subscription isn't set up or lapses.
- **GENA eventing** (active when a `web_app`/`http_port` are supplied): a
  SUBSCRIBE to one household member's ZoneGroupTopology event channel
  (see sonos_events.py) pushes a NOTIFY the moment anything actually
  changes, cutting detection latency from "up to one poll interval" to
  near-instant. When a subscription is healthy, polling backs off to an
  infrequent safety net instead of stopping — insurance against a missed
  NOTIFY or a subscription that silently lapsed, not the primary signal.

Only group *coordinators* are reported — a non-coordinator group member's
AVTransport doesn't accept new sources while grouped (see sonos_topology's
module docstring), so it isn't a valid independent playback target. A
room becoming a coordinator, or stopping being one, is reported as
found/lost/retargeted/rekeyed as appropriate (see those callbacks' own
docs) rather than always torn down and rebuilt.

Resilience is intentionally conservative: an update pass that finds no
Sonos device, or whose topology can't be parsed, changes nothing — the
previous snapshot is kept as-is rather than tearing every speaker down on
a transient network hiccup. Similarly, a room whose Speaker fails to start
is not recorded as "known", so it naturally reappears as newly-found on
the next update — a free retry loop with no separate backoff bookkeeping.

**"Active" is a Qobuz Connect concept, not a Sonos one.** This manager
only ever sees Sonos topology — it has no idea whether any of the groups
it's watching are actually playing Qobuz content right now (the Qobuz
server tells each Speaker that directly, via SRVR_RNDR_SET_ACTIVE). At
most one group is ever "the" active one at a time, but this manager can't
tell which, so on_room_members_departed reports every departure equally
and leaves it to the caller (app.py) to act only when it's the active
group that lost a member — see Speaker.is_active.
"""

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Awaitable, Callable, Optional

from .discovery import DiscoveredDevice, discover_dlna_devices
from .sonos_events import SonosEventSubscriber, get_local_ip
from .sonos_topology import (
    SonosGroup,
    SonosZoneMember,
    fetch_sonos_groups,
    fetch_sonos_topology,
    parse_zone_group_state,
    parse_zone_groups,
)

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 30.0
DEFAULT_SSDP_TIMEOUT_SECONDS = 3.0
# How often the background loop wakes to check whether it's time to poll
# and/or whether the event subscription needs renewal. Independent of the
# actual poll cadence, which varies (see _effective_poll_interval).
TICK_INTERVAL_SECONDS = 10.0
# Poll cadence once a GENA subscription is healthy — a safety net, not the
# primary change-detection mechanism, so it can be much less frequent.
SAFETY_NET_POLL_INTERVAL_SECONDS = 300.0


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
    # Same members, same order, as their own uuids instead of display names
    # — lets a diff against a previous snapshot's member_uuids tell exactly
    # *which* device left a group (member_names alone only says the name
    # changed), which is what departure detection needs.
    member_uuids: tuple[str, ...] = ()
    # The Sonos ZoneGroup's own id — confirmed stable across a coordinator
    # handoff, and this is the actual tracking identity (see tracking_key):
    # one group_id = one Qobuz renderer, for as long as that group_id
    # exists, regardless of which physical player currently coordinates it
    # or how many members it has. A demotion/promotion is therefore just an
    # ordinary in-place update to the one entity tracked under this key —
    # nothing to correlate across separate rooms.
    group_id: str = ""

    @property
    def tracking_key(self) -> str:
        """The identity SonosDiscoveryManager tracks this room under.
        group_id, falling back to the coordinator's own uuid on the
        (unexpected) chance group_id is unavailable."""
        return self.group_id or self.uuid

    @property
    def display_name(self) -> str:
        """Name for the Qobuz Connect device — comma-joined room names like
        the Sonos app shows a group ("Kitchen, Living Room"), or just the
        room name when not grouped with anything else."""
        return ", ".join(self.member_names) if self.member_names else self.name


@dataclass(frozen=True)
class DepartedMember:
    """A device that just left a group it was (visibly) a member of, still
    reachable at this ip/port as of the snapshot that noticed it leaving."""

    uuid: str
    ip: str
    port: int


def _departed_members(
    old: SonosRoom, new: SonosRoom, members: dict[str, SonosZoneMember]
) -> tuple[DepartedMember, ...]:
    """uuids in old.member_uuids but not new.member_uuids — i.e. members of
    *this* group that just left it — paired with their current ip/port
    from this snapshot's fresh topology (skipped if a departed uuid isn't
    in it at all, i.e. it went offline in the very same snapshot)."""
    result = []
    for uuid in set(old.member_uuids) - set(new.member_uuids):
        m = members.get(uuid)
        if m is not None and m.ip:
            result.append(DepartedMember(uuid=uuid, ip=m.ip, port=m.port))
    return tuple(result)


# Called when a still-tracked group's membership shrinks (its coordinator's
# own uuid/ip/port may or may not have also changed — orthogonal to
# found/lost/renamed/retargeted/rekeyed, which only look at the
# coordinator). Args: (tracking_key, departed members). It's the caller's
# job to decide whether tracking_key is the group it's actively playing to
# — see the module docstring's note on "active" — since this manager has
# no notion of Qobuz playback state at all, only Sonos topology.
RoomMembersDepartedCallback = Callable[[str, tuple[DepartedMember, ...]], Awaitable[None]]

# Returns True if the room was successfully turned into a running Speaker —
# only then is it considered "known" until it's reported lost.
RoomFoundCallback = Callable[[SonosRoom], Awaitable[bool]]
# Args: (tracking_key, still_present). still_present is True when the
# coordinator's own uuid is still visible somewhere in the current
# topology — it just isn't a coordinator anymore, having been absorbed as
# a plain member into another group. That device hasn't gone anywhere and
# Sonos is already directing its audio, so tearing down its Speaker must
# not also send it a live device stop (see Speaker.stop()) — that would
# interrupt the very playback Sonos just set up. False means the device
# genuinely dropped off the household (offline, powered down, etc.).
RoomLostCallback = Callable[[str, bool], Awaitable[None]]
# Returns True if the rename was applied — only then does the manager stop
# retrying it on subsequent updates.
RoomRenamedCallback = Callable[[SonosRoom], Awaitable[bool]]
# Repoint the Speaker already tracked under this room's tracking_key at its
# (possibly new) coordinator ip/port — the key itself didn't change, so
# there's no re-keying to do, just "the same group, new physical target."
# Returns True if the retarget succeeded.
RoomRetargetedCallback = Callable[[SonosRoom], Awaitable[bool]]
# The *same* physical coordinator (uuid unchanged) is now tracked under a
# different tracking_key — group_id is only confirmed stable across an
# actual coordinator handoff, not across every topology mutation (e.g. a
# plain membership change to an otherwise-untouched group can apparently
# mint a fresh group_id too). Move the Speaker already running for
# old_key to live under room.tracking_key instead of tearing it down and
# starting a fresh one — the coordinator never actually changed, so
# there's nothing here for a listener to even notice. Returns True if
# applied.
RoomRekeyedCallback = Callable[[str, SonosRoom], Awaitable[bool]]


class SonosDiscoveryManager:
    """Continuously discovers Sonos group coordinators and reports changes."""

    def __init__(
        self,
        on_room_found: RoomFoundCallback,
        on_room_lost: RoomLostCallback,
        on_room_renamed: RoomRenamedCallback,
        on_room_retargeted: RoomRetargetedCallback,
        on_room_rekeyed: RoomRekeyedCallback,
        on_room_members_departed: RoomMembersDepartedCallback,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        ssdp_timeout: float = DEFAULT_SSDP_TIMEOUT_SECONDS,
        event_subscriber: Optional[SonosEventSubscriber] = None,
        http_port: int = 0,
    ) -> None:
        """
        Args:
            event_subscriber: A SonosEventSubscriber whose aiohttp route was
                already registered before the app started serving (routes
                can't be added afterwards — aiohttp freezes the router once
                AppRunner.setup() runs, which happens well before a
                SonosDiscoveryManager exists, since that's only created
                after login). This manager claims it by setting its
                `on_notify` for as long as it's running, releasing it on
                stop(). Combined with `http_port`, enables event
                subscription; omit either to fall back to polling only.
            http_port: Port the subscriber's app is listening on — needed
                to build the callback URL Sonos devices push NOTIFYs to.
        """
        self._on_room_found = on_room_found
        self._on_room_lost = on_room_lost
        self._on_room_renamed = on_room_renamed
        self._on_room_retargeted = on_room_retargeted
        self._on_room_rekeyed = on_room_rekeyed
        self._on_room_members_departed = on_room_members_departed
        self._poll_interval = poll_interval
        self._ssdp_timeout = ssdp_timeout

        self._known: dict[str, SonosRoom] = {}
        self._task: Optional[asyncio.Task[None]] = None
        self._running = False
        self._last_poll_at: float = 0.0

        self._subscriber: Optional[SonosEventSubscriber] = None
        self._callback_url: Optional[str] = None
        if event_subscriber is not None and http_port:
            local_ip = get_local_ip()
            if local_ip:
                self._subscriber = event_subscriber
                self._subscriber.on_notify = self._handle_notify
                self._callback_url = (
                    f"http://{local_ip}:{http_port}{event_subscriber.callback_path}"
                )
            else:
                logger.warning(
                    "Could not determine local IP — Sonos topology event "
                    "subscription disabled, falling back to polling only"
                )

    async def start(self) -> None:
        """Run one update pass synchronously (so speakers exist
        immediately), attempt an event subscription if configured, then
        keep watching in the background."""
        self._running = True
        await self._poll_once()
        self._last_poll_at = time.monotonic()
        if self._subscriber is not None:
            await self._try_subscribe()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop watching. Does not tear down any speakers it created — the
        caller owns that lifecycle."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._subscriber is not None:
            await self._subscriber.unsubscribe()
            self._subscriber.on_notify = None  # release the shared subscriber
        self._known.clear()

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(TICK_INTERVAL_SECONDS)
                if not self._running:
                    break
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Sonos discovery tick failed: {e}")

    async def _tick(self) -> None:
        subscription_healthy = False
        if self._subscriber is not None:
            sub = self._subscriber.subscription
            if sub is None:
                await self._try_subscribe()
                sub = self._subscriber.subscription
            elif sub.needs_renewal:
                if await self._subscriber.renew():
                    sub = self._subscriber.subscription
                else:
                    logger.info("Sonos discovery: event subscription lost, will retry")
                    sub = None
            subscription_healthy = sub is not None

        interval = SAFETY_NET_POLL_INTERVAL_SECONDS if subscription_healthy else self._poll_interval
        if time.monotonic() - self._last_poll_at >= interval:
            await self._poll_once()
            self._last_poll_at = time.monotonic()

    async def _try_subscribe(self) -> None:
        if self._subscriber is None or self._callback_url is None:
            return
        sonos_devices = await self._discover_sonos_devices()
        if not sonos_devices:
            return
        await self._subscriber.subscribe(sonos_devices, self._callback_url)

    async def _discover_sonos_devices(self) -> list[DiscoveredDevice]:
        try:
            devices = await discover_dlna_devices(timeout=self._ssdp_timeout)
        except Exception as e:
            logger.debug(f"Sonos discovery: SSDP scan failed: {e}")
            return []
        return [d for d in devices if "sonos" in d.manufacturer.lower()]

    async def _poll_once(self) -> None:
        sonos_devices = await self._discover_sonos_devices()
        if not sonos_devices:
            logger.debug("Sonos discovery: no Sonos devices found this cycle")
            return

        members = await fetch_sonos_topology(sonos_devices)
        groups = await fetch_sonos_groups(sonos_devices)
        if not members or not groups:
            logger.debug("Sonos discovery: topology unavailable this cycle")
            return

        await self._apply_topology(members, groups)

    async def _handle_notify(self, body: str) -> None:
        """Callback for SonosEventSubscriber: a GENA NOTIFY pushed fresh
        topology directly, no SSDP scan or SOAP round trip needed."""
        members = parse_zone_group_state(body)
        groups = parse_zone_groups(body)
        if not members or not groups:
            logger.debug("Sonos discovery: NOTIFY body could not be parsed")
            return
        logger.debug("Sonos discovery: applying topology pushed via NOTIFY")
        await self._apply_topology(members, groups)

    async def _apply_topology(
        self,
        members: dict[str, SonosZoneMember],
        groups: list[SonosGroup],
    ) -> None:
        # Tracked by group_id (SonosRoom.tracking_key), not by coordinator
        # uuid: a group's coordinator changing is then just an ordinary
        # in-place update to the one entity already tracked under that key
        # — never two separate rooms to correlate, whether the change
        # arrives in one topology snapshot or is spread across several
        # (e.g. a user's two separate taps in the Sonos app to swap which
        # room plays). One group_id = one Qobuz renderer, for as long as
        # that group_id exists, regardless of who currently coordinates it
        # or how many members it has.
        #
        # That stability is only confirmed across an actual handoff, though
        # — group_id can apparently also change on a plain membership
        # change to a group whose coordinator never moved. The rekey pass
        # below (see `rekeyed`) catches that case via the coordinator's own
        # uuid, so it isn't misread as the group disappearing.
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
            # group's name), remaining rooms alphabetical — uuids sorted
            # alongside so member_uuids[i] is member_names[i]'s own uuid.
            other = sorted(
                (m.zone_name, uuid)
                for uuid in g.member_uuids
                if uuid != g.coordinator_uuid
                and (m := members.get(uuid)) is not None
                and not m.invisible
                and m.zone_name
            )
            member_names = (room_name, *(name for name, _ in other))
            member_uuids = (g.coordinator_uuid, *(uuid for _, uuid in other))

            room = SonosRoom(
                uuid=g.coordinator_uuid,
                name=room_name,
                ip=member.ip,
                port=member.port,
                is_stereo_pair=member.is_stereo_pair,
                member_names=member_names,
                member_uuids=member_uuids,
                group_id=g.group_id,
            )
            current[room.tracking_key] = room

        removed_keys = [key for key in self._known if key not in current]
        added_keys = [key for key in current if key not in self._known]

        # A key disappearing and a *different* key appearing for the same
        # physical coordinator (uuid unchanged) isn't a real loss — it's
        # group_id churning for a reason other than a handoff (a plain
        # membership change, most likely). Correlate by uuid within this
        # one snapshot and rekey the existing Speaker in place instead of
        # tearing it down and spinning up a fresh Qobuz Connect session
        # (which, worse, sends the still-playing coordinator a real DLNA
        # Stop as a side effect of tearing down the "lost" one).
        removed_by_uuid = {self._known[key].uuid: key for key in removed_keys}
        rekeyed: list[tuple[str, SonosRoom]] = []  # (old_key, new_room)
        added: list[SonosRoom] = []
        for key in added_keys:
            room = current[key]
            old_key = removed_by_uuid.pop(room.uuid, None)
            if old_key is not None:
                rekeyed.append((old_key, room))
            else:
                added.append(room)
        removed = list(removed_by_uuid.values())

        # Same key (group_id) as before, but something about it changed.
        # Whoever's coordinating now (uuid/ip/port) is a physical-target
        # change — retarget in place, keeping the Qobuz Connect session
        # alive throughout (Sonos already migrates the audio itself).
        # Anything else (membership, name) is cosmetic — rename in place.
        #
        # Orthogonally, whenever a persisting entity's membership shrinks —
        # whether it was classified retargeted, renamed, or rekeyed above —
        # report exactly who left. This manager has no idea whether that
        # matters (only the caller knows which group, if any, is actively
        # playing — see the module docstring), so every departure is
        # reported equally; deciding what to do about it is the caller's
        # job entirely.
        retargeted: list[SonosRoom] = []
        renamed: list[SonosRoom] = []
        departures: list[tuple[str, tuple[DepartedMember, ...]]] = []
        for key, room in current.items():
            if key not in self._known:
                continue
            old = self._known[key]
            if old == room:
                continue
            if old.uuid != room.uuid or old.ip != room.ip or old.port != room.port:
                retargeted.append(room)
            else:
                renamed.append(room)
            departed = _departed_members(old, room, members)
            if departed:
                departures.append((key, departed))
        for old_key, room in rekeyed:
            departed = _departed_members(self._known[old_key], room, members)
            if departed:
                departures.append((old_key, departed))

        changed = bool(removed or rekeyed or added or renamed or retargeted)
        if changed and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Sonos discovery: topology changed:\n"
                + json.dumps(
                    {
                        "members": {uuid: asdict(m) for uuid, m in members.items()},
                        "groups": [asdict(g) for g in groups],
                        "known_before": {key: asdict(room) for key, room in self._known.items()},
                        "diff": {
                            "removed": removed,
                            "rekeyed": [
                                {"old_key": old_key, "new_key": room.tracking_key}
                                for old_key, room in rekeyed
                            ],
                            "added": [room.tracking_key for room in added],
                            "renamed": [room.tracking_key for room in renamed],
                            "retargeted": [room.tracking_key for room in retargeted],
                            "members_departed": {
                                key: [d.uuid for d in departed] for key, departed in departures
                            },
                        },
                    },
                    indent=2,
                    default=str,
                )
            )

        # Each category runs concurrently within itself (distinct rooms/
        # devices, no shared per-item state — see _report_found's docstring
        # and app.py's _on_sonos_room_found for the one case, a fresh
        # speaker's name/port reservation, that needed care to stay safe
        # under concurrency). Categories stay sequential relative to each
        # other: a room's departures/loss/rekey must be settled before it's
        # treated as newly found/renamed/retargeted.
        await asyncio.gather(
            *(self._report_members_departed(key, departed) for key, departed in departures)
        )
        await asyncio.gather(
            *(
                # Still visible in this snapshot's topology (just not a
                # coordinator anymore) means it was absorbed as a plain
                # member into another group, not that it went offline —
                # see RoomLostCallback.
                self._report_lost(key, self._known[key].uuid in members)
                for key in removed
            )
        )
        await asyncio.gather(*(self._report_rekeyed(old_key, room) for old_key, room in rekeyed))
        await asyncio.gather(*(self._report_found(room) for room in added))
        await asyncio.gather(*(self._report_renamed(room) for room in renamed))
        await asyncio.gather(*(self._report_retargeted(room) for room in retargeted))

    async def _report_members_departed(
        self, tracking_key: str, departed: tuple[DepartedMember, ...]
    ) -> None:
        # Purely informational — doesn't touch self._known, since it has no
        # bearing on the coordinator entity's own found/lost/renamed/
        # retargeted/rekeyed status (that's tracked independently above).
        try:
            await self._on_room_members_departed(tracking_key, departed)
        except Exception as e:
            logger.warning(
                f"Sonos discovery: error reporting members departed from {tracking_key}: {e}"
            )

    async def _report_found(self, room: SonosRoom) -> None:
        try:
            started = await self._on_room_found(room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error starting speaker for '{room.name}': {e}")
            started = False
        if started:
            self._known[room.tracking_key] = room
        # else: left out of _known, so it's retried as "newly found" next update

    async def _report_lost(self, key: str, still_present: bool) -> None:
        self._known.pop(key, None)
        try:
            await self._on_room_lost(key, still_present)
        except Exception as e:
            logger.warning(f"Sonos discovery: error stopping speaker for {key}: {e}")

    async def _report_renamed(self, room: SonosRoom) -> None:
        try:
            renamed = await self._on_room_renamed(room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error renaming speaker for '{room.name}': {e}")
            renamed = False
        if renamed:
            self._known[room.tracking_key] = room
        # else: _known keeps the stale name, so the rename is retried next update

    async def _report_rekeyed(self, old_key: str, room: SonosRoom) -> None:
        try:
            applied = await self._on_room_rekeyed(old_key, room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error rekeying speaker for '{room.name}': {e}")
            applied = False
        if applied:
            self._known.pop(old_key, None)
            self._known[room.tracking_key] = room
        # else: self._known keeps the stale entry under old_key, so this is
        # retried next update — same free-retry pattern as found/renamed.

    async def _report_retargeted(self, room: SonosRoom) -> bool:
        try:
            retargeted = await self._on_room_retargeted(room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error retargeting speaker for '{room.name}': {e}")
            retargeted = False
        if retargeted:
            self._known[room.tracking_key] = room
        # else: _known keeps the stale entry, so this is retried next
        # update — same free-retry pattern as found/renamed.
        return retargeted


__all__ = ["SonosRoom", "DepartedMember", "SonosDiscoveryManager"]

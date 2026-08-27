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
found/lost/renamed/retargeted as appropriate (see those callbacks' own
docs) rather than always torn down and rebuilt.

**A group_id is the sole, permanent identity of a group — never reused,
never reassigned to a different group, for as long as that group exists.**
See topology.py's SonosGroup docstring for what "as long as that group
exists" actually covers (a coordinator handoff, and a membership edit that
swaps the group's entire composition, both preserve it; only a room that's
actually removed from the group gets a new id of its own). A group_id that
shows up under a different key than before is therefore never "the same
group renamed" — it is a new, distinct group, full stop. There is no
correlation-by-coordinator-uuid fallback: that was tried (see git history)
and turned out to guess wrong in exactly the handoff case that matters,
because a real handoff can transiently look identical to a coordinator
peeling off to start its own new group before the topology settles.

That means a group_id can genuinely, if briefly, disappear from the
topology altogether mid-handoff (observed directly: a household's
GetZoneGroupState response omitted a still-live group for one update, then
reported it again one update later under a different coordinator). Tearing
the Speaker down the instant its group_id is momentarily unreported would
both destroy a session that didn't need to die and — worse, for whichever
group is actively playing — leave two physical devices thinking they're in
charge at once (the old coordinator never told to stop, the newly-found
"different" room never told it's a continuation) — see the module's
_apply_topology for how a vanished group_id is held pending rather than
declared lost outright.

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
tell which, so on_room_members_departed/on_room_pending report every
group equally and leave it to the caller (app.py) to act only when it's
the active group affected — see Speaker.is_active.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from ..discovery import DiscoveredDevice, discover_dlna_devices
from .events import SonosEventSubscriber, get_local_ip
from .topology import (
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
# While anything is pending (see PENDING_GRACE_SECONDS), the loop wakes at
# this much shorter cadence instead, so a pending group's grace period
# expires close to on time rather than up to a full TICK_INTERVAL_SECONDS
# late. Reverts to the normal cadence once nothing is pending.
PENDING_TICK_INTERVAL_SECONDS = 1.0
# Poll cadence once a GENA subscription is healthy — a safety net, not the
# primary change-detection mechanism, so it can be much less frequent.
SAFETY_NET_POLL_INTERVAL_SECONDS = 300.0
# How long a vanished group_id is held pending before its Speaker is
# actually torn down — see the module docstring. Resolved early, well
# before this elapses, the moment either the group_id reappears (any
# coordinator) or every one of its former members is confirmed to have
# turned up in some other group this same update.
PENDING_GRACE_SECONDS = 10.0


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
    # The Sonos ZoneGroup's own id — see topology.py's SonosGroup for what
    # this is guaranteed to survive. This is the sole tracking identity
    # (see tracking_key): one group_id = one Qobuz renderer, for as long as
    # that group_id exists, regardless of which physical player currently
    # coordinates it or how many members it has.
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


@dataclass(frozen=True)
class _PendingRoom:
    """A group_id that just vanished from the topology, held for possible
    resolution instead of being declared lost outright — see
    PENDING_GRACE_SECONDS and the module docstring."""

    room: SonosRoom  # last known state, before it vanished
    since: float  # time.monotonic() when it first went pending


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


def _describe_member(uuid: str, members: dict[str, SonosZoneMember]) -> dict[str, object]:
    """uuid + zone_name for one topology-changed log entry — looked up
    defensively rather than by direct indexing, because a coordinator or
    member going briefly unresolvable in `members` is exactly the kind of
    incomplete response this module exists to survive (see module
    docstring), not something the log itself should ever crash on."""
    member = members.get(uuid)
    if member is None:
        return {"uuid": uuid, "zone_name": "?"}
    return {"uuid": member.uuid, "zone_name": member.zone_name}


# Called when a still-tracked group's membership shrinks (its coordinator's
# own uuid/ip/port may or may not have also changed — orthogonal to
# found/lost/renamed/retargeted, which only look at the coordinator). Args:
# (tracking_key, departed members). It's the caller's job to decide whether
# tracking_key is the group it's actively playing to — see the module
# docstring's note on "active" — since this manager has no notion of Qobuz
# playback state at all, only Sonos topology.
RoomMembersDepartedCallback = Callable[[str, tuple[DepartedMember, ...]], Awaitable[None]]

# Returns True if the room was successfully turned into a running Speaker —
# only then is it considered "known" until it's reported pending/lost.
RoomFoundCallback = Callable[[SonosRoom], Awaitable[bool]]
# A group_id just vanished from the topology and is now pending (see
# PENDING_GRACE_SECONDS) rather than declared lost outright. Purely
# advisory — this manager doesn't tear anything down or forget the room
# because of it (that only happens via on_room_lost, once the grace period
# actually resolves as a real loss). The caller's typical use: if this
# group is the one actively being played to, stop driving its coordinator
# now rather than risk it and a later "different" room both thinking
# they're in charge (see the module docstring).
RoomPendingCallback = Callable[[str], Awaitable[None]]
# The pending grace period resolved as a real loss — every former member
# was confirmed elsewhere, or the grace period simply ran out. Tear the
# Speaker down; nothing further will be reported for this tracking_key.
RoomLostCallback = Callable[[str], Awaitable[None]]
# Returns True if the rename was applied — only then does the manager stop
# retrying it on subsequent updates.
RoomRenamedCallback = Callable[[SonosRoom], Awaitable[bool]]
# Repoint the Speaker already tracked under this room's tracking_key at its
# (possibly new) coordinator ip/port — the group_id itself didn't change,
# so there's no identity change to apply, just "the same group, new
# physical target." Also used to resolve a group_id coming back out of
# pending (see RoomPendingCallback) — the Speaker may currently be
# detached from any controller, in which case this reconnects it rather
# than no-opping. Returns True if the retarget succeeded.
RoomRetargetedCallback = Callable[[SonosRoom], Awaitable[bool]]


class SonosDiscoveryManager:
    """Continuously discovers Sonos group coordinators and reports changes."""

    def __init__(
        self,
        on_room_found: RoomFoundCallback,
        on_room_lost: RoomLostCallback,
        on_room_renamed: RoomRenamedCallback,
        on_room_retargeted: RoomRetargetedCallback,
        on_room_members_departed: RoomMembersDepartedCallback,
        on_room_pending: RoomPendingCallback,
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
        self._on_room_members_departed = on_room_members_departed
        self._on_room_pending = on_room_pending
        self._poll_interval = poll_interval
        self._ssdp_timeout = ssdp_timeout

        # Every tracked group_id lives in exactly one of these two — never
        # both, never neither once it's been seen.
        self._known: dict[str, SonosRoom] = {}
        self._pending: dict[str, _PendingRoom] = {}

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
        self._pending.clear()

    async def _loop(self) -> None:
        while self._running:
            try:
                interval = PENDING_TICK_INTERVAL_SECONDS if self._pending else TICK_INTERVAL_SECONDS
                await asyncio.sleep(interval)
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

        # Independent of whether a poll just ran above: a pending group_id
        # that's simply timed out (PENDING_GRACE_SECONDS) with no fresh
        # topology arriving at all — e.g. the safety-net poll interval is
        # 300s once GENA is healthy — still needs to be finalized. The
        # "confirmed elsewhere" resolution only has fresh data to work with
        # inside _apply_topology, so this sweep is timeout-only.
        await self._reap_pending(current=None)

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
        # Tracked by group_id (SonosRoom.tracking_key) alone — see the
        # module docstring and topology.py's SonosGroup for why that's the
        # only identity this manager ever correlates through.
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

        # A known key that's no longer in `current` doesn't mean the group
        # is gone — it may just be a transiently incomplete topology
        # response mid-handoff (see module docstring). Hold it pending
        # rather than tearing it down; _reap_pending resolves it once
        # there's enough evidence either way.
        newly_pending_keys = [key for key in self._known if key not in current]

        # A pending key reappearing — under *any* coordinator — is the
        # strongest possible continuity signal there is: it's Sonos's own
        # word that this is still the same group. Always wins, regardless
        # of how long it was pending.
        reappeared_keys = [key for key in current if key in self._pending]

        # Genuinely new: never seen before, and not a pending group_id
        # coming back.
        added = [
            current[key] for key in current if key not in self._known and key not in self._pending
        ]

        # Same key as before, but something about it changed. Whoever's
        # coordinating now (uuid/ip/port) is a physical-target change —
        # retarget in place, keeping the Qobuz Connect session alive
        # throughout (Sonos already migrates the audio itself). Anything
        # else (membership, name) is cosmetic — rename in place.
        #
        # Orthogonally, whenever a persisting or just-reappeared entity's
        # membership shrunk, report exactly who left. This manager has no
        # idea whether that matters (only the caller knows which group, if
        # any, is actively playing — see the module docstring), so every
        # departure is reported equally; deciding what to do about it is
        # the caller's job entirely.
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
        for key in reappeared_keys:
            room = current[key]
            departed = _departed_members(self._pending[key].room, room, members)
            if departed:
                departures.append((key, departed))

        changed = bool(newly_pending_keys or reappeared_keys or added or renamed or retargeted)
        if changed:
            self._log_topology(members, groups)

        # Each category runs concurrently within itself (distinct rooms/
        # devices, no shared per-item state — see _report_found's docstring
        # and app.py's _on_sonos_room_found for the one case, a fresh
        # speaker's name/port reservation, that needed care to stay safe
        # under concurrency). Categories stay sequential relative to each
        # other: a room's departures/pending status must be settled before
        # it's treated as newly found/renamed/retargeted, and reappearances
        # must be resolved before the pending sweep runs (so a group_id
        # that just came back is never also reaped in the same pass).
        await asyncio.gather(
            *(self._report_members_departed(key, departed) for key, departed in departures)
        )
        await asyncio.gather(*(self._report_pending(key) for key in newly_pending_keys))
        await asyncio.gather(*(self._report_found(room) for room in added))
        await asyncio.gather(*(self._report_renamed(room) for room in renamed))
        await asyncio.gather(*(self._report_retargeted(room) for room in retargeted))
        await asyncio.gather(*(self._report_reappeared(current[key]) for key in reappeared_keys))
        await self._reap_pending(current)

    def _log_topology(
        self,
        members: dict[str, SonosZoneMember],
        groups: list[SonosGroup],
    ) -> None:
        """Log the raw household topology at INFO whenever something in it
        actually changed — the main tool for diagnosing a live household's
        exact behavior against what we assumed it would do. One entry per
        *dynamic* group (see topology.py's SonosGroup — the coordinator
        plus whoever it's currently combined with to play together, not
        the household's static rooms), coordinator called out separately
        from its other members since it's the only one commands can
        target. Bonded stereo pairs and HT satellites are never dynamic
        group members in their own right (SonosGroup.member_uuids only
        ever holds direct ZoneGroupMember children — see topology.py's
        module docstring), so nothing here needs to filter Invisible
        entries the way discovery/enrichment does."""
        if not logger.isEnabledFor(logging.INFO):
            return
        logger.info(
            "Sonos discovery: topology changed:\n"
            + json.dumps(
                [
                    {
                        "group_id": g.group_id,
                        "coordinator": _describe_member(g.coordinator_uuid, members),
                        "other_members": [
                            _describe_member(uuid, members)
                            for uuid in g.member_uuids
                            if uuid != g.coordinator_uuid
                        ],
                    }
                    for g in groups
                ],
                indent=2,
            )
        )

    async def _report_members_departed(
        self, tracking_key: str, departed: tuple[DepartedMember, ...]
    ) -> None:
        # Purely informational — doesn't touch self._known/_pending, since
        # it has no bearing on the coordinator entity's own found/lost/
        # renamed/retargeted/pending status (that's tracked independently).
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

    async def _report_pending(self, key: str) -> None:
        room = self._known.pop(key)
        self._pending[key] = _PendingRoom(room=room, since=time.monotonic())
        try:
            await self._on_room_pending(key)
        except Exception as e:
            logger.warning(f"Sonos discovery: error handling pending group {key}: {e}")
        # No retry bookkeeping here — on_room_pending is advisory (typically
        # "detach if this was active"); the pending state itself is tracked
        # regardless of its outcome, and _reap_pending is what eventually
        # resolves it one way or the other.

    async def _report_renamed(self, room: SonosRoom) -> None:
        try:
            renamed = await self._on_room_renamed(room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error renaming speaker for '{room.name}': {e}")
            renamed = False
        if renamed:
            self._known[room.tracking_key] = room
        # else: _known keeps the stale name, so the rename is retried next update

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

    async def _report_reappeared(self, room: SonosRoom) -> None:
        """A previously-pending group_id is back — resolve it exactly like
        an ordinary retarget (which also renames if needed, and no-ops if
        the coordinator/address turns out unchanged), then drop it from
        _pending either way. A failed retarget still leaves the room
        trackable under its last-known (stale) state, so the normal
        retarget-detection diff above naturally retries it next update —
        same free-retry pattern as everything else here."""
        pending = self._pending.pop(room.tracking_key, None)
        stale = pending.room if pending is not None else room
        try:
            retargeted = await self._on_room_retargeted(room)
        except Exception as e:
            logger.warning(f"Sonos discovery: error retargeting speaker for '{room.name}': {e}")
            retargeted = False
        self._known[room.tracking_key] = room if retargeted else stale

    async def _reap_pending(self, current: Optional[dict[str, SonosRoom]]) -> None:
        """Finalize pending group_ids that are either confirmed gone (every
        former member now accounted for in some other group this update)
        or have simply waited long enough. `current` is the freshly parsed
        topology when called from _apply_topology (enabling the
        confirmed-elsewhere check); None when called from the plain timer
        tick, which can only ever check the timeout."""
        if not self._pending:
            return
        now = time.monotonic()
        accounted_for: set[str] = set()
        if current is not None:
            for room in current.values():
                accounted_for.update(room.member_uuids)

        to_finalize = [
            key
            for key, pending in self._pending.items()
            if now - pending.since >= PENDING_GRACE_SECONDS
            or (
                current is not None
                and all(uuid in accounted_for for uuid in pending.room.member_uuids)
            )
        ]
        await asyncio.gather(*(self._report_lost(key) for key in to_finalize))

    async def _report_lost(self, key: str) -> None:
        self._pending.pop(key, None)
        try:
            await self._on_room_lost(key)
        except Exception as e:
            logger.warning(f"Sonos discovery: error stopping speaker for {key}: {e}")


__all__ = ["SonosRoom", "DepartedMember", "SonosDiscoveryManager"]

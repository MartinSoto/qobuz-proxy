"""Tests for continuous Sonos household discovery (poll/diff/retry logic)."""

from unittest.mock import AsyncMock, MagicMock, patch
from xml.sax.saxutils import escape

from aiohttp import web

from qobuz_proxy.backends.dlna.discovery import DiscoveredDevice
from qobuz_proxy.backends.dlna.sonos_discovery_manager import (
    SAFETY_NET_POLL_INTERVAL_SECONDS,
    SonosDiscoveryManager,
    SonosRoom,
)
from qobuz_proxy.backends.dlna.sonos_events import SonosEventSubscriber
from qobuz_proxy.backends.dlna.sonos_topology import SonosGroup, SonosZoneMember

MODULE = "qobuz_proxy.backends.dlna.sonos_discovery_manager"

SONOS_DEVICE = DiscoveredDevice(
    friendly_name="Kitchen", ip="10.0.1.30", port=1400, manufacturer="Sonos, Inc."
)


def _member(
    uuid: str, name: str, ip: str, port: int = 1400, invisible: bool = False
) -> SonosZoneMember:
    return SonosZoneMember(
        uuid=uuid, zone_name=name, invisible=invisible, is_stereo_pair=False, ip=ip, port=port
    )


def _make_manager(
    on_found=None, on_lost=None, on_renamed=None, on_retargeted=None
) -> tuple[SonosDiscoveryManager, list, list, list, list]:
    found_calls: list = []
    lost_calls: list = []
    renamed_calls: list = []
    retargeted_calls: list = []

    async def default_on_found(room: SonosRoom) -> bool:
        found_calls.append(room)
        return True

    async def default_on_lost(tracking_key: str) -> None:
        lost_calls.append(tracking_key)

    async def default_on_renamed(room: SonosRoom) -> bool:
        renamed_calls.append(room)
        return True

    async def default_on_retargeted(room: SonosRoom) -> bool:
        retargeted_calls.append(room)
        return True

    manager = SonosDiscoveryManager(
        on_room_found=on_found or default_on_found,
        on_room_lost=on_lost or default_on_lost,
        on_room_renamed=on_renamed or default_on_renamed,
        on_room_retargeted=on_retargeted or default_on_retargeted,
    )
    return manager, found_calls, lost_calls, renamed_calls, retargeted_calls


class TestPollOnce:
    async def test_new_room_reported_found(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls, _ = _make_manager()

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(return_value={"RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30")}),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"])
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        assert len(found_calls) == 1
        assert found_calls[0] == SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen",),
        )
        assert found_calls[0].display_name == "Kitchen"
        assert lost_calls == []
        assert "RINCON_A" in manager._known

    async def test_no_devices_found_keeps_previous_state(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls, _ = _make_manager()
        manager._known = {
            "RINCON_A": SonosRoom("RINCON_A", "Kitchen", "10.0.1.30", 1400, False, ("Kitchen",))
        }

        with patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[])):
            await manager._poll_once()

        assert found_calls == []
        assert lost_calls == []
        assert "RINCON_A" in manager._known  # untouched, not torn down

    async def test_topology_unavailable_keeps_previous_state(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls, _ = _make_manager()
        manager._known = {
            "RINCON_A": SonosRoom("RINCON_A", "Kitchen", "10.0.1.30", 1400, False, ("Kitchen",))
        }

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=None)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=None)),
        ):
            await manager._poll_once()

        assert found_calls == []
        assert lost_calls == []
        assert "RINCON_A" in manager._known

    async def test_failed_room_start_is_retried_next_poll(self) -> None:
        results = [False, True]
        calls: list[SonosRoom] = []

        async def flaky_on_found(room: SonosRoom) -> bool:
            calls.append(room)
            return results.pop(0)

        manager, _, _, _renamed_calls, _ = _make_manager(on_found=flaky_on_found)

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(return_value={"RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30")}),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"])
                    ]
                ),
            ),
        ):
            await manager._poll_once()  # fails
            assert "RINCON_A" not in manager._known
            await manager._poll_once()  # succeeds

        assert len(calls) == 2  # retried automatically, no separate backoff needed
        assert "RINCON_A" in manager._known

    async def test_room_removed_reports_lost(self) -> None:
        # Realistic partial removal: RINCON_B went offline, but RINCON_A is
        # still around to answer the topology query and no longer lists it.
        manager, found_calls, lost_calls, _renamed_calls, _ = _make_manager()
        manager._known = {
            "RINCON_A": SonosRoom("RINCON_A", "Kitchen", "10.0.1.30", 1400, False, ("Kitchen",)),
            "RINCON_B": SonosRoom("RINCON_B", "Bedroom", "10.0.1.31", 1400, False, ("Bedroom",)),
        }

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(return_value={"RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30")}),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"])
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        assert lost_calls == ["RINCON_B"]
        assert found_calls == []  # RINCON_A was already known, nothing changed about it
        assert manager._known == {
            "RINCON_A": SonosRoom("RINCON_A", "Kitchen", "10.0.1.30", 1400, False, ("Kitchen",))
        }

    async def test_coordinator_ip_change_is_retargeted_not_reset(self) -> None:
        # Same physical coordinator (same uuid), just a new address (e.g.
        # DHCP renewal) — no pairing needed, no reason to drop the Qobuz
        # session either.
        manager, found_calls, lost_calls, _renamed_calls, retargeted_calls = _make_manager()
        manager._known = {
            "RINCON_A": SonosRoom("RINCON_A", "Kitchen", "10.0.1.30", 1400, False, ("Kitchen",))
        }

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(return_value={"RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.99")}),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"])
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        assert lost_calls == []
        assert found_calls == []
        assert len(retargeted_calls) == 1
        new_room = retargeted_calls[0]
        assert new_room.uuid == "RINCON_A"
        assert new_room.ip == "10.0.1.99"
        assert manager._known["RINCON_A"].ip == "10.0.1.99"

    async def test_active_group_gets_comma_joined_display_name(self) -> None:
        manager, found_calls, _, _renamed_calls, _ = _make_manager()

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(
                    return_value={
                        "RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30"),
                        "RINCON_B": _member("RINCON_B", "Living Room", "10.0.1.31"),
                    }
                ),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(
                            coordinator_uuid="RINCON_A", member_uuids=["RINCON_A", "RINCON_B"]
                        )
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        assert len(found_calls) == 1  # only the coordinator becomes a Speaker
        room = found_calls[0]
        assert room.name == "Kitchen"  # coordinator's own room name, unaffected
        assert room.member_names == ("Kitchen", "Living Room")
        assert room.display_name == "Kitchen, Living Room"

    async def test_group_orders_coordinator_first_then_alphabetical(self) -> None:
        # Coordinator is "Office" — must stay first even though it wouldn't
        # sort first alphabetically; the other two must be alphabetized
        # regardless of the order the topology happens to list them in.
        manager, found_calls, _, _renamed_calls, _ = _make_manager()

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(
                    return_value={
                        "RINCON_A": _member("RINCON_A", "Office", "10.0.1.30"),
                        "RINCON_B": _member("RINCON_B", "Living Room", "10.0.1.31"),
                        "RINCON_C": _member("RINCON_C", "Bedroom", "10.0.1.32"),
                    }
                ),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(
                            coordinator_uuid="RINCON_A",
                            member_uuids=["RINCON_A", "RINCON_B", "RINCON_C"],
                        )
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        room = found_calls[0]
        assert room.member_names == ("Office", "Bedroom", "Living Room")
        assert room.display_name == "Office, Bedroom, Living Room"

    async def test_member_leaving_group_renames_coordinator_without_restart(self) -> None:
        # Reproduces the reported bug: 3 rooms grouped, a non-coordinator
        # leaves. The coordinator's ip/port never changed, so it must be
        # renamed in place — never lost+found (which would kill its
        # WebSocket session and lose playback position).
        manager, found_calls, lost_calls, renamed_calls, _ = _make_manager()

        topology_v1 = {
            "RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30"),
            "RINCON_B": _member("RINCON_B", "Living Room", "10.0.1.31"),
            "RINCON_C": _member("RINCON_C", "Bedroom", "10.0.1.32"),
        }
        groups_v1 = [
            SonosGroup(
                coordinator_uuid="RINCON_A", member_uuids=["RINCON_A", "RINCON_B", "RINCON_C"]
            )
        ]
        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=topology_v1)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=groups_v1)),
        ):
            await manager._poll_once()

        assert len(found_calls) == 1  # only the coordinator gets a Speaker
        assert found_calls[0].display_name == "Kitchen, Bedroom, Living Room"
        found_calls.clear()

        # Bedroom leaves: RINCON_A keeps coordinating just RINCON_B, and
        # RINCON_C becomes its own solo (single-member) group.
        topology_v2 = dict(topology_v1)
        groups_v2 = [
            SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A", "RINCON_B"]),
            SonosGroup(coordinator_uuid="RINCON_C", member_uuids=["RINCON_C"]),
        ]
        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=topology_v2)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=groups_v2)),
        ):
            await manager._poll_once()

        assert lost_calls == []  # the coordinator was never torn down
        assert len(renamed_calls) == 1
        assert renamed_calls[0].uuid == "RINCON_A"
        assert renamed_calls[0].display_name == "Kitchen, Living Room"
        assert len(found_calls) == 1  # Bedroom, now solo, gets its own Speaker
        assert found_calls[0].uuid == "RINCON_C"
        assert found_calls[0].display_name == "Bedroom"

    async def test_coordinator_itself_removed_is_retargeted_kitchen_refound_solo(self) -> None:
        # Reproduces the harder reported bug: 3 rooms grouped, the
        # *coordinator* is removed from the group. Sonos promotes a former
        # peer (Living Room) to coordinate what continues playing; Kitchen
        # is left solo. Must not be silently renamed to "Kitchen" alone
        # (that would leave a stale Speaker impersonating a group that
        # moved elsewhere — the split-brain symptom originally reported).
        # Nor must it be a full lost+found: since Living Room shares the
        # continuing group's group_id, the existing Speaker is retargeted
        # to it (keeping the Qobuz session alive) instead — and Kitchen,
        # now solo, gets a fresh Speaker of its own in the same pass.
        manager, found_calls, lost_calls, renamed_calls, retargeted_calls = _make_manager()

        topology_v1 = {
            "RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30"),
            "RINCON_B": _member("RINCON_B", "Living Room", "10.0.1.31"),
            "RINCON_C": _member("RINCON_C", "Bedroom", "10.0.1.32"),
        }
        groups_v1 = [
            SonosGroup(
                coordinator_uuid="RINCON_A",
                member_uuids=["RINCON_A", "RINCON_B", "RINCON_C"],
                group_id="RINCON_A:1",
            )
        ]
        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=topology_v1)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=groups_v1)),
        ):
            await manager._poll_once()

        assert len(found_calls) == 1
        found_calls.clear()

        # Kitchen is removed from the group; Sonos promotes Living Room.
        # Confirmed against a real household: the continuing group keeps
        # the SAME group_id under its new coordinator; the now-solo Kitchen
        # gets a different one (its own, separate, single-member group).
        topology_v2 = dict(topology_v1)
        groups_v2 = [
            SonosGroup(
                coordinator_uuid="RINCON_B",
                member_uuids=["RINCON_B", "RINCON_C"],
                group_id="RINCON_A:1",  # unchanged — same continuing group
            ),
            SonosGroup(
                coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"], group_id="RINCON_A:2"
            ),
        ]
        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=topology_v2)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=groups_v2)),
        ):
            await manager._poll_once()

        assert renamed_calls == []  # never silently relabeled
        assert lost_calls == []  # never torn down — retargeted instead

        assert len(retargeted_calls) == 1
        new_room = retargeted_calls[0]
        assert new_room.uuid == "RINCON_B"  # the continuing group now points at Living Room
        assert new_room.ip == "10.0.1.31"
        assert new_room.group_id == "RINCON_A:1"  # inherits the continuing group's id

        assert len(found_calls) == 1  # Kitchen, now solo, gets its own fresh Speaker
        assert found_calls[0].uuid == "RINCON_A"
        assert found_calls[0].ip == "10.0.1.30"
        assert found_calls[0].group_id == "RINCON_A:2"  # a distinct, new solo group

        # Tracking is by group_id (tracking_key), which never changes across
        # the handoff — no re-keying needed for the continuing group, and
        # Kitchen's fresh solo group gets its own, separate key.
        assert set(manager._known) == {"RINCON_A:1", "RINCON_A:2"}
        assert manager._known["RINCON_A:1"].uuid == "RINCON_B"
        assert manager._known["RINCON_A:2"].uuid == "RINCON_A"

    async def test_last_peer_leaving_a_2room_group_renames_not_resets(self) -> None:
        # Regression: a 2-room group's coordinator, after its *only* peer
        # leaves, ends up solo. The coordinator never changed, so its
        # group_id ("RINCON_A:1") stays attached to it — this must be a
        # plain in-place rename, not a lost+found (which would drop the
        # Qobuz session for no reason). The departed peer, now solo for the
        # first time, gets a brand new group_id of its own.
        manager, found_calls, lost_calls, renamed_calls, _ = _make_manager()

        topology_v1 = {
            "RINCON_A": _member("RINCON_A", "Cuarto", "10.0.1.30"),
            "RINCON_B": _member("RINCON_B", "Cocina", "10.0.1.31"),
        }
        groups_v1 = [
            SonosGroup(
                coordinator_uuid="RINCON_A",
                member_uuids=["RINCON_A", "RINCON_B"],
                group_id="RINCON_A:1",
            )
        ]
        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=topology_v1)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=groups_v1)),
        ):
            await manager._poll_once()

        assert len(found_calls) == 1
        found_calls.clear()

        # Cocina leaves; Cuarto is left solo, still coordinating (itself).
        # Cuarto's group_id is unchanged — same continuing group, just
        # shrunk. Cocina, solo for the first time, gets a fresh group_id.
        topology_v2 = dict(topology_v1)
        groups_v2 = [
            SonosGroup(
                coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"], group_id="RINCON_A:1"
            ),
            SonosGroup(
                coordinator_uuid="RINCON_B", member_uuids=["RINCON_B"], group_id="RINCON_B:1"
            ),
        ]
        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(f"{MODULE}.fetch_sonos_topology", AsyncMock(return_value=topology_v2)),
            patch(f"{MODULE}.fetch_sonos_groups", AsyncMock(return_value=groups_v2)),
        ):
            await manager._poll_once()

        assert lost_calls == []  # Cuarto's Speaker was never torn down
        assert len(renamed_calls) == 1
        assert renamed_calls[0].uuid == "RINCON_A"
        assert renamed_calls[0].display_name == "Cuarto"
        assert len(found_calls) == 1  # Cocina, now solo, gets its own new Speaker
        assert found_calls[0].uuid == "RINCON_B"
        assert found_calls[0].display_name == "Cocina"

    async def test_stereo_pair_solo_display_name_has_no_duplicate(self) -> None:
        # A bonded pair's secondary is Invisible but still listed in
        # g.member_uuids — it must not turn "Kitchen" into "Kitchen, Kitchen".
        manager, found_calls, _, _renamed_calls, _ = _make_manager()

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(
                    return_value={
                        "RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30"),
                        "RINCON_A2": _member("RINCON_A2", "Kitchen", "10.0.1.33", invisible=True),
                    }
                ),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(
                            coordinator_uuid="RINCON_A", member_uuids=["RINCON_A", "RINCON_A2"]
                        )
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        assert found_calls[0].display_name == "Kitchen"

    async def test_invisible_coordinator_is_skipped(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls, _ = _make_manager()

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(
                    return_value={
                        "RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30", invisible=True)
                    }
                ),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"])
                    ]
                ),
            ),
        ):
            await manager._poll_once()

        assert found_calls == []
        assert lost_calls == []


class TestStartStop:
    async def test_start_polls_once_immediately(self) -> None:
        manager, found_calls, _, _renamed_calls, _ = _make_manager()

        with (
            patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[SONOS_DEVICE])),
            patch(
                f"{MODULE}.fetch_sonos_topology",
                AsyncMock(return_value={"RINCON_A": _member("RINCON_A", "Kitchen", "10.0.1.30")}),
            ),
            patch(
                f"{MODULE}.fetch_sonos_groups",
                AsyncMock(
                    return_value=[
                        SonosGroup(coordinator_uuid="RINCON_A", member_uuids=["RINCON_A"])
                    ]
                ),
            ),
        ):
            await manager.start()
            try:
                assert len(found_calls) == 1  # no need to wait for the poll interval
            finally:
                await manager.stop()

    async def test_stop_cancels_background_loop(self) -> None:
        manager, _, _, _renamed_calls, _ = _make_manager()

        with patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[])):
            await manager.start()
        assert manager._task is not None and not manager._task.done()

        await manager.stop()
        assert manager._task is None


def _notify_body(zone_group_state_xml: str) -> str:
    return (
        '<?xml version="1.0"?>'
        '<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0">'
        "<e:property>"
        f"<ZoneGroupState>{escape(zone_group_state_xml)}</ZoneGroupState>"
        "</e:property>"
        "</e:propertyset>"
    )


KITCHEN_ZONE_GROUP_STATE = (
    "<ZoneGroups>"
    '<ZoneGroup Coordinator="RINCON_A" ID="RINCON_A:1">'
    '<ZoneGroupMember UUID="RINCON_A" '
    'Location="http://10.0.1.30:1400/xml/device_description.xml" ZoneName="Kitchen"/>'
    "</ZoneGroup>"
    "</ZoneGroups>"
)


class TestEventSubscriptionWiring:
    """SonosDiscoveryManager attaches to an already-registered
    SonosEventSubscriber rather than registering its own route — routes
    can't be added once aiohttp freezes the router, which happens well
    before a manager exists (only created after login)."""

    def test_no_subscriber_without_one_given(self) -> None:
        manager, _, _, _, _ = _make_manager()

        assert manager._subscriber is None

    def test_attaches_to_a_pre_registered_subscriber(self) -> None:
        app = web.Application()
        subscriber = SonosEventSubscriber()
        subscriber.register_route(app)  # as app.py does, before serving starts

        async def on_found(room: SonosRoom) -> bool:
            return True

        async def on_lost(uuid: str) -> None:
            pass

        async def on_renamed(room: SonosRoom) -> bool:
            return True

        async def on_retargeted(room: SonosRoom) -> bool:
            return True

        manager = SonosDiscoveryManager(
            on_room_found=on_found,
            on_room_lost=on_lost,
            on_room_renamed=on_renamed,
            on_room_retargeted=on_retargeted,
            event_subscriber=subscriber,
            http_port=8689,
        )

        assert manager._subscriber is subscriber
        assert subscriber.on_notify == manager._handle_notify
        assert manager._callback_url is not None
        assert manager._callback_url.endswith(subscriber.callback_path)
        # The NOTIFY route really was registered on the given app.
        assert any(
            route.method == "NOTIFY" for resource in app.router.resources() for route in resource
        )

    def test_no_subscriber_when_local_ip_cannot_be_determined(self) -> None:
        subscriber = SonosEventSubscriber()

        async def on_found(room: SonosRoom) -> bool:
            return True

        async def on_lost(uuid: str) -> None:
            pass

        async def on_renamed(room: SonosRoom) -> bool:
            return True

        async def on_retargeted(room: SonosRoom) -> bool:
            return True

        with patch(f"{MODULE}.get_local_ip", return_value=None):
            manager = SonosDiscoveryManager(
                on_room_found=on_found,
                on_room_lost=on_lost,
                on_room_renamed=on_renamed,
                on_room_retargeted=on_retargeted,
                event_subscriber=subscriber,
                http_port=8689,
            )

        assert manager._subscriber is None
        assert subscriber.on_notify is None  # never claimed

    async def test_stop_releases_the_shared_subscriber(self) -> None:
        app = web.Application()
        subscriber = SonosEventSubscriber()
        subscriber.register_route(app)
        manager, _, _, _, _ = _make_manager()
        manager._subscriber = subscriber  # simulate a successful attach
        subscriber.on_notify = manager._handle_notify

        with patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[])):
            await manager.start()
        await manager.stop()

        assert subscriber.on_notify is None


class TestHandleNotify:
    async def test_notify_body_is_parsed_and_applied(self) -> None:
        manager, found_calls, _, _, _ = _make_manager()

        await manager._handle_notify(_notify_body(KITCHEN_ZONE_GROUP_STATE))

        assert len(found_calls) == 1
        assert found_calls[0].name == "Kitchen"
        assert found_calls[0].group_id == "RINCON_A:1"
        assert "RINCON_A:1" in manager._known

    async def test_unparseable_notify_body_is_ignored(self) -> None:
        manager, found_calls, lost_calls, _, _ = _make_manager()

        await manager._handle_notify("not xml at all")

        assert found_calls == []
        assert lost_calls == []


class TestTickCadence:
    async def test_polls_at_normal_interval_without_healthy_subscription(self) -> None:
        manager, _, _, _, _ = _make_manager()
        manager._poll_once = AsyncMock()
        manager._last_poll_at = 0.0

        with patch(f"{MODULE}.time") as mock_time:
            mock_time.monotonic.return_value = manager._poll_interval + 1
            await manager._tick()

        manager._poll_once.assert_awaited_once()

    async def test_safety_net_interval_used_once_subscription_is_healthy(self) -> None:
        manager, _, _, _, _ = _make_manager()
        manager._poll_once = AsyncMock()
        manager._try_subscribe = AsyncMock()
        manager._subscriber = MagicMock()
        healthy_sub = MagicMock()
        healthy_sub.needs_renewal = False
        manager._subscriber.subscription = healthy_sub

        # Short-interval elapsed (normal poll_interval would fire), but the
        # much longer safety-net interval has not — must NOT poll.
        manager._last_poll_at = 0.0
        with patch(f"{MODULE}.time") as mock_time:
            mock_time.monotonic.return_value = manager._poll_interval + 1
            await manager._tick()
        manager._poll_once.assert_not_called()

        # Now the safety-net interval has elapsed too — must poll.
        with patch(f"{MODULE}.time") as mock_time:
            mock_time.monotonic.return_value = SAFETY_NET_POLL_INTERVAL_SECONDS + 1
            await manager._tick()
        manager._poll_once.assert_awaited_once()

    async def test_tick_attempts_resubscribe_when_no_subscription(self) -> None:
        manager, _, _, _, _ = _make_manager()
        manager._poll_once = AsyncMock()
        manager._try_subscribe = AsyncMock()
        manager._subscriber = MagicMock()
        manager._subscriber.subscription = None
        manager._last_poll_at = 0.0

        with patch(f"{MODULE}.time") as mock_time:
            mock_time.monotonic.return_value = 0.0
            await manager._tick()

        manager._try_subscribe.assert_awaited_once()

    async def test_tick_renews_subscription_needing_renewal(self) -> None:
        manager, _, _, _, _ = _make_manager()
        manager._poll_once = AsyncMock()
        manager._subscriber = MagicMock()
        stale_sub = MagicMock()
        stale_sub.needs_renewal = True
        manager._subscriber.subscription = stale_sub
        manager._subscriber.renew = AsyncMock(return_value=True)
        manager._last_poll_at = 0.0

        with patch(f"{MODULE}.time") as mock_time:
            mock_time.monotonic.return_value = 0.0
            await manager._tick()

        manager._subscriber.renew.assert_awaited_once()

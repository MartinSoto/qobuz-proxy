"""Tests for continuous Sonos household discovery (poll/diff/retry logic)."""

from unittest.mock import AsyncMock, patch

from qobuz_proxy.backends.dlna.discovery import DiscoveredDevice
from qobuz_proxy.backends.dlna.sonos_discovery_manager import SonosDiscoveryManager, SonosRoom
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
    on_found=None, on_lost=None, on_renamed=None
) -> tuple[SonosDiscoveryManager, list, list, list]:
    found_calls: list = []
    lost_calls: list = []
    renamed_calls: list = []

    async def default_on_found(room: SonosRoom) -> bool:
        found_calls.append(room)
        return True

    async def default_on_lost(uuid: str) -> None:
        lost_calls.append(uuid)

    async def default_on_renamed(room: SonosRoom) -> bool:
        renamed_calls.append(room)
        return True

    manager = SonosDiscoveryManager(
        on_room_found=on_found or default_on_found,
        on_room_lost=on_lost or default_on_lost,
        on_room_renamed=on_renamed or default_on_renamed,
    )
    return manager, found_calls, lost_calls, renamed_calls


class TestPollOnce:
    async def test_new_room_reported_found(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls = _make_manager()

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
        manager, found_calls, lost_calls, _renamed_calls = _make_manager()
        manager._known = {
            "RINCON_A": SonosRoom("RINCON_A", "Kitchen", "10.0.1.30", 1400, False, ("Kitchen",))
        }

        with patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[])):
            await manager._poll_once()

        assert found_calls == []
        assert lost_calls == []
        assert "RINCON_A" in manager._known  # untouched, not torn down

    async def test_topology_unavailable_keeps_previous_state(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls = _make_manager()
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

        manager, _, _, _renamed_calls = _make_manager(on_found=flaky_on_found)

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
        manager, found_calls, lost_calls, _renamed_calls = _make_manager()
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

    async def test_coordinator_ip_change_reports_lost_then_found(self) -> None:
        manager, found_calls, lost_calls, _renamed_calls = _make_manager()
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

        assert lost_calls == ["RINCON_A"]
        assert len(found_calls) == 1
        assert found_calls[0].ip == "10.0.1.99"
        assert manager._known["RINCON_A"].ip == "10.0.1.99"

    async def test_active_group_gets_comma_joined_display_name(self) -> None:
        manager, found_calls, _, _renamed_calls = _make_manager()

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
        manager, found_calls, _, _renamed_calls = _make_manager()

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
        manager, found_calls, lost_calls, renamed_calls = _make_manager()

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

    async def test_coordinator_itself_removed_resets_rather_than_renames(self) -> None:
        # Reproduces the harder reported bug: 3 rooms grouped, the
        # *coordinator* is removed from the group. Sonos promotes a former
        # peer (Living Room) to coordinate what continues playing; Kitchen
        # is left solo. Same ip/port as before, so this must NOT be
        # silently renamed to "Kitchen" alone — that would leave a stale
        # Speaker impersonating a group that has actually moved elsewhere
        # (the exact split-brain symptom reported: Qobuz stays bound to
        # Kitchen while Sonos audio has moved to Living Room+Bedroom).
        manager, found_calls, lost_calls, renamed_calls = _make_manager()

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
        assert lost_calls == ["RINCON_A"]  # demoted coordinator resets
        uuids_found = {r.uuid for r in found_calls}
        assert uuids_found == {"RINCON_A", "RINCON_B"}  # Kitchen re-found solo; Living Room new

        living_room = next(r for r in found_calls if r.uuid == "RINCON_B")
        kitchen_solo = next(r for r in found_calls if r.uuid == "RINCON_A")
        assert living_room.group_id == "RINCON_A:1"  # inherits the continuing group's id
        assert kitchen_solo.group_id == "RINCON_A:2"  # a distinct, new solo group

    async def test_stereo_pair_solo_display_name_has_no_duplicate(self) -> None:
        # A bonded pair's secondary is Invisible but still listed in
        # g.member_uuids — it must not turn "Kitchen" into "Kitchen, Kitchen".
        manager, found_calls, _, _renamed_calls = _make_manager()

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
        manager, found_calls, lost_calls, _renamed_calls = _make_manager()

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
        manager, found_calls, _, _renamed_calls = _make_manager()

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
        manager, _, _, _renamed_calls = _make_manager()

        with patch(f"{MODULE}.discover_dlna_devices", AsyncMock(return_value=[])):
            await manager.start()
        assert manager._task is not None and not manager._task.done()

        await manager.stop()
        assert manager._task is None

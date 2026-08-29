"""Tests for SonosController — turns SonosDiscoveryManager's Sonos-topology
callbacks into actual Speaker/Qobuz Connect session lifecycle actions."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from qobuz_proxy.backends.dlna.sonos.controller import SonosController
from qobuz_proxy.backends.dlna.sonos.discovery_manager import DepartedMember, SonosRoom


def _make_controller() -> SonosController:
    return SonosController(
        api_client=MagicMock(),
        app_id="test-app-id",
        webui_http_port=8689,
        event_subscriber=MagicMock(),
        stream_resolver=MagicMock(),
    )


def _mock_speaker(name: str = "Kitchen", starts: bool = True) -> MagicMock:
    speaker = MagicMock()
    speaker.start = AsyncMock(return_value=starts)
    speaker.stop = AsyncMock()
    speaker.detach = AsyncMock()
    speaker.name = name
    speaker.is_active = False  # explicit default — a MagicMock attribute is truthy otherwise
    return speaker


ROOM = SonosRoom(uuid="RINCON_A", name="Kitchen", ip="10.0.1.30", port=1400, is_stereo_pair=False)


class TestRoomFoundAndLost:
    async def test_room_found_starts_and_registers_speaker(self) -> None:
        controller = _make_controller()

        speaker = _mock_speaker("Kitchen")
        with patch("qobuz_proxy.speaker.Speaker", return_value=speaker) as MockSpeaker:
            result = await controller._on_room_found(ROOM)

        assert result is True
        assert controller.speakers == [speaker]
        assert controller._speakers_by_group_id["RINCON_A"] is speaker

        sc = MockSpeaker.call_args.kwargs["config"]
        assert sc.name == "Kitchen"
        assert sc.dlna_ip == "10.0.1.30"
        assert sc.dlna_port == 1400
        assert sc.auto_managed is True
        assert sc.uuid  # deterministic uuid was assigned

    async def test_room_found_derives_identity_from_group_id_not_coordinator_uuid(self) -> None:
        # A coordinator handoff: two different physical rooms (different
        # `uuid`/`ip`), same continuing group (same `group_id`) — the
        # promoted coordinator must compute the SAME Qobuz Connect identity
        # its predecessor had, so the app sees a reconnect of a device it
        # already knows rather than a stranger.
        from qobuz_proxy.config import generate_sonos_speaker_uuid

        controller = _make_controller()

        old_coordinator = SonosRoom(
            uuid="RINCON_KITCHEN",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
            group_id="RINCON_KITCHEN:1",
        )
        new_coordinator = SonosRoom(
            uuid="RINCON_LIVINGROOM",
            name="Living Room",
            ip="10.0.1.31",
            port=1400,
            is_stereo_pair=False,
            member_names=("Living Room",),
            group_id="RINCON_KITCHEN:1",  # same continuing group
        )

        with patch("qobuz_proxy.speaker.Speaker", return_value=_mock_speaker()) as MockSpeaker:
            await controller._on_room_found(old_coordinator)
            old_sc = MockSpeaker.call_args.kwargs["config"]

        assert old_sc.uuid == generate_sonos_speaker_uuid("RINCON_KITCHEN:1")

        # Simulate the handoff: the old Speaker is gone, a fresh one is
        # created for the promoted coordinator (reset here to isolate this
        # from the name-collision guard, unrelated to identity).
        controller._speakers_by_group_id = {}

        with patch("qobuz_proxy.speaker.Speaker", return_value=_mock_speaker()) as MockSpeaker:
            await controller._on_room_found(new_coordinator)
            new_sc = MockSpeaker.call_args.kwargs["config"]

        assert new_sc.uuid == old_sc.uuid  # same identity, inherited via group_id

    async def test_room_found_different_group_id_gets_different_identity(self) -> None:
        from qobuz_proxy.config import generate_sonos_speaker_uuid

        controller = _make_controller()

        kitchen_solo = SonosRoom(
            uuid="RINCON_KITCHEN",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen",),
            group_id="RINCON_KITCHEN:2",  # a distinct, new solo group
        )

        with patch("qobuz_proxy.speaker.Speaker", return_value=_mock_speaker()) as MockSpeaker:
            await controller._on_room_found(kitchen_solo)
            sc = MockSpeaker.call_args.kwargs["config"]

        assert sc.uuid == generate_sonos_speaker_uuid("RINCON_KITCHEN:2")
        assert sc.uuid != generate_sonos_speaker_uuid("RINCON_KITCHEN:1")

    async def test_room_found_returns_false_on_name_collision(self) -> None:
        controller = _make_controller()
        existing = _mock_speaker("Kitchen")
        controller._speakers_by_group_id["existing-key"] = existing

        with patch("qobuz_proxy.speaker.Speaker") as MockSpeaker:
            result = await controller._on_room_found(ROOM)

        assert result is False
        MockSpeaker.assert_not_called()
        assert controller.speakers == [existing]  # unchanged

    async def test_room_found_returns_false_when_speaker_fails_to_start(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen", starts=False)

        with patch("qobuz_proxy.speaker.Speaker", return_value=speaker):
            result = await controller._on_room_found(ROOM)

        assert result is False
        assert controller.speakers == []
        assert "RINCON_A" not in controller._speakers_by_group_id

    async def test_room_lost_stops_and_removes_speaker(self) -> None:
        # Never sends a live device stop here — see _on_room_pending: if
        # this was the active speaker, it was already stopped the moment
        # its group_id first went pending, well before a real loss is ever
        # confirmed (see SonosDiscoveryManager's pending state).
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        controller._speakers_by_group_id["RINCON_A"] = speaker

        await controller._on_room_lost("RINCON_A")

        speaker.stop.assert_awaited_once_with(send_device_stop=False)
        assert controller.speakers == []
        assert "RINCON_A" not in controller._speakers_by_group_id

    async def test_room_lost_is_a_noop_for_unknown_uuid(self) -> None:
        controller = _make_controller()

        await controller._on_room_lost("RINCON_UNKNOWN")  # must not raise


class TestConcurrentRoomFound:
    """SonosDiscoveryManager runs _on_room_found concurrently for multiple
    rooms discovered in the same batch (startup with several rooms, or
    several changes landing in one topology diff) — see _apply_topology's
    use of asyncio.gather. _on_room_found must register (name + assigned
    ports) before the slow await speaker.start(), or two concurrent calls
    both compute against the same stale snapshot and collide."""

    async def test_concurrent_finds_get_distinct_ports_and_names(self) -> None:
        controller = _make_controller()

        def make_speaker(*, config, **_kwargs) -> MagicMock:
            speaker = _mock_speaker(config.name)
            speaker._config = config

            async def slow_start() -> bool:
                # Yield so the other concurrently-scheduled call's own
                # name/port-assignment prefix runs before this one returns —
                # reproduces the interleaving a real, slower speaker.start()
                # (DLNA connect, mDNS registration) would cause.
                await asyncio.sleep(0)
                return True

            speaker.start = AsyncMock(side_effect=slow_start)
            return speaker

        room_a = SonosRoom(
            uuid="RINCON_A", name="Kitchen", ip="10.0.1.30", port=1400, is_stereo_pair=False
        )
        room_b = SonosRoom(
            uuid="RINCON_B", name="Bedroom", ip="10.0.1.31", port=1400, is_stereo_pair=False
        )

        with patch("qobuz_proxy.speaker.Speaker", side_effect=make_speaker):
            results = await asyncio.gather(
                controller._on_room_found(room_a),
                controller._on_room_found(room_b),
            )

        assert results == [True, True]
        assert len(controller.speakers) == 2
        ports = {s._config.http_port for s in controller.speakers}
        assert len(ports) == 2, f"expected distinct ports, got {ports}"
        names = {s._config.name for s in controller.speakers}
        assert names == {"Kitchen", "Bedroom"}

    async def test_a_failed_concurrent_start_rolls_back_its_reservation(self) -> None:
        controller = _make_controller()

        def make_speaker(*, config, **_kwargs) -> MagicMock:
            speaker = _mock_speaker(config.name)
            speaker._config = config
            fails = config.name == "Bedroom"

            async def slow_start() -> bool:
                await asyncio.sleep(0)
                return not fails

            speaker.start = AsyncMock(side_effect=slow_start)
            return speaker

        room_a = SonosRoom(
            uuid="RINCON_A", name="Kitchen", ip="10.0.1.30", port=1400, is_stereo_pair=False
        )
        room_b = SonosRoom(
            uuid="RINCON_B", name="Bedroom", ip="10.0.1.31", port=1400, is_stereo_pair=False
        )

        with patch("qobuz_proxy.speaker.Speaker", side_effect=make_speaker):
            results = await asyncio.gather(
                controller._on_room_found(room_a),
                controller._on_room_found(room_b),
            )

        assert results == [True, False]
        assert [s._config.name for s in controller.speakers] == ["Kitchen"]
        assert "RINCON_B" not in controller._speakers_by_group_id


class TestRoomRenamed:
    async def test_renames_running_speaker_in_place(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.rename = AsyncMock(return_value=True)
        controller._speakers_by_group_id["RINCON_A"] = speaker

        grouped_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
        )
        result = await controller._on_room_renamed(grouped_room)

        assert result is True
        speaker.rename.assert_awaited_once_with("Kitchen, Living Room")

    async def test_returns_false_when_speaker_not_running(self) -> None:
        controller = _make_controller()

        result = await controller._on_room_renamed(ROOM)

        assert result is False

    async def test_returns_false_on_name_collision(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.rename = AsyncMock(return_value=True)
        controller._speakers_by_group_id["RINCON_A"] = speaker

        other = _mock_speaker("Kitchen, Living Room")
        controller._speakers_by_group_id["RINCON_OTHER"] = other

        grouped_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
        )
        result = await controller._on_room_renamed(grouped_room)

        assert result is False
        speaker.rename.assert_not_called()


class TestRoomRetargeted:
    async def test_retargets_running_speaker_in_place(self) -> None:
        # A genuine handoff: the Speaker keeps its identity — tracked by the
        # group's stable tracking_key (group_id) — even though the physical
        # coordinator moved to Living Room's ip/port. No re-keying needed.
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=True)
        speaker.rename = AsyncMock(return_value=True)
        controller._speakers_by_group_id["RINCON_A:1"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_B",
            name="Living Room",
            ip="10.0.1.31",
            port=1400,
            is_stereo_pair=False,
            member_names=("Living Room", "Bedroom"),
            group_id="RINCON_A:1",  # same continuing group
        )
        result = await controller._on_room_retargeted(new_room)

        assert result is True
        speaker.retarget.assert_awaited_once_with("10.0.1.31", 1400)
        speaker.rename.assert_awaited_once_with("Living Room, Bedroom")
        assert controller._speakers_by_group_id["RINCON_A:1"] is speaker  # key unchanged

    async def test_ip_only_change_retargets_without_rename(self) -> None:
        # A plain IP change (e.g. DHCP) for the same coordinator — no
        # rename needed, display name is unchanged.
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=True)
        speaker.rename = AsyncMock(return_value=True)
        controller._speakers_by_group_id["RINCON_A"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.99",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen",),
        )
        result = await controller._on_room_retargeted(new_room)

        assert result is True
        speaker.retarget.assert_awaited_once_with("10.0.1.99", 1400)
        speaker.rename.assert_not_called()  # display name unchanged
        assert controller._speakers_by_group_id["RINCON_A"] is speaker

    async def test_returns_false_when_speaker_not_running(self) -> None:
        controller = _make_controller()

        result = await controller._on_room_retargeted(ROOM)

        assert result is False

    async def test_returns_false_when_backend_retarget_fails(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=False)
        controller._speakers_by_group_id["RINCON_A:1"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_B",
            name="Living Room",
            ip="10.0.1.31",
            port=1400,
            is_stereo_pair=False,
            group_id="RINCON_A:1",
        )
        result = await controller._on_room_retargeted(new_room)

        assert result is False
        assert controller._speakers_by_group_id["RINCON_A:1"] is speaker  # unchanged


class TestRoomPending:
    """A group_id going pending only matters for the speaker Qobuz is
    actually driving — see the module docstring's split-brain note. A
    merely-discovered, idle room is left running untouched either way."""

    async def test_detaches_the_active_speaker(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        await controller._on_room_pending("RINCON_A")

        speaker.detach.assert_awaited_once()
        # Not torn down — still tracked, just no longer driving a target.
        assert controller._speakers_by_group_id["RINCON_A"] is speaker

    async def test_leaves_an_idle_speaker_untouched(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = False
        controller._speakers_by_group_id["RINCON_A"] = speaker

        await controller._on_room_pending("RINCON_A")

        speaker.detach.assert_not_called()

    async def test_is_a_noop_for_unknown_tracking_key(self) -> None:
        controller = _make_controller()

        await controller._on_room_pending("RINCON_UNKNOWN")  # must not raise


class TestRoomMembersDeparted:
    """The central rule from live testing: a device leaving the group we're
    actively playing to must be stopped directly (nothing else will ever
    tell it to); a device leaving any other, merely-discovered group must
    never be touched — see Speaker.is_active."""

    def _mock_dlna_client(self):  # type: ignore[no-untyped-def]
        client = MagicMock()
        client.connect = AsyncMock()
        client.stop = AsyncMock()
        client.get_transport_info = AsyncMock(return_value="STOPPED")
        client.disconnect = AsyncMock()
        return client

    async def test_stops_a_device_leaving_the_active_group(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        client = self._mock_dlna_client()
        with patch(
            "qobuz_proxy.backends.dlna.sonos.controller.DLNAClient", return_value=client
        ) as MockClient:
            await controller._on_room_members_departed(
                "RINCON_A", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )

        MockClient.assert_called_once_with("10.0.1.31", 1400)
        client.connect.assert_awaited_once()
        client.stop.assert_awaited_once()
        client.disconnect.assert_awaited_once()

    async def test_stops_every_departed_device(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        with patch(
            "qobuz_proxy.backends.dlna.sonos.controller.DLNAClient",
            return_value=self._mock_dlna_client(),
        ) as Mock:
            await controller._on_room_members_departed(
                "RINCON_A",
                (
                    DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),
                    DepartedMember(uuid="RINCON_C", ip="10.0.1.32", port=1400),
                ),
            )

        assert Mock.call_count == 2

    async def test_ignores_departure_from_a_group_that_is_not_active(self) -> None:
        # Central rule: never touch a device leaving a group we're not
        # playing to — Sonos is handling its own regrouping.
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = False
        controller._speakers_by_group_id["RINCON_A"] = speaker

        with patch("qobuz_proxy.backends.dlna.sonos.controller.DLNAClient") as MockClient:
            await controller._on_room_members_departed(
                "RINCON_A", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )

        MockClient.assert_not_called()

    async def test_ignores_departure_for_unknown_tracking_key(self) -> None:
        controller = _make_controller()

        with patch("qobuz_proxy.backends.dlna.sonos.controller.DLNAClient") as MockClient:
            await controller._on_room_members_departed(
                "RINCON_UNKNOWN", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )

        MockClient.assert_not_called()

    async def test_skips_a_departed_member_still_present_elsewhere(self) -> None:
        # A device absorbed into another (legitimate) group in the same
        # snapshot wasn't abandoned — touching it risks interrupting
        # playback Sonos itself just set up. Same reasoning _on_room_lost's
        # own still_present check already uses at the group level.
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        with patch("qobuz_proxy.backends.dlna.sonos.controller.DLNAClient") as MockClient:
            await controller._on_room_members_departed(
                "RINCON_A",
                (
                    DepartedMember(
                        uuid="RINCON_B", ip="10.0.1.31", port=1400, still_present_elsewhere=True
                    ),
                ),
            )

        MockClient.assert_not_called()

    async def test_stops_only_the_genuinely_departed_member_in_a_mixed_batch(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        with patch(
            "qobuz_proxy.backends.dlna.sonos.controller.DLNAClient",
            return_value=self._mock_dlna_client(),
        ) as MockClient:
            await controller._on_room_members_departed(
                "RINCON_A",
                (
                    DepartedMember(
                        uuid="RINCON_B", ip="10.0.1.31", port=1400, still_present_elsewhere=True
                    ),
                    DepartedMember(uuid="RINCON_C", ip="10.0.1.32", port=1400),
                ),
            )

        MockClient.assert_called_once_with("10.0.1.32", 1400)

    async def test_a_failed_stop_does_not_raise(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        client = self._mock_dlna_client()
        client.connect.side_effect = OSError("unreachable")
        with patch("qobuz_proxy.backends.dlna.sonos.controller.DLNAClient", return_value=client):
            await controller._on_room_members_departed(
                "RINCON_A", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )  # must not raise

        client.disconnect.assert_awaited_once()  # still cleaned up


class TestStop:
    async def test_stop_stops_discovery_manager(self) -> None:
        controller = _make_controller()
        mock_manager = MagicMock()
        mock_manager.stop = AsyncMock()
        controller._discovery = mock_manager
        controller._speakers_by_group_id["RINCON_A"] = _mock_speaker()

        await controller.stop()

        mock_manager.stop.assert_awaited_once()
        assert controller._discovery is None
        assert controller._speakers_by_group_id == {}

    async def test_active_speaker_gets_a_device_stop(self) -> None:
        controller = _make_controller()
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        controller._speakers_by_group_id["RINCON_A"] = speaker

        await controller.stop()

        speaker.stop.assert_awaited_once_with(send_device_stop=True)

    async def test_idle_speaker_is_not_sent_a_device_stop(self) -> None:
        # Shutting down (or logging out) must not interrupt a merely
        # discovered, idle Sonos room nobody is actually playing to.
        controller = _make_controller()
        speaker = _mock_speaker("Living Room")
        speaker.is_active = False
        controller._speakers_by_group_id["RINCON_A"] = speaker

        await controller.stop()

        speaker.stop.assert_awaited_once_with(send_device_stop=False)

    async def test_mixed_active_and_idle_speakers_each_get_the_right_treatment(self) -> None:
        controller = _make_controller()
        active_speaker = _mock_speaker("Kitchen")
        active_speaker.is_active = True
        idle_speaker = _mock_speaker("Living Room")
        idle_speaker.is_active = False
        controller._speakers_by_group_id["RINCON_A"] = active_speaker
        controller._speakers_by_group_id["RINCON_B"] = idle_speaker

        await controller.stop()

        active_speaker.stop.assert_awaited_once_with(send_device_stop=True)
        idle_speaker.stop.assert_awaited_once_with(send_device_stop=False)

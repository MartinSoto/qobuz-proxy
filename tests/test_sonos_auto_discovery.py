"""Tests for wiring SonosDiscoveryManager into QobuzProxy's speaker lifecycle."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from qobuz_proxy.app import QobuzProxy
from qobuz_proxy.backends.dlna.sonos_discovery_manager import DepartedMember, SonosRoom
from qobuz_proxy.config import Config, QobuzConfig, SpeakerConfig


def _make_config(sonos_auto_discover: bool = True, speakers: list | None = None) -> Config:
    config = Config()
    config.qobuz = QobuzConfig(email="test@example.com", auth_token="secret", user_id="12345")
    config.sonos_auto_discover = sonos_auto_discover
    config.speakers = speakers or []
    return config


def _mock_speaker(name: str = "Kitchen", starts: bool = True) -> MagicMock:
    speaker = MagicMock()
    speaker.start = AsyncMock(return_value=starts)
    speaker.stop = AsyncMock()
    speaker.name = name
    speaker.is_active = False  # explicit default — a MagicMock attribute is truthy otherwise
    return speaker


ROOM = SonosRoom(uuid="RINCON_A", name="Kitchen", ip="10.0.1.30", port=1400, is_stereo_pair=False)


class TestStartSpeakersBranching:
    async def test_auto_discover_starts_discovery_manager_not_static_speakers(self) -> None:
        config = _make_config(sonos_auto_discover=True, speakers=[SpeakerConfig(name="Manual")])
        app = QobuzProxy(config)
        app._api_client = MagicMock()

        mock_manager = MagicMock()
        mock_manager.start = AsyncMock()

        with (
            patch("qobuz_proxy.app.Speaker") as MockSpeaker,
            patch("qobuz_proxy.app.SonosDiscoveryManager", return_value=mock_manager) as MockMgr,
        ):
            await app._start_speakers()

        MockMgr.assert_called_once()
        mock_manager.start.assert_awaited_once()
        MockSpeaker.assert_not_called()  # the static 'Manual' entry is ignored
        assert app._sonos_discovery is mock_manager

    async def test_disabled_uses_static_speakers_as_before(self) -> None:
        config = _make_config(sonos_auto_discover=False, speakers=[SpeakerConfig(name="Manual")])
        app = QobuzProxy(config)
        app._api_client = MagicMock()

        speaker = _mock_speaker("Manual")
        with (
            patch("qobuz_proxy.app.Speaker", return_value=speaker),
            patch("qobuz_proxy.app.SonosDiscoveryManager") as MockMgr,
        ):
            await app._start_speakers()

        MockMgr.assert_not_called()
        speaker.start.assert_awaited_once()
        assert app._speakers == [speaker]


class TestRoomFoundAndLost:
    async def test_room_found_starts_and_registers_speaker(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()

        speaker = _mock_speaker("Kitchen")
        with patch("qobuz_proxy.app.Speaker", return_value=speaker) as MockSpeaker:
            result = await app._on_sonos_room_found(ROOM)

        assert result is True
        assert app._speakers == [speaker]
        assert app._sonos_speakers_by_group_id["RINCON_A"] is speaker

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

        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()

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

        with patch("qobuz_proxy.app.Speaker", return_value=_mock_speaker()) as MockSpeaker:
            await app._on_sonos_room_found(old_coordinator)
            old_sc = MockSpeaker.call_args.kwargs["config"]

        assert old_sc.uuid == generate_sonos_speaker_uuid("RINCON_KITCHEN:1")

        # Simulate the handoff: the old Speaker is gone, a fresh one is
        # created for the promoted coordinator (app._speakers reset here to
        # isolate this from the name-collision guard, unrelated to identity).
        app._speakers = []
        app._sonos_speakers_by_group_id = {}

        with patch("qobuz_proxy.app.Speaker", return_value=_mock_speaker()) as MockSpeaker:
            await app._on_sonos_room_found(new_coordinator)
            new_sc = MockSpeaker.call_args.kwargs["config"]

        assert new_sc.uuid == old_sc.uuid  # same identity, inherited via group_id

    async def test_room_found_different_group_id_gets_different_identity(self) -> None:
        from qobuz_proxy.config import generate_sonos_speaker_uuid

        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()

        kitchen_solo = SonosRoom(
            uuid="RINCON_KITCHEN",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen",),
            group_id="RINCON_KITCHEN:2",  # a distinct, new solo group
        )

        with patch("qobuz_proxy.app.Speaker", return_value=_mock_speaker()) as MockSpeaker:
            await app._on_sonos_room_found(kitchen_solo)
            sc = MockSpeaker.call_args.kwargs["config"]

        assert sc.uuid == generate_sonos_speaker_uuid("RINCON_KITCHEN:2")
        assert sc.uuid != generate_sonos_speaker_uuid("RINCON_KITCHEN:1")

    async def test_room_found_returns_false_when_not_authenticated(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = None

        result = await app._on_sonos_room_found(ROOM)

        assert result is False
        assert app._speakers == []

    async def test_room_found_returns_false_on_name_collision(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()
        existing = _mock_speaker("Kitchen")
        app._speakers.append(existing)

        with patch("qobuz_proxy.app.Speaker") as MockSpeaker:
            result = await app._on_sonos_room_found(ROOM)

        assert result is False
        MockSpeaker.assert_not_called()
        assert app._speakers == [existing]  # unchanged

    async def test_room_found_returns_false_when_speaker_fails_to_start(self) -> None:
        app = QobuzProxy(_make_config())
        app._api_client = MagicMock()
        speaker = _mock_speaker("Kitchen", starts=False)

        with patch("qobuz_proxy.app.Speaker", return_value=speaker):
            result = await app._on_sonos_room_found(ROOM)

        assert result is False
        assert app._speakers == []
        assert "RINCON_A" not in app._sonos_speakers_by_group_id

    async def test_room_lost_offline_stops_and_removes_speaker(self) -> None:
        # The device genuinely went offline — tell it to stop too.
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        await app._on_sonos_room_lost("RINCON_A", still_present=False)

        speaker.stop.assert_awaited_once_with(send_device_stop=True)
        assert app._speakers == []
        assert "RINCON_A" not in app._sonos_speakers_by_group_id

    async def test_room_lost_absorbed_into_another_group_does_not_stop_the_device(
        self,
    ) -> None:
        # Still present in the topology — just no longer a coordinator,
        # because it joined another group as a plain member. Sonos is
        # already directing its audio; sending it a device stop here would
        # interrupt exactly that.
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        await app._on_sonos_room_lost("RINCON_A", still_present=True)

        speaker.stop.assert_awaited_once_with(send_device_stop=False)
        assert app._speakers == []
        assert "RINCON_A" not in app._sonos_speakers_by_group_id

    async def test_room_lost_is_a_noop_for_unknown_uuid(self) -> None:
        app = QobuzProxy(_make_config())

        await app._on_sonos_room_lost("RINCON_UNKNOWN", still_present=False)  # must not raise


class TestRoomRenamed:
    async def test_renames_running_speaker_in_place(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        grouped_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
        )
        result = await app._on_sonos_room_renamed(grouped_room)

        assert result is True
        speaker.rename.assert_awaited_once_with("Kitchen, Living Room")

    async def test_returns_false_when_speaker_not_running(self) -> None:
        app = QobuzProxy(_make_config())

        result = await app._on_sonos_room_renamed(ROOM)

        assert result is False

    async def test_returns_false_on_name_collision(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        other = _mock_speaker("Kitchen, Living Room")
        app._speakers.append(other)

        grouped_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
        )
        result = await app._on_sonos_room_renamed(grouped_room)

        assert result is False
        speaker.rename.assert_not_called()


class TestRoomRetargeted:
    async def test_retargets_running_speaker_in_place(self) -> None:
        # A genuine handoff: the Speaker keeps its identity — tracked by the
        # group's stable tracking_key (group_id) — even though the physical
        # coordinator moved to Living Room's ip/port. No re-keying needed.
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=True)
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A:1"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_B",
            name="Living Room",
            ip="10.0.1.31",
            port=1400,
            is_stereo_pair=False,
            member_names=("Living Room", "Bedroom"),
            group_id="RINCON_A:1",  # same continuing group
        )
        result = await app._on_sonos_room_retargeted(new_room)

        assert result is True
        speaker.retarget.assert_awaited_once_with("10.0.1.31", 1400)
        speaker.rename.assert_awaited_once_with("Living Room, Bedroom")
        assert app._sonos_speakers_by_group_id["RINCON_A:1"] is speaker  # key unchanged

    async def test_ip_only_change_retargets_without_rename(self) -> None:
        # A plain IP change (e.g. DHCP) for the same coordinator — no
        # rename needed, display name is unchanged.
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=True)
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.99",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen",),
        )
        result = await app._on_sonos_room_retargeted(new_room)

        assert result is True
        speaker.retarget.assert_awaited_once_with("10.0.1.99", 1400)
        speaker.rename.assert_not_called()  # display name unchanged
        assert app._sonos_speakers_by_group_id["RINCON_A"] is speaker

    async def test_returns_false_when_speaker_not_running(self) -> None:
        app = QobuzProxy(_make_config())

        result = await app._on_sonos_room_retargeted(ROOM)

        assert result is False

    async def test_returns_false_when_backend_retarget_fails(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=False)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A:1"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_B",
            name="Living Room",
            ip="10.0.1.31",
            port=1400,
            is_stereo_pair=False,
            group_id="RINCON_A:1",
        )
        result = await app._on_sonos_room_retargeted(new_room)

        assert result is False
        assert app._sonos_speakers_by_group_id["RINCON_A:1"] is speaker  # unchanged


class TestRoomRekeyed:
    async def test_moves_speaker_to_the_new_key(self) -> None:
        # The coordinator never moved (same uuid/ip/port) — its group_id
        # just changed, most likely from a plain membership change. Must
        # not touch the backend beyond a no-op retarget, and must not
        # create/tear down anything.
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=True)
        speaker.rename = AsyncMock(return_value=True)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A:1"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            member_names=("Kitchen", "Living Room"),
            group_id="RINCON_A:2",
        )
        result = await app._on_sonos_room_rekeyed("RINCON_A:1", new_room)

        assert result is True
        speaker.retarget.assert_awaited_once_with("10.0.1.30", 1400)
        speaker.rename.assert_awaited_once_with("Kitchen, Living Room")
        assert "RINCON_A:1" not in app._sonos_speakers_by_group_id
        assert app._sonos_speakers_by_group_id["RINCON_A:2"] is speaker

    async def test_returns_false_when_speaker_not_running(self) -> None:
        app = QobuzProxy(_make_config())

        result = await app._on_sonos_room_rekeyed("RINCON_A:1", ROOM)

        assert result is False

    async def test_returns_false_when_backend_retarget_fails(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.retarget = AsyncMock(return_value=False)
        app._speakers.append(speaker)
        app._sonos_speakers_by_group_id["RINCON_A:1"] = speaker

        new_room = SonosRoom(
            uuid="RINCON_A",
            name="Kitchen",
            ip="10.0.1.30",
            port=1400,
            is_stereo_pair=False,
            group_id="RINCON_A:2",
        )
        result = await app._on_sonos_room_rekeyed("RINCON_A:1", new_room)

        assert result is False
        assert app._sonos_speakers_by_group_id["RINCON_A:1"] is speaker  # unchanged
        assert "RINCON_A:2" not in app._sonos_speakers_by_group_id


class TestRoomMembersDeparted:
    """The central rule from live testing: a device leaving the group we're
    actively playing to must be stopped directly (nothing else will ever
    tell it to); a device leaving any other, merely-discovered group must
    never be touched — see Speaker.is_active and _on_sonos_room_lost's
    still_present handling for the sibling case (a group's coordinator
    itself being absorbed elsewhere)."""

    def _mock_dlna_client(self):  # type: ignore[no-untyped-def]
        client = MagicMock()
        client.connect = AsyncMock()
        client.stop = AsyncMock()
        client.disconnect = AsyncMock()
        return client

    async def test_stops_a_device_leaving_the_active_group(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        client = self._mock_dlna_client()
        with patch("qobuz_proxy.app.DLNAClient", return_value=client) as MockClient:
            await app._on_sonos_room_members_departed(
                "RINCON_A", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )

        MockClient.assert_called_once_with("10.0.1.31", 1400)
        client.connect.assert_awaited_once()
        client.stop.assert_awaited_once()
        client.disconnect.assert_awaited_once()

    async def test_stops_every_departed_device(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        with patch("qobuz_proxy.app.DLNAClient", return_value=self._mock_dlna_client()) as Mock:
            await app._on_sonos_room_members_departed(
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
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = False
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        with patch("qobuz_proxy.app.DLNAClient") as MockClient:
            await app._on_sonos_room_members_departed(
                "RINCON_A", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )

        MockClient.assert_not_called()

    async def test_ignores_departure_for_unknown_tracking_key(self) -> None:
        app = QobuzProxy(_make_config())

        with patch("qobuz_proxy.app.DLNAClient") as MockClient:
            await app._on_sonos_room_members_departed(
                "RINCON_UNKNOWN", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )

        MockClient.assert_not_called()

    async def test_a_failed_stop_does_not_raise(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        app._sonos_speakers_by_group_id["RINCON_A"] = speaker

        client = self._mock_dlna_client()
        client.connect.side_effect = OSError("unreachable")
        with patch("qobuz_proxy.app.DLNAClient", return_value=client):
            await app._on_sonos_room_members_departed(
                "RINCON_A", (DepartedMember(uuid="RINCON_B", ip="10.0.1.31", port=1400),)
            )  # must not raise

        client.disconnect.assert_awaited_once()  # still cleaned up


class TestAddSpeakerGuard:
    async def test_manual_add_rejected_while_auto_discover_enabled(self) -> None:
        app = QobuzProxy(_make_config(sonos_auto_discover=True))

        with pytest.raises(ValueError, match="sonos_auto_discover"):
            await app._on_add_speaker({"name": "New Speaker", "dlna_ip": "10.0.1.50"})


class TestStopSpeakers:
    async def test_stop_speakers_stops_discovery_manager(self) -> None:
        app = QobuzProxy(_make_config())
        mock_manager = MagicMock()
        mock_manager.stop = AsyncMock()
        app._sonos_discovery = mock_manager
        app._sonos_speakers_by_group_id["RINCON_A"] = _mock_speaker()

        await app._stop_speakers()

        mock_manager.stop.assert_awaited_once()
        assert app._sonos_discovery is None
        assert app._sonos_speakers_by_group_id == {}

    async def test_active_speaker_gets_a_device_stop(self) -> None:
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Kitchen")
        speaker.is_active = True
        app._speakers.append(speaker)

        await app._stop_speakers()

        speaker.stop.assert_awaited_once_with(send_device_stop=True)

    async def test_idle_speaker_is_not_sent_a_device_stop(self) -> None:
        # Shutting down (or logging out) must not interrupt a merely
        # discovered, idle Sonos room nobody is actually playing to.
        app = QobuzProxy(_make_config())
        speaker = _mock_speaker("Living Room")
        speaker.is_active = False
        app._speakers.append(speaker)

        await app._stop_speakers()

        speaker.stop.assert_awaited_once_with(send_device_stop=False)

    async def test_mixed_active_and_idle_speakers_each_get_the_right_treatment(self) -> None:
        app = QobuzProxy(_make_config())
        active_speaker = _mock_speaker("Kitchen")
        active_speaker.is_active = True
        idle_speaker = _mock_speaker("Living Room")
        idle_speaker.is_active = False
        app._speakers.extend([active_speaker, idle_speaker])

        await app._stop_speakers()

        active_speaker.stop.assert_awaited_once_with(send_device_stop=True)
        idle_speaker.stop.assert_awaited_once_with(send_device_stop=False)

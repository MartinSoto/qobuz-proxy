"""AVTransport LastChange parsing, against real events captured from a Sonos One
(tests/fixtures/sonos_events)."""

from pathlib import Path

from qobuz_proxy.backends.dlna.sonos.audio_url import parse_entry_url
from qobuz_proxy.backends.dlna.sonos.transport_events import (
    parse_group_volume_event,
    parse_transport_event,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "sonos_events"


def event(name: str):  # type: ignore[no-untyped-def]
    return parse_transport_event((FIXTURES / name).read_text())


class TestRealEvents:
    def test_the_first_event_after_subscribing_says_everything(self) -> None:
        e = event("avt_initial_stopped.xml")

        assert e is not None
        assert e.transport_state == "STOPPED"
        assert e.play_mode == "NORMAL"
        assert e.number_of_tracks == 0 and e.current_track == 0
        assert e.current_track_uri == ""  # present and empty is not the same as absent
        assert e.current_track_duration_ms == 0

    def test_a_load_is_transitioning_with_the_entry_and_its_url(self) -> None:
        e = event("avt_transitioning_load.xml")

        assert e is not None
        assert e.transport_state == "TRANSITIONING"
        assert (e.current_track, e.number_of_tracks) == (1, 3)
        assert e.current_track_duration_ms == (2 * 60 + 54) * 1000
        parsed = parse_entry_url(e.current_track_uri or "")
        assert parsed is not None  # the escaping inside the escaped document is undone
        assert (parsed.key.track_id, parsed.key.queue_item_id) == ("417994379", 0)
        assert parsed.major == 1
        assert e.next_track_uri is not None
        next_parsed = parse_entry_url(e.next_track_uri)
        assert next_parsed is not None and next_parsed.key.queue_item_id == 1

    def test_playing_and_paused(self) -> None:
        assert event("avt_playing.xml").transport_state == "PLAYING"  # type: ignore[union-attr]
        assert event("avt_paused.xml").transport_state == "PAUSED_PLAYBACK"  # type: ignore[union-attr]

    def test_next_moves_to_the_following_entry(self) -> None:
        e = event("avt_next_track.xml")

        assert e is not None
        assert e.current_track == 2
        parsed = parse_entry_url(e.current_track_uri or "")
        assert parsed is not None and parsed.key.queue_item_id == 1

    def test_skipping_over_an_entry_names_the_one_landed_on(self) -> None:
        e = event("avt_skip_to_track_3.xml")

        assert e is not None
        assert e.current_track == 3
        parsed = parse_entry_url(e.current_track_uri or "")
        assert parsed is not None and parsed.key.queue_item_id == 2

    def test_a_play_mode_change_is_an_event(self) -> None:
        assert event("avt_repeat_all.xml").play_mode == "REPEAT_ALL"  # type: ignore[union-attr]

    def test_the_end_of_the_queue_stops_back_on_the_first_entry(self) -> None:
        e = event("avt_stopped_end_of_queue.xml")

        assert e is not None
        assert e.transport_state == "STOPPED"
        assert e.current_track == 1


class TestNotAnAvTransportEvent:
    def test_a_volume_event_is_not_one(self) -> None:
        assert event("rc_volume_event.xml") is None
        assert event("grc_group_volume.xml") is None

    def test_garbage_is_not_one(self) -> None:
        assert parse_transport_event("not xml at all") is None
        assert parse_transport_event("") is None

    def test_a_property_set_without_a_last_change_is_not_one(self) -> None:
        body = '<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0"></e:propertyset>'
        assert parse_transport_event(body) is None


class TestPartialEvents:
    def test_only_what_the_event_mentions_is_set(self) -> None:
        inner = (
            '<Event xmlns="urn:schemas-upnp-org:metadata-1-0/AVT/">'
            '<InstanceID val="0"><TransportState val="PAUSED_PLAYBACK"/></InstanceID></Event>'
        )
        body = (
            '<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0"><e:property>'
            f"<LastChange>{inner.replace('<', '&lt;').replace('>', '&gt;')}</LastChange>"
            "</e:property></e:propertyset>"
        )

        e = parse_transport_event(body)

        assert e is not None
        assert e.transport_state == "PAUSED_PLAYBACK"
        assert e.current_track is None and e.current_track_uri is None
        assert e.play_mode is None and e.current_track_duration_ms is None


class TestGroupVolumeEvents:
    def test_the_volume_and_mute_of_the_group(self) -> None:
        e = parse_group_volume_event((FIXTURES / "grc_group_volume.xml").read_text())

        assert e is not None
        assert e.volume == 5 and e.muted is True

    def test_other_events_are_not_volume_events(self) -> None:
        assert parse_group_volume_event((FIXTURES / "avt_playing.xml").read_text()) is None
        assert parse_group_volume_event("not xml") is None

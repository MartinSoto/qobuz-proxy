"""Tests for DLNABackend._build_didl metadata generation."""

import asyncio

import pytest

from qobuz_proxy.backends.types import BackendTrackMetadata, PlaybackState
from qobuz_proxy.backends.dlna.backend import DLNABackend
from qobuz_proxy.backends.dlna.capabilities import DLNACapabilities


def _make_backend() -> DLNABackend:
    backend = DLNABackend.__new__(DLNABackend)
    backend._capabilities = None
    return backend


def _make_metadata(**kwargs) -> BackendTrackMetadata:  # type: ignore[no-untyped-def]
    defaults = {
        "track_id": "1",
        "title": "Test Track",
        "artist": "Test Artist",
        "album": "Test Album",
        "duration_ms": 180000,
        "artwork_url": "",
    }
    defaults.update(kwargs)
    return BackendTrackMetadata(**defaults)


class TestBuildDidl:
    def test_hires_96k_includes_correct_audio_attributes(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert 'sampleFrequency="96000"' in didl
        assert 'bitsPerSample="24"' in didl

    def test_hires_192k_includes_correct_audio_attributes(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=192000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert 'sampleFrequency="192000"' in didl
        assert 'bitsPerSample="24"' in didl

    def test_cd_quality_includes_correct_audio_attributes(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=44100, bit_depth=16)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert 'sampleFrequency="44100"' in didl
        assert 'bitsPerSample="16"' in didl

    def test_no_audio_attributes_when_format_unknown(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=0, bit_depth=0)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert "sampleFrequency" not in didl
        assert "bitsPerSample" not in didl

    def test_audio_attributes_are_on_res_element(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        # Attributes must appear inside the <res ...> tag, not elsewhere
        res_start = didl.index("<res ")
        res_end = didl.index(">", res_start)
        res_tag = didl[res_start:res_end]
        assert 'sampleFrequency="96000"' in res_tag
        assert 'bitsPerSample="24"' in res_tag

    def test_track_metadata_in_didl(self) -> None:
        backend = _make_backend()
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        didl = backend._build_didl("http://proxy/track.flac", metadata)

        assert "Test Track" in didl
        assert "Test Artist" in didl
        assert "Test Album" in didl


class TestQualityDetectionConfirmed:
    """Backend exposes whether auto-detected quality came from explicit device info."""

    def test_unconfirmed_without_capabilities(self):
        from qobuz_proxy.backends.dlna.backend import DLNABackend

        backend = DLNABackend("192.168.1.10")
        assert backend.quality_detection_confirmed is False

    def test_follows_capabilities_flag(self):
        from qobuz_proxy.backends.dlna.backend import DLNABackend
        from qobuz_proxy.backends.dlna.capabilities import parse_protocol_info_sink

        backend = DLNABackend("192.168.1.10")

        backend._capabilities = parse_protocol_info_sink(
            "http-get:*:audio/flac:DLNA.ORG_PN=FLAC_192;DLNA.ORG_OP=01"
        )
        assert backend.quality_detection_confirmed is True

        backend._capabilities = parse_protocol_info_sink("http-get:*:audio/x-flac:*")
        assert backend.quality_detection_confirmed is False


class TestIsPlayingOurContent:
    """Detects an external takeover (another source now playing to this
    renderer) — get_state() alone reports PLAYING either way, so this
    compares the device's actual current track URI against the one we
    last set. Standard DLNA: GetMediaInfo.CurrentURI. See
    test_sonos_backend.py's own TestIsPlayingOurContent for the Sonos
    queue-playback variant (GetPositionInfo.TrackURI instead)."""

    def _make_backend(self, proxy_url: str = "http://proxy/track.flac"):  # type: ignore[no-untyped-def]
        from unittest.mock import AsyncMock

        backend = DLNABackend.__new__(DLNABackend)
        backend._client = AsyncMock()
        backend._current_proxy_url = proxy_url
        backend._next_track_proxy_url = None
        backend._playback_started_at = 0.0  # well outside the grace period
        return backend

    async def test_true_when_uri_matches(self):
        backend = self._make_backend()
        backend._client.get_media_info.return_value = "http://proxy/track.flac"

        assert await backend.is_playing_our_content() is True

    async def test_false_when_uri_does_not_match(self):
        backend = self._make_backend()
        backend._client.get_media_info.return_value = "http://someone-else/spotify-stream"

        assert await backend.is_playing_our_content() is False

    async def test_true_when_uri_matches_the_armed_next_track(self):
        # A gapless transition already in flight is a legitimate URI
        # change, not a takeover.
        backend = self._make_backend()
        backend._next_track_proxy_url = "http://proxy/next.flac"
        backend._client.get_media_info.return_value = "http://proxy/next.flac"

        assert await backend.is_playing_our_content() is True

    async def test_true_when_nothing_of_ours_playing_yet(self):
        backend = self._make_backend(proxy_url="")
        backend._current_proxy_url = None

        assert await backend.is_playing_our_content() is True
        backend._client.get_media_info.assert_not_called()

    async def test_true_on_transient_read_failure(self):
        backend = self._make_backend()
        backend._client.get_media_info.return_value = None

        assert await backend.is_playing_our_content() is True

    async def test_true_during_grace_period_even_on_mismatched_uri(self):
        # Just started playback (or just retargeted — see TestRetarget) —
        # the device's own reported state can be transiently wrong while
        # things settle; a mismatch here must not be treated as evidence
        # of anything.
        import time

        backend = self._make_backend()
        backend._playback_started_at = time.monotonic()
        backend._client.get_media_info.return_value = "http://someone-else/spotify-stream"

        assert await backend.is_playing_our_content() is True
        backend._client.get_media_info.assert_not_called()  # short-circuits, no read needed

    async def test_false_once_grace_period_has_elapsed(self):
        import time

        from qobuz_proxy.backends.dlna.backend import PLAYBACK_START_GRACE_PERIOD_SECONDS

        backend = self._make_backend()
        backend._playback_started_at = time.monotonic() - PLAYBACK_START_GRACE_PERIOD_SECONDS - 1
        backend._client.get_media_info.return_value = "http://someone-else/spotify-stream"

        assert await backend.is_playing_our_content() is False


class TestRetarget:
    """DLNABackend.retarget() — repoint at a new DLNA endpoint in place,
    without dropping whatever session this backend is part of."""

    def _make_device_info(self, **kwargs):  # type: ignore[no-untyped-def]
        from qobuz_proxy.backends.dlna.client import DLNADeviceInfo

        defaults = dict(
            friendly_name="Living Room",
            manufacturer="Sonos, Inc.",
            model_name="Sonos Five",
            udn="uuid:RINCON_LIVINGROOM",
            av_transport_url="http://10.0.1.31:1400/MediaRenderer/AVTransport/Control",
            rendering_control_url="http://10.0.1.31:1400/MediaRenderer/RenderingControl/Control",
            connection_manager_url="http://10.0.1.31:1400/MediaRenderer/ConnectionManager/Control",
        )
        defaults.update(kwargs)
        return DLNADeviceInfo(**defaults)

    async def test_retarget_swaps_client_ip_and_name_on_success(self):
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend("10.0.1.30", 1400)
        old_client = AsyncMock()
        old_client.disconnect = AsyncMock()
        backend._client = old_client
        backend._is_connected = True

        new_client = AsyncMock()
        new_client.connect = AsyncMock(return_value=self._make_device_info())
        new_client.get_protocol_info = AsyncMock(return_value=None)

        with patch.object(backend, "_client_class", return_value=new_client):
            result = await backend.retarget("10.0.1.31", 1400)

        assert result is True
        assert backend._ip == "10.0.1.31"
        assert backend._port == 1400
        assert backend._client is new_client
        assert backend.name == "Living Room"
        # Not stopped here — a separate caller decides whether the old
        # device should also be told to stop, since that depends on
        # context this backend has no visibility into.
        old_client.stop.assert_not_called()
        old_client.disconnect.assert_awaited_once()

    async def test_retarget_resets_the_grace_period(self):
        # See PLAYBACK_START_GRACE_PERIOD_SECONDS / is_playing_our_content:
        # a fresh coordinator's own reported state can be transiently wrong
        # right after a handoff, so a successful retarget must restart the
        # same grace window used when playback first starts.
        import time
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend("10.0.1.30", 1400)
        backend._client = AsyncMock()
        backend._is_connected = True
        backend._playback_started_at = time.monotonic() - 3600  # long stale

        new_client = AsyncMock()
        new_client.connect = AsyncMock(return_value=self._make_device_info())
        new_client.get_protocol_info = AsyncMock(return_value=None)

        with patch.object(backend, "_client_class", return_value=new_client):
            before = time.monotonic()
            result = await backend.retarget("10.0.1.31", 1400)

        assert result is True
        assert backend._playback_started_at >= before

    async def test_retarget_clears_gapless_state(self):
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend("10.0.1.30", 1400)
        backend._client = AsyncMock()
        backend._next_track_proxy_url = "http://proxy/audio/1.flac"
        backend._next_track_metadata = _make_metadata(track_id="1")
        backend._next_track_queue_nr = 3

        new_client = AsyncMock()
        new_client.connect = AsyncMock(return_value=self._make_device_info())
        new_client.get_protocol_info = AsyncMock(return_value=None)

        with patch.object(backend, "_client_class", return_value=new_client):
            await backend.retarget("10.0.1.31", 1400)

        assert backend._next_track_proxy_url is None
        assert backend._next_track_metadata is None
        assert backend._next_track_queue_nr is None

    async def test_retarget_reconnects_a_detached_backend_even_to_the_same_address(self):
        # A backend that was detach()ed (see Speaker.detach — used while a
        # Sonos group_id is pending) has self._client is None and
        # _is_connected False, but self._ip/_port still hold the last
        # target. If that same address is where the group_id resolves back
        # to, the naive "ip/port unchanged" fast path must NOT no-op —
        # nothing would ever actually reconnect or resume polling.
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend("10.0.1.30", 1400)
        backend._client = None
        backend._is_connected = False
        backend._poll_task = None

        new_client = AsyncMock()
        new_client.connect = AsyncMock(return_value=self._make_device_info())
        new_client.get_protocol_info = AsyncMock(return_value=None)

        with patch.object(backend, "_client_class", return_value=new_client):
            result = await backend.retarget("10.0.1.30", 1400)  # same address as before

        assert result is True
        assert backend._client is new_client
        assert backend._is_connected is True
        assert backend._poll_task is not None
        backend._poll_task.cancel()

    async def test_retarget_keeps_old_target_on_connect_failure(self):
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend("10.0.1.30", 1400)
        old_client = AsyncMock()
        backend._client = old_client

        new_client = AsyncMock()
        new_client.connect = AsyncMock(side_effect=ConnectionError("unreachable"))
        new_client.disconnect = AsyncMock()

        with patch.object(backend, "_client_class", return_value=new_client):
            result = await backend.retarget("10.0.1.31", 1400)

        assert result is False
        assert backend._ip == "10.0.1.30"
        assert backend._client is old_client
        old_client.disconnect.assert_not_called()

    async def test_retarget_to_same_target_is_a_noop(self):
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend("10.0.1.30", 1400)
        old_client = AsyncMock()
        backend._client = old_client
        backend._is_connected = True  # already connected to this exact target

        with patch.object(backend, "_client_class") as MockClient:
            result = await backend.retarget("10.0.1.30", 1400)

        assert result is True
        MockClient.assert_not_called()
        assert backend._client is old_client


class TestWaitForReconnect:
    """play() landing while this backend is detached (see Speaker.detach,
    SonosDiscoveryManager's pending state) must not just fail outright —
    a legitimate in-flight command (e.g. a track auto-advancing) can race
    a topology-driven detach that resolves moments later."""

    async def test_true_immediately_if_already_connected(self):
        from unittest.mock import AsyncMock

        backend = DLNABackend.__new__(DLNABackend)
        backend._client = AsyncMock()

        assert await backend._wait_for_reconnect() is True

    async def test_true_once_client_reappears_within_the_window(self):
        import asyncio
        from unittest.mock import AsyncMock, patch

        backend = DLNABackend.__new__(DLNABackend)
        backend._client = None

        async def reconnect_shortly() -> None:
            await asyncio.sleep(0.01)
            backend._client = AsyncMock()

        with patch("qobuz_proxy.backends.dlna.backend._RECONNECT_POLL_INTERVAL_SECONDS", 0.005):
            task = asyncio.create_task(reconnect_shortly())
            result = await backend._wait_for_reconnect(timeout=1.0)
        await task

        assert result is True

    async def test_false_once_the_window_elapses(self):
        from unittest.mock import patch

        backend = DLNABackend.__new__(DLNABackend)
        backend._client = None

        with patch("qobuz_proxy.backends.dlna.backend._RECONNECT_POLL_INTERVAL_SECONDS", 0.01):
            result = await backend._wait_for_reconnect(timeout=0.03)

        assert result is False

    async def test_play_raises_only_after_the_wait_fails(self):
        from unittest.mock import AsyncMock

        backend = DLNABackend.__new__(DLNABackend)
        backend._client = None
        backend._wait_for_reconnect = AsyncMock(return_value=False)

        with pytest.raises(RuntimeError, match="Not connected"):
            await backend.play("http://proxy/audio/1.flac", _make_metadata())

        backend._wait_for_reconnect.assert_awaited_once()


class TestPollStateLoop:
    """DLNABackend._poll_state_loop is the only thing that reads transport
    state/position from the physical device (rows 1+2 of the concurrent-
    actors design work). It also owns hijack detection and paused-
    external-stop confirmation, pushed out to Player via callbacks — see
    tests/playback/test_player_hijack_detection.py and
    test_player_reporting.py for the player-side reaction to those."""

    def _make_backend(self) -> DLNABackend:
        import time
        from unittest.mock import AsyncMock

        backend = DLNABackend.__new__(DLNABackend)
        backend._client = AsyncMock()
        backend._is_connected = True
        backend._state = PlaybackState.STOPPED
        backend._current_metadata = None
        backend._current_proxy_url = "http://proxy/track.flac"
        backend._next_track_proxy_url = None
        backend._next_track_metadata = None
        backend._next_track_queue_nr = None
        backend._position_ms = 0
        backend._duration_ms = 0
        backend._playback_started_at = time.monotonic() - 3600  # well outside grace
        backend._paused_stop_polls = 0
        backend._hijack_check_countdown = 0
        backend._on_state_change = None
        backend._on_position_update = None
        backend._on_track_ended = None
        backend._on_playback_error = None
        backend._on_next_track_started = None
        backend._on_external_takeover = None
        backend.get_state = AsyncMock(return_value=PlaybackState.STOPPED)
        backend.get_position = AsyncMock(return_value=0)
        backend.is_playing_our_content = AsyncMock(return_value=True)
        return backend

    async def _run_poll_cycles(self, backend: DLNABackend, cycles: int = 1) -> None:
        """Run _poll_state_loop for exactly `cycles` poll intervals, then
        stop it. Counts backend.get_state() calls (one per iteration)
        rather than a flat sleep — this loop's own iteration is fast once
        the interval is patched down, so a flat-time buffer generous
        enough not to undercount would run several extra cycles past what
        a precise test (e.g. "one stray reading doesn't confirm a stop")
        needs to stay exact."""
        from unittest.mock import patch

        with patch("qobuz_proxy.backends.dlna.backend.STATE_POLL_INTERVAL_SECONDS", 0.005):
            task = asyncio.create_task(backend._poll_state_loop())
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 2.0
            while backend.get_state.call_count < cycles and loop.time() < deadline:
                await asyncio.sleep(0.001)
            backend._is_connected = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_takeover_fires_the_callback(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        backend = self._make_backend()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.is_playing_our_content = AsyncMock(return_value=False)
        backend._hijack_check_countdown = 1  # force the check on the first cycle
        callback = MagicMock()
        backend.on_external_takeover(callback)

        await self._run_poll_cycles(backend)

        callback.assert_called_once()

    async def test_no_takeover_does_not_fire_the_callback(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        backend = self._make_backend()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.is_playing_our_content = AsyncMock(return_value=True)
        backend._hijack_check_countdown = 1
        callback = MagicMock()
        backend.on_external_takeover(callback)

        await self._run_poll_cycles(backend)

        callback.assert_not_called()

    async def test_hijack_check_is_throttled_not_run_every_cycle(self) -> None:
        from unittest.mock import AsyncMock

        from qobuz_proxy.backends.dlna.backend import _HIJACK_CHECK_INTERVAL_POLLS

        backend = self._make_backend()
        backend._state = PlaybackState.PLAYING
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)
        backend.is_playing_our_content = AsyncMock(return_value=True)
        backend._hijack_check_countdown = _HIJACK_CHECK_INTERVAL_POLLS + 5  # not due yet

        await self._run_poll_cycles(backend)

        backend.is_playing_our_content.assert_not_called()

    async def test_hijack_not_checked_while_paused(self) -> None:
        from unittest.mock import AsyncMock

        backend = self._make_backend()
        backend._state = PlaybackState.PAUSED
        backend.get_state = AsyncMock(return_value=PlaybackState.PAUSED)
        backend.is_playing_our_content = AsyncMock(return_value=False)

        await self._run_poll_cycles(backend)

        backend.is_playing_our_content.assert_not_called()

    async def test_paused_stop_confirmed_after_consecutive_polls(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from qobuz_proxy.backends.dlna.backend import _PAUSED_STOP_CONFIRMATIONS

        backend = self._make_backend()
        backend._state = PlaybackState.PAUSED
        backend.get_state = AsyncMock(return_value=PlaybackState.STOPPED)
        callback = MagicMock()
        backend.on_state_change(callback)

        await self._run_poll_cycles(backend, cycles=_PAUSED_STOP_CONFIRMATIONS + 2)

        callback.assert_called_once_with(PlaybackState.STOPPED)

    async def test_single_transient_stopped_read_does_not_confirm(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        backend = self._make_backend()
        backend._state = PlaybackState.PAUSED
        backend.get_state = AsyncMock(return_value=PlaybackState.STOPPED)
        callback = MagicMock()
        backend.on_state_change(callback)

        await self._run_poll_cycles(backend, cycles=1)

        callback.assert_not_called()

    async def test_paused_stop_confirmation_resets_on_non_stopped_read(self) -> None:
        """A single non-STOPPED read between two STOPPED ones must restart
        the confirmation count, not just pause it."""
        from unittest.mock import AsyncMock

        backend = self._make_backend()
        backend._state = PlaybackState.PAUSED
        backend._paused_stop_polls = 2  # one more STOPPED read would confirm
        backend.get_state = AsyncMock(return_value=PlaybackState.PLAYING)

        await self._run_poll_cycles(backend, cycles=1)

        assert backend._paused_stop_polls == 0

    async def test_cold_pause_backend_never_started_is_not_confirmed_as_stopped(self) -> None:
        """A track loaded paused but never started on this backend (see
        Player._pause_locked's cold-pause branch) never calls
        backend.pause(), so this backend's own _state never becomes
        PAUSED — the confirmation logic (keyed off self._state) must never
        trigger for it even though get_state() legitimately reports
        STOPPED (nothing loaded)."""
        from unittest.mock import AsyncMock, MagicMock

        backend = self._make_backend()
        backend._state = PlaybackState.STOPPED  # never played here
        backend.get_state = AsyncMock(return_value=PlaybackState.STOPPED)
        callback = MagicMock()
        backend.on_state_change(callback)

        await self._run_poll_cycles(backend, cycles=5)

        callback.assert_not_called()


class TestTranscodeDecision:
    """DLNABackend._transcode_sample_rate_for decides whether a specific
    track needs on-the-fly downsampling — see TranscodingFlacReader and
    capabilities.py's max_quality docstring for the full rationale.

    hires_downsampling is experimental and opt-in (default off) — every
    test below except the dedicated "flag off" ones explicitly enables it,
    since that's the state actually being tested."""

    def _make_backend(self) -> DLNABackend:
        backend = _make_backend()
        backend._hires_downsampling = True
        return backend

    def _caps(self, max_sample_rate: int, max_bit_depth: int) -> DLNACapabilities:
        return DLNACapabilities(
            supports_flac=True, max_sample_rate=max_sample_rate, max_bit_depth=max_bit_depth
        )

    def test_no_transcode_needed_when_track_fits_the_cap(self) -> None:
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=44100, bit_depth=24)

        assert backend._transcode_sample_rate_for(metadata) is None

    def test_transcodes_when_track_exceeds_the_cap(self) -> None:
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        assert backend._transcode_sample_rate_for(metadata) == 48000

    def test_no_transcode_for_a_cd_only_device(self) -> None:
        """A device without real 24-bit support (max_quality already falls
        back to CD for it) never needs this — it never receives a track
        that could exceed 44.1/16 in the first place."""
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=16)
        metadata = _make_metadata(sample_rate=44100, bit_depth=16)

        assert backend._transcode_sample_rate_for(metadata) is None

    def test_no_transcode_without_capabilities(self) -> None:
        backend = self._make_backend()
        backend._capabilities = None
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        assert backend._transcode_sample_rate_for(metadata) is None

    def test_no_transcode_when_track_sample_rate_is_unknown(self) -> None:
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=0, bit_depth=0)

        assert backend._transcode_sample_rate_for(metadata) is None

    def test_content_type_becomes_wav_when_transcoding(self) -> None:
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        content_type, rate = backend._resolve_content_type_and_transcode(
            "http://cdn/track.flac", metadata
        )

        assert content_type == "audio/wav"
        assert rate == 48000

    def test_content_type_stays_flac_when_not_transcoding(self) -> None:
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=44100, bit_depth=24)

        content_type, rate = backend._resolve_content_type_and_transcode(
            "http://cdn/track.flac", metadata
        )

        assert content_type == "audio/flac"
        assert rate is None

    def test_logs_downsampling_at_info(self, caplog) -> None:  # type: ignore[no-untyped-def]
        """The actual path taken (kept as-is vs. downsampled) must be
        visible in the logs for every track, not just inferable from
        content-type — see _resolve_content_type_and_transcode's docstring."""
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        with caplog.at_level("INFO"):
            backend._resolve_content_type_and_transcode("http://cdn/track.flac", metadata)

        assert "downsampling" in caplog.text
        assert "96000" in caplog.text
        assert "48000" in caplog.text

    def test_logs_keeping_native_rate_at_info(self, caplog) -> None:  # type: ignore[no-untyped-def]
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=44100, bit_depth=24)

        with caplog.at_level("INFO"):
            backend._resolve_content_type_and_transcode("http://cdn/track.flac", metadata)

        assert "keeping" in caplog.text
        assert "44100" in caplog.text
        assert "downsampling" not in caplog.text

    def test_logs_something_even_without_quality_info(self, caplog) -> None:  # type: ignore[no-untyped-def]
        backend = self._make_backend()
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=0, bit_depth=0)

        with caplog.at_level("INFO"):
            backend._resolve_content_type_and_transcode("http://cdn/track.flac", metadata)

        assert "Track 1" in caplog.text

    def test_flag_off_never_transcodes_even_with_24bit_capabilities(self) -> None:
        """hires_downsampling defaults to off — this is the actual default
        behavior for every speaker unless explicitly opted in via
        config.yaml/QOBUZPROXY_DLNA_HIRES_DOWNSAMPLING."""
        backend = _make_backend()  # note: plain _make_backend(), not self._make_backend()
        backend._hires_downsampling = False
        backend._capabilities = self._caps(max_sample_rate=48000, max_bit_depth=24)
        metadata = _make_metadata(sample_rate=96000, bit_depth=24)

        assert backend._transcode_sample_rate_for(metadata) is None

        content_type, rate = backend._resolve_content_type_and_transcode(
            "http://cdn/track.flac", metadata
        )
        assert content_type == "audio/flac"
        assert rate is None

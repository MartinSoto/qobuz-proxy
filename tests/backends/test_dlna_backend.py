"""Tests for DLNABackend._build_didl metadata generation."""

from qobuz_proxy.backends.types import BackendTrackMetadata
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

        with patch.object(backend, "_client_class") as MockClient:
            result = await backend.retarget("10.0.1.30", 1400)

        assert result is True
        MockClient.assert_not_called()
        assert backend._client is old_client


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

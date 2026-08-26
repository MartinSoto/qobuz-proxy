"""Tests for the Sonos device-capability override.

Every Sonos device is capped at 48kHz (a platform-wide ceiling, not a
per-model quirk); bit depth is a blacklist of known 16-bit-only legacy
models, defaulting new/unlisted models to 24-bit — see
sonos.capabilities's own module docstring for why a blacklist and not a
whitelist.
"""

from qobuz_proxy.backends.dlna.capabilities import (
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_CD,
    DLNACapabilities,
    apply_device_overrides,
)

# Importing this is what registers the override into capabilities.py's
# generic registry — see sonos/__init__.py.
import qobuz_proxy.backends.dlna.sonos  # noqa: F401


def _caps(advertised_sr: int = 192000, advertised_bd: int = 24) -> DLNACapabilities:
    return DLNACapabilities(
        supports_flac=True, max_sample_rate=advertised_sr, max_bit_depth=advertised_bd
    )


class TestSonosDeviceOverrides:
    def test_unlisted_sonos_model_defaults_to_24bit_48k_when_hires_enabled(self) -> None:
        """A model not on the legacy blacklist — including any future Sonos
        product this code has never heard of — gets treated as 24-bit
        capable, sample-rate-capped at 48kHz. Requires the experimental
        hires_downsampling opt-in (see the flag-off test below)."""
        caps = _caps()
        apply_device_overrides(caps, "Sonos, Inc.", "One SL", hires_downsampling=True)

        assert caps.max_sample_rate == 48000
        assert caps.max_bit_depth == 24
        assert caps.max_quality == QOBUZ_QUALITY_96K

    def test_hires_disabled_by_default_even_for_an_unlisted_model(self) -> None:
        """hires_downsampling is experimental and opt-in — without it,
        every Sonos stays at the old conservative 16-bit cap regardless
        of model, matching the pre-feature default."""
        caps = _caps()
        apply_device_overrides(caps, "Sonos, Inc.", "One SL")

        assert caps.max_sample_rate == 48000
        assert caps.max_bit_depth == 16
        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_blacklisted_legacy_model_stays_16bit(self) -> None:
        """Even with hires enabled, a blacklisted legacy model stays capped
        — verifies the blacklist match itself, not just the flag gate
        (which alone would already produce 16-bit)."""
        caps = _caps()
        apply_device_overrides(caps, "Sonos, Inc.", "Play:1", hires_downsampling=True)

        assert caps.max_sample_rate == 48000
        assert caps.max_bit_depth == 16
        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_blacklist_match_is_case_insensitive(self) -> None:
        caps = _caps()
        apply_device_overrides(caps, "SONOS", "PLAY:3", hires_downsampling=True)

        assert caps.max_bit_depth == 16

    def test_override_wins_even_if_advertised_more(self) -> None:
        """A legacy model that (wrongly) advertises Hi-Res 192k must still
        end up capped — the override doesn't just fill in gaps."""
        caps = _caps(advertised_sr=192000, advertised_bd=24)
        apply_device_overrides(caps, "Sonos, Inc.", "Play:1", hires_downsampling=True)

        assert caps.max_quality == QOBUZ_QUALITY_CD

    def test_non_sonos_device_is_untouched(self) -> None:
        caps = _caps(advertised_sr=192000, advertised_bd=24)
        apply_device_overrides(caps, "Denon", "AVR-X1700H", hires_downsampling=True)

        assert caps.max_sample_rate == 192000
        assert caps.max_bit_depth == 24

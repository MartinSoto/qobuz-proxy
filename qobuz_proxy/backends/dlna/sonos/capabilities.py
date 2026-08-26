"""
Sonos-specific device-capability override, registered into capabilities.py's
generic registry (see register_override()).

Importing this module (which backends/dlna/sonos/__init__.py always does)
is what makes the override active — the generic capabilities.py module
itself has no Sonos knowledge at all.
"""

import logging

from ..capabilities import DLNACapabilities, register_override

logger = logging.getLogger(__name__)

# Every Sonos device is capped at 48kHz — that's a platform-wide ceiling of
# the S2 audio pipeline itself (confirmed against Sonos's own community docs:
# https://en.community.sonos.com/controllers-and-music-services-228995/is-sonos-downgrading-down-sampling-all-hd-to-44-1-khz-24-48-16-44-1-s2-what-s-the-point-6862835),
# not a per-model quirk, so it's unconditional. Bit depth is a blacklist
# instead of a whitelist: Sonos's current lineup is small, every actively
# S2-compatible model newer than the two below supports 24-bit hi-res audio,
# and a blacklist means a *new* Sonos model defaults to being treated as
# 24-bit-capable without needing a code update, rather than silently getting
# capped to CD quality until someone adds it to a whitelist.
SONOS_MAX_SAMPLE_RATE = 48000
SONOS_16BIT_ONLY_MODELS = frozenset({"play:1", "play:3"})


def _apply_sonos_overrides(
    caps: DLNACapabilities, manufacturer: str, model: str, hires_downsampling: bool
) -> None:
    """
    Apply known Sonos-wide (and, for bit depth, per-model) limitations.

    Args:
        caps: Capabilities object to modify in place
        manufacturer: Device manufacturer string
        model: Device model string
        hires_downsampling: Opt-in for the 24-bit/48kHz-by-default
            detection (see SONOS_16BIT_ONLY_MODELS) and the on-the-fly
            downsampling it depends on (TranscodingFlacReader) — both are
            experimental. False (the default) keeps every Sonos device at
            the old, conservative 16-bit/48kHz cap regardless of model.
    """
    caps.max_sample_rate = SONOS_MAX_SAMPLE_RATE
    if not hires_downsampling:
        caps.max_bit_depth = 16
        return

    model_l = model.lower()
    if any(legacy in model_l for legacy in SONOS_16BIT_ONLY_MODELS):
        logger.info(f"Applying Sonos {model!r} override: 16-bit/48kHz (legacy model)")
        caps.max_bit_depth = 16
    else:
        logger.info(f"Applying Sonos {model!r} override: 24-bit/48kHz")
        caps.max_bit_depth = 24


register_override("Sonos", _apply_sonos_overrides)

__all__ = ["SONOS_MAX_SAMPLE_RATE", "SONOS_16BIT_ONLY_MODELS"]

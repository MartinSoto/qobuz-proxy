"""Choosing a stream's container from track metadata, without asking Qobuz
for the stream.

The Sonos device decodes by the MIME type in the queue entry's DIDL-Lite
(a wrong one makes it run through the queue, a wildcard is refused —
docs/sonos-queue-handover-design.md, 9.2), so the container (FLAC passthrough,
WAV transcode, MP3) has to be known when an entry is *published*, not when
the device later requests the stream. When a whole queue is published that
rules out a getFileUrl call per track; the track's best available format
(track/getList: maximum_sampling_rate / maximum_bit_depth) is enough, because
what the stream endpoint returns can only be at or below it.

The decision mirrors SonosAudioProxyServer.resolve_track's, which makes it
from the stream actually returned: a track that fits the device passes
straight through at the tier that holds it; one that does not is downsampled
on the fly (when allowed); anything else falls back to CD quality. A track
whose format is unknown is treated like one that does not fit, because a
wrong guess in the other direction would publish a FLAC entry the device
cannot play.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from ..capabilities import (
    QOBUZ_QUALITY_192K,
    QOBUZ_QUALITY_96K,
    QOBUZ_QUALITY_CD,
    QOBUZ_QUALITY_MP3,
)

if TYPE_CHECKING:
    from ..capabilities import DLNACapabilities

# What each Qobuz quality tier can deliver at most: (sample rate Hz, bit depth).
_TIER_LIMITS = {
    QOBUZ_QUALITY_MP3: (44100, 16),
    QOBUZ_QUALITY_CD: (44100, 16),
    QOBUZ_QUALITY_96K: (96000, 24),
    QOBUZ_QUALITY_192K: (192000, 24),
}


@dataclass(frozen=True)
class ContainerChoice:
    """How an entry will be served: the URL parameters, the MIME type to
    declare in its DIDL-Lite, and what the device will actually receive."""

    fmt: str  # "flac" | "wav" | "mp3" — the URL's `format`
    depth: int  # the URL's `depth`
    rate: int  # the URL's `rate`: a tier's top rate for passthrough, the target for wav
    content_type: str
    format_id: int  # the Qobuz tier served from, for quality reporting
    sample_rate: int  # Hz the device receives
    bit_depth: int


def _passthrough_tier(rate_hz: int, depth: int) -> int:
    """The lowest Qobuz tier that holds a source of this rate and depth."""
    if depth <= 16 and rate_hz <= 44100:
        return QOBUZ_QUALITY_CD
    if rate_hz <= 96000:
        return QOBUZ_QUALITY_96K
    return QOBUZ_QUALITY_192K


def choose_container(
    max_sample_rate_hz: int,
    max_bit_depth: int,
    capabilities: Optional["DLNACapabilities"],
    *,
    hires_downsampling: bool,
    force_transcode: bool = False,
    forced_format_id: Optional[int] = None,
) -> ContainerChoice:
    """Decide how to serve a track whose best available format is given.

    Args:
        max_sample_rate_hz: Best sample rate Qobuz has for the track (0 =
            unknown).
        max_bit_depth: Best bit depth (0 = unknown).
        capabilities: What the device can play (None = never discovered).
        hires_downsampling: Whether a track the device cannot play as-is may
            be downsampled on the fly instead of falling back to CD quality.
        force_transcode: Route every FLAC-capable track through the
            transcoder (playback gain needs it); MP3 is never transcoded.
        forced_format_id: A manual quality override: the source is limited to
            what that tier delivers.
    """
    rate, depth = max_sample_rate_hz, max_bit_depth
    if forced_format_id is not None:
        tier_rate, tier_depth = _TIER_LIMITS.get(forced_format_id, (192000, 24))
        rate = min(rate, tier_rate) if rate else tier_rate
        depth = min(depth, tier_depth) if depth else tier_depth

    if (
        capabilities is None
        or not capabilities.supports_flac
        or forced_format_id == QOBUZ_QUALITY_MP3
    ):
        return ContainerChoice("mp3", 16, 44100, "audio/mpeg", QOBUZ_QUALITY_MP3, 44100, 16)

    known = rate > 0 and depth > 0
    fits = known and rate <= capabilities.max_sample_rate and depth <= capabilities.max_bit_depth

    if fits and not force_transcode:
        tier = _passthrough_tier(rate, depth)
        tier_rate, tier_depth = _TIER_LIMITS[tier]
        return ContainerChoice("flac", tier_depth, tier_rate, "audio/flac", tier, rate, depth)

    if (hires_downsampling or force_transcode) and capabilities.max_bit_depth >= 24:
        # Nothing to resample for a track already at or below the device's rate
        # (the force_transcode case): keep its own rate. Unknown format: assume
        # the device's ceiling, the transcoder never upsamples past the source.
        target = min(rate, capabilities.max_sample_rate) if rate else capabilities.max_sample_rate
        tier = _passthrough_tier(rate or 192000, depth or 24)
        return ContainerChoice("wav", 24, target, "audio/wav", tier, target, 24)

    return ContainerChoice("flac", 16, 44100, "audio/flac", QOBUZ_QUALITY_CD, 44100, 16)

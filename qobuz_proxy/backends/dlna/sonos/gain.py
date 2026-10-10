"""
Approximate loudness normalization for Sonos's Hi-Res downsampling
pipeline — see proxy_server.py's ``hires_downsampling_gain``.

Qobuz exposes only *track* gain (``audio_info.replaygain_track_gain`` /
``replaygain_track_peak`` — see api_client.py's ``get_tracks_audio_info``),
never an album one. "Album gain" here is therefore an approximation: a
duration-weighted average of the individual track gains of whatever is
currently in the queue (see playback/queue.py's ``QueueMembership``),
applied only when every track in it shares one ``album_id`` — confirmed
live not to require the queue containing the *complete* album, or its
tracks being in original order, only that they all belong to the same
one. Falls back to plain per-track gain otherwise, and to no adjustment
at all (0 dB) if Qobuz has no gain data for a track or a lookup fails —
this must never block or delay playback; a later track just tries again.

The duration-weighted average mirrors what a real album-loudness
integration would emphasize (louder *and* longer tracks dominate
perceived loudness) better than picking a single track's value would,
while the per-track peak clamp in ``_clamp`` keeps it safe: the shared
group gain can never push the one track actually being clamped past its
own peak, even though it was computed from other tracks' mastering.
"""

from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from qobuz_proxy.auth.api_client import QobuzAPIClient, TrackAudioInfo
    from qobuz_proxy.playback.queue import QobuzQueue

logger = logging.getLogger(__name__)

# Gain lines closer together than this belong to one burst (a queue publish).
_INFO_BURST_SECONDS = 10.0


class QueueGainResolver:
    """Resolves the effective playback gain (dB) for one track, given the
    current queue — see module docstring."""

    def __init__(self, api_client: "QobuzAPIClient", queue: Optional["QobuzQueue"]) -> None:
        self._api = api_client
        self._queue = queue
        # Unbounded on purpose: a TrackAudioInfo is a handful of floats
        # and strings, and a real session never plays enough distinct
        # tracks for that to matter — see MetadataCache for the pattern
        # this deliberately doesn't need to copy.
        self._cache: dict[str, "TrackAudioInfo"] = {}
        # A whole queue is resolved in one burst: only the first line of a burst
        # is worth the info level, the rest is debug.
        self._last_info_at: float = 0.0

    async def resolve(self, track_id: str) -> float:
        """The effective gain, in dB, to apply when transcoding
        ``track_id`` right now. Never raises — any failure degrades to
        0 dB (no adjustment) rather than blocking playback."""
        try:
            return await self._resolve(track_id)
        except Exception as e:
            logger.warning(f"Gain resolution failed for track {track_id}, using 0 dB: {e}")
            return 0.0

    async def _resolve(self, track_id: str) -> float:
        own = await self._get_one(track_id)
        if own is None:
            self._log(f"No gain data for track {track_id} — using 0 dB")
            return 0.0

        group_ids = await self._album_group(track_id)
        if group_ids is None:
            result = self._clamp(own.gain_db, own.peak)
            self._log(f"Track gain for {track_id}: {result:.2f} dB")
            return result

        infos = await self._get_many(group_ids)
        group_gain = self._weighted_average_gain(infos.values())
        result = self._clamp(group_gain, own.peak)
        self._log(
            f"Approximated album gain for track {track_id}: {result:.2f} dB "
            f"(unclamped {group_gain:.2f} dB, from {len(infos)} tracks sharing one album)"
        )
        return result

    def _log(self, message: str) -> None:
        now = time.monotonic()
        if now - self._last_info_at > _INFO_BURST_SECONDS:
            logger.info(message + " (the rest of this queue at debug level)")
        else:
            logger.debug(message)
        self._last_info_at = now

    async def _album_group(self, track_id: str) -> Optional[frozenset[str]]:
        """The set of track ids to average over, if ``track_id`` is
        currently part of a same-album queue — None if there's no queue,
        its membership isn't trustworthy yet (see QueueMembership), or
        its tracks don't all share one album."""
        if self._queue is None:
            return None
        track_ids = await self._queue.current_track_ids()
        if not track_ids or track_id not in track_ids or len(track_ids) < 2:
            return None

        infos = await self._get_many(track_ids)
        if len(infos) != len(track_ids):
            # Missing gain data for at least one queued track — don't
            # guess at album membership from a partial picture.
            return None
        album_ids = {info.album_id for info in infos.values() if info.album_id}
        if len(album_ids) != 1:
            return None
        return track_ids

    async def _get_one(self, track_id: str) -> Optional["TrackAudioInfo"]:
        infos = await self._get_many(frozenset((track_id,)))
        return infos.get(track_id)

    async def _get_many(self, track_ids: "Iterable[str]") -> dict[str, "TrackAudioInfo"]:
        track_ids = list(track_ids)
        missing = [t for t in track_ids if t not in self._cache]
        if missing:
            fetched = await self._api.get_tracks_audio_info(missing)
            self._cache.update(fetched)
        return {t: self._cache[t] for t in track_ids if t in self._cache}

    @staticmethod
    def _weighted_average_gain(infos: "Iterable[TrackAudioInfo]") -> float:
        infos = list(infos)
        total_duration = sum(i.duration_ms for i in infos)
        if total_duration <= 0:
            # No duration data at all (shouldn't normally happen) — an
            # unweighted mean is still better than picking one arbitrarily.
            return sum(i.gain_db for i in infos) / len(infos)
        return sum(i.gain_db * i.duration_ms for i in infos) / total_duration

    @staticmethod
    def _clamp(gain_db: float, peak: float) -> float:
        """Never let the chosen gain push this specific track's own peak
        sample over 0 dBFS, even when the gain came from an album-wide
        average computed from other tracks' louder or quieter mastering."""
        if peak <= 0:
            return gain_db
        ceiling_db = -20.0 * math.log10(peak)
        return min(gain_db, ceiling_db)


__all__ = ["QueueGainResolver"]

"""Reports play start/end to Qobuz so plays land in listening history.

Qobuz scrobbles to a linked Last.fm account server-side, but only for plays a
Connect device reports via track/reportStreamingStart and
track/reportStreamingEndJson. This class owns the lifecycle on top of those
raw API calls: one start per play, a played-duration on stop, and idempotency
so the player can call into it from several transition points without
double-counting.
"""

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

from ..auth.api_client import QobuzAPIClient

logger = logging.getLogger(__name__)


@dataclass
class _PlaySession:
    track_id: str
    format_id: int
    blob: str
    context_uuid: Optional[str]
    started_at_ms: int
    # Played-time accounting that excludes paused intervals: ``played_ms`` is
    # the time accumulated over finished play segments, and ``segment_started_ms``
    # marks when the current playing segment began (None while paused).
    played_ms: int = 0
    segment_started_ms: Optional[int] = None


class PlayReporter:
    """Tracks the active play and emits start/end reports to Qobuz."""

    def __init__(
        self,
        api_client: QobuzAPIClient,
        clock: Optional[Callable[[], int]] = None,
    ):
        self._api = api_client
        self._now_ms: Callable[[], int] = clock or (lambda: int(time.time() * 1000))
        self._active: Optional[_PlaySession] = None

    async def note_playing(
        self,
        *,
        track_id: str,
        format_id: int,
        blob: str,
        context_uuid: Optional[str],
        report_start: bool = True,
    ) -> None:
        """Mark that ``track_id`` is now playing.

        The reporter tracks a single active play. No-op if the same track is
        already active (redundant call / resume); if a different track is
        active, that play is ended first. The player decides when a same-track
        replay is a distinct play (it ends the prior play explicitly).

        ``report_start=False`` tracks the play locally but suppresses the
        reportStreamingStart call — used when we adopt a track mid-stream from
        the controlling app, which already reported (and scrobbled) it.
        """
        if self._active is not None:
            if self._active.track_id == track_id:
                # Same track already active: a resume if paused (restart the
                # played-time clock), otherwise a redundant call (no-op).
                if self._active.segment_started_ms is None:
                    self._active.segment_started_ms = self._now_ms()
                return
            await self._end_active()

        now = self._now_ms()
        self._active = _PlaySession(
            track_id=track_id,
            format_id=format_id,
            blob=blob,
            context_uuid=context_uuid,
            started_at_ms=now,
            segment_started_ms=now,
        )
        if report_start:
            await self._api.report_streaming_start(track_id=track_id, format_id=format_id)

    def note_paused(self) -> None:
        """Pause the played-time clock without ending the play.

        Keeps the session open (no streaming-end / scrobble on pause) but stops
        counting elapsed time, so a long pause is not reported as listening
        time. A subsequent note_playing for the same track resumes the clock.
        """
        session = self._active
        if session is not None and session.segment_started_ms is not None:
            session.played_ms += self._now_ms() - session.segment_started_ms
            session.segment_started_ms = None

    def update_context(self, *, track_id: str, context_uuid: Optional[str]) -> None:
        """Adopt a context UUID that arrived after the play started.

        The controller sometimes supplies the play context in a later SET_STATE
        than the one that began the play. Without this, the streaming-end report
        for the active play would carry a stale/None context. Only a real value
        for the currently-active track is applied, so a context-less resend
        cannot wipe a known context.
        """
        if (
            self._active is not None
            and self._active.track_id == track_id
            and context_uuid is not None
        ):
            self._active.context_uuid = context_uuid

    async def note_stopped(self) -> None:
        """Mark that playback stopped (pause/stop/track-end). Idempotent."""
        await self._end_active()

    async def _end_active(self) -> None:
        session = self._active
        if session is None:
            return
        self._active = None
        # Played time excludes paused intervals: finished segments plus the
        # current segment if still playing.
        played_ms = session.played_ms
        if session.segment_started_ms is not None:
            played_ms += self._now_ms() - session.segment_started_ms
        played_seconds = max(0, played_ms // 1000)
        await self._api.report_streaming_end(
            track_id=session.track_id,
            blob=session.blob,
            context_uuid=session.context_uuid,
            started_at_ms=session.started_at_ms,
            played_seconds=played_seconds,
        )

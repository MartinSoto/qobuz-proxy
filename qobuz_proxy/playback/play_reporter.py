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

        No-op if the same track is already the active play (redundant call);
        if a different track is active, that play is ended first.

        ``report_start=False`` tracks the play locally but suppresses the
        reportStreamingStart call — used when we adopt a track mid-stream from
        the controlling app, which already reported (and scrobbled) it.
        """
        if self._active is not None:
            if self._active.track_id == track_id:
                return
            await self._end_active()

        self._active = _PlaySession(
            track_id=track_id,
            format_id=format_id,
            blob=blob,
            context_uuid=context_uuid,
            started_at_ms=self._now_ms(),
        )
        if report_start:
            await self._api.report_streaming_start(track_id=track_id, format_id=format_id)

    async def note_stopped(self) -> None:
        """Mark that playback stopped (pause/stop/track-end). Idempotent."""
        await self._end_active()

    async def _end_active(self) -> None:
        session = self._active
        if session is None:
            return
        self._active = None
        played_seconds = max(0, (self._now_ms() - session.started_at_ms) // 1000)
        await self._api.report_streaming_end(
            track_id=session.track_id,
            blob=session.blob,
            context_uuid=session.context_uuid,
            started_at_ms=session.started_at_ms,
            played_seconds=played_seconds,
        )

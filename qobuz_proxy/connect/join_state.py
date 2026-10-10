"""What the renderer reports about itself when it (re)joins the session."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class JoinSnapshot:
    """Playback in progress at the moment of a (re)join.

    `playing_state` is the protocol value (2 playing, 3 paused); `queue_index`
    is the position of the current entry in the play order, if known.
    """

    playing_state: int
    position_ms: int
    duration_ms: int
    queue_index: Optional[int] = None

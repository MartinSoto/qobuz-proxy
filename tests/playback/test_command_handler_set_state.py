"""Integration tests for SET_STATE handling via PlaybackCommandHandler.

Covers the residual race called out in PR review: each SET_STATE message is
dispatched as its own task, so two overlapping SET_STATE sequences must not
interleave their load/seek/play steps. The handler now delegates the whole
sequence to player.apply_remote_state(), which applies it atomically.
"""

import functools
from unittest.mock import AsyncMock

from qobuz_proxy.backends import PlaybackState
from qobuz_proxy.playback.command_handler import MSG_TYPE_SET_STATE, PlaybackCommandHandler
from qobuz_proxy.proto import qconnect_payload_pb2 as pb

from tests.playback.test_player_serialization import _make_player


def _set_state_msg(
    *,
    track_id: int,
    queue_item_id: int,
    playing_state: int | None = 2,
    position_ms: int | None = None,
    context_uuid: bytes | None = None,
):
    """Build a server->renderer SET_STATE (type 41) protobuf message."""
    msg = pb.QConnectMessage()
    msg.messageType = 41
    st = msg.srvrRndrSetState
    if playing_state is not None:
        st.playingState = playing_state
    if position_ms is not None:
        st.currentPosition = position_ms
    st.currentQueueItem.queueItemId = queue_item_id
    st.currentQueueItem.trackId = track_id
    if context_uuid is not None:
        st.currentQueueItem.contextUuid = context_uuid
    return msg


class TestSetStateHandling:
    async def test_set_state_stores_queue_version(self) -> None:
        """Regression (test1.log, 2026-08-28): the server's queueVersion
        was never stored anywhere for a renderer session — it only ever
        updated via SRVR_CTRL_QUEUE_STATE/QUEUE_TRACKS_LOADED, which never
        actually arrive for a renderer session (see queue.py's module
        docstring) — so every outbound state report always echoed back
        0.0 regardless of what the server had just sent (observed
        directly: every SET_STATE carried queueVersion, e.g. 4.1, every
        report we sent back claimed 0.0). QueueVersion's own docstring
        says it exists "for synchronization" — a plausible reason a
        client-side sync check on the app's end would hold back further
        navigation commands."""
        from qobuz_proxy.playback.queue import QobuzQueue, QueueVersion

        player, backend = _make_player()
        queue = QobuzQueue()
        handler = PlaybackCommandHandler(player, queue=queue)

        msg = _set_state_msg(track_id=2001, queue_item_id=5)
        msg.srvrRndrSetState.queueVersion.major = 4
        msg.srvrRndrSetState.queueVersion.minor = 1

        await handler._handle_set_state(msg)

        assert await queue.get_version() == QueueVersion(major=4, minor=1)

    async def test_single_set_state_loads_and_plays(self) -> None:
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)

        await handler._handle_set_state(_set_state_msg(track_id=2001, queue_item_id=5))

        assert player.current_track is not None
        assert player.current_track.track_id == "2001"
        assert backend.played == ["2001"]
        assert player.state == PlaybackState.PLAYING

    async def test_set_state_propagates_context_uuid(self) -> None:
        """The currentQueueItem context UUID must reach the played track so
        the play report (listening history / scrobble) carries it."""
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)

        ctx = bytes(range(16))
        await handler._handle_set_state(
            _set_state_msg(track_id=2001, queue_item_id=5, context_uuid=ctx)
        )

        assert player.current_track is not None
        assert player.current_track.context_uuid == ctx

    async def test_next_item_context_preserved_on_contextless_resend(self) -> None:
        """A context-less resend of the same nextQueueItem must keep the context."""
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)
        ctx = bytes(range(16))

        first = _set_state_msg(track_id=1, queue_item_id=1)
        first.srvrRndrSetState.nextQueueItem.queueItemId = 2
        first.srvrRndrSetState.nextQueueItem.trackId = 1002
        first.srvrRndrSetState.nextQueueItem.contextUuid = ctx
        await handler._handle_set_state(first)
        assert handler.get_next_track_info()["contextUuid"] == ctx

        # Server resends the same next item without the optional contextUuid.
        second = _set_state_msg(track_id=1, queue_item_id=1)
        second.srvrRndrSetState.nextQueueItem.queueItemId = 2
        second.srvrRndrSetState.nextQueueItem.trackId = 1002
        await handler._handle_set_state(second)

        assert handler.get_next_track_info()["contextUuid"] == ctx

    async def test_overlapping_set_state_newest_wins(self) -> None:
        """Two SET_STATE messages arriving in quick succession — dispatched
        through the command queue the way speaker.py actually wires it
        (coalesce=True on MSG_TYPE_SET_STATE): their load/play steps must
        not interleave and the newer track must win — the exact path that
        previously left playback on a stale track."""
        player, backend = _make_player()
        await player.start()
        handler = PlaybackCommandHandler(player)

        older = _set_state_msg(track_id=1001, queue_item_id=1)
        newer = _set_state_msg(track_id=1002, queue_item_id=2)

        player.enqueue(
            functools.partial(handler.handle_message, MSG_TYPE_SET_STATE, older), coalesce=True
        )
        player.enqueue(
            functools.partial(handler.handle_message, MSG_TYPE_SET_STATE, newer), coalesce=True
        )
        await player._command_queue.join()
        await player.stop()

        # No interleaving of load/play across the two SET_STATE sequences.
        assert backend.max_active == 1
        # The newer SET_STATE wins as a whole — never left on the stale older track.
        assert player.current_track is not None
        assert player.current_track.track_id == "1002"
        assert backend.played[-1] == "1002"


class TestNoTrackSentinel:
    """The server signals "no current/next track" with an all-bits-set
    QueueTrackRef (fixed32 trackId, uint64 queueItemId all-1s) rather than
    omitting the field — HasField() alone doesn't distinguish this from a
    real track reference."""

    async def test_current_queue_item_sentinel_does_not_start_playback(self) -> None:
        # A freshly (re)connected renderer legitimately has an empty queue —
        # this is exactly the state a just-reset Sonos coordinator's Speaker
        # starts in, which is what first exposed this bug in practice.
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)

        msg = _set_state_msg(track_id=0xFFFFFFFF, queue_item_id=0xFFFFFFFFFFFFFFFF)
        await handler._handle_set_state(msg)

        assert player.current_track is None
        assert backend.played == []

    async def test_current_queue_item_sentinel_via_queue_item_id_alone(self) -> None:
        # A real (small) trackId paired with the queueItemId sentinel must
        # also be treated as "no track" — either field alone marks it.
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)

        msg = _set_state_msg(track_id=2001, queue_item_id=0xFFFFFFFFFFFFFFFF)
        await handler._handle_set_state(msg)

        assert player.current_track is None
        assert backend.played == []

    async def test_next_queue_item_sentinel_clears_stored_next_track(self) -> None:
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)

        real_next = _set_state_msg(track_id=1001, queue_item_id=1)
        real_next.srvrRndrSetState.nextQueueItem.queueItemId = 2
        real_next.srvrRndrSetState.nextQueueItem.trackId = 1002
        await handler._handle_set_state(real_next)
        assert handler.get_next_track_info() is not None

        sentinel_next = _set_state_msg(track_id=1001, queue_item_id=1)
        sentinel_next.srvrRndrSetState.nextQueueItem.queueItemId = 0xFFFFFFFFFFFFFFFF
        sentinel_next.srvrRndrSetState.nextQueueItem.trackId = 0xFFFFFFFF
        await handler._handle_set_state(sentinel_next)

        assert handler.get_next_track_info() is None

    async def test_next_queue_item_absent_from_message_does_not_clear_stored_next_track(
        self,
    ) -> None:
        """Regression (test1.log, 2026-08-28): only the explicit sentinel
        means "no next track" (see test above) — a SET_STATE that's just a
        position/state update (a seek, a restart-current-track tap) can
        legitimately omit nextQueueItem without meaning that. Treating
        plain field-absence as an implicit clear tore down a still-valid
        gapless arm on every such message, forcing a needless remove-then-
        re-add of the armed track on the device (observed directly:
        "Gapless: armed next track" logged again for the exact same track
        a few seconds later, with nothing between but an intervening seek)."""
        player, backend = _make_player()
        handler = PlaybackCommandHandler(player)
        on_changed = AsyncMock()
        handler.set_on_next_track_changed(on_changed)

        real_next = _set_state_msg(track_id=1001, queue_item_id=1)
        real_next.srvrRndrSetState.nextQueueItem.queueItemId = 2
        real_next.srvrRndrSetState.nextQueueItem.trackId = 1002
        await handler._handle_set_state(real_next)
        assert handler.get_next_track_info() is not None
        on_changed.reset_mock()  # only care about the seek-only message below

        # A follow-up SET_STATE (e.g. a seek/restart) that just doesn't
        # mention nextQueueItem at all — not the explicit sentinel.
        seek_only = _set_state_msg(track_id=1001, queue_item_id=1, position_ms=0)
        await handler._handle_set_state(seek_only)

        assert handler.get_next_track_info() is not None
        assert handler.get_next_track_info()["trackId"] == "1002"
        on_changed.assert_not_awaited()

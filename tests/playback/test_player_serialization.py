"""Tests that playback commands are serialized and superseded (latest-wins).

A track switch in the Qobuz app sends a burst of SET_STATE messages, which used
to fire overlapping load/play/stop calls and concurrent SOAP requests that wedge
DLNA renderers. The player now serializes commands through its command queue's
single consumer, and coalesce=True (see QobuzPlayer.enqueue()) lets a newer
command supersede an older one still waiting to run.
"""

import asyncio
import functools
from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends.base import AudioBackend
from qobuz_proxy.backends import BackendTrackMetadata, PlaybackState
from qobuz_proxy.playback.player import QobuzPlayer


class ConcurrencyTrackingBackend(AudioBackend):
    """Backend that records overlap and the order of played tracks."""

    def __init__(self) -> None:
        super().__init__(name="test")
        self.active = 0
        self.max_active = 0
        self.played: list[str] = []
        # Ordered log of lifecycle calls (disconnect/retarget) — used by
        # TestPlayerDetachAndRetarget to prove they never overlap play().
        self.events: list[str] = []

    async def play(self, url: str, metadata: BackendTrackMetadata) -> None:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            # Yield so any concurrent play() would be observed as overlap.
            await asyncio.sleep(0.01)
            self.played.append(metadata.track_id)
        finally:
            self.active -= 1

    async def pause(self) -> None: ...
    async def resume(self) -> bool:
        return True

    async def stop(self) -> None: ...
    async def seek(self, position_ms: int) -> None: ...
    async def get_position(self) -> int:
        return 0

    async def set_volume(self, level: int) -> None: ...
    async def get_volume(self) -> int:
        return 0

    async def get_state(self) -> PlaybackState:
        return self._state

    async def connect(self) -> bool:
        return True

    async def disconnect(self, send_device_stop: bool = True) -> None:
        self.events.append("disconnect")

    async def retarget(self, ip: str, port: int) -> bool:
        self.events.append("retarget")
        return True


def _make_player() -> tuple[QobuzPlayer, ConcurrencyTrackingBackend]:
    backend = ConcurrencyTrackingBackend()

    metadata = MagicMock()
    metadata.get_streaming_url = MagicMock(
        side_effect=lambda track_id: _coro(f"http://test/{track_id}")
    )
    metadata.get_metadata = MagicMock(side_effect=lambda track_id: _coro(None))
    metadata.get_track_actual_quality = MagicMock(return_value=None)
    # (actual_quality, sample_rate, bit_depth); 0s = cache miss, fall back to max quality.
    metadata.get_track_format = MagicMock(return_value=(0, 0, 0))
    metadata.log_now_playing_info = MagicMock()

    queue = MagicMock()
    queue.start = AsyncMock()
    queue.stop = AsyncMock()
    queue.set_current_by_item_id = AsyncMock(return_value=True)
    # Player routes track loading through queue.get_track_url/get_track_metadata
    # (see QobuzQueue's own implementation) rather than fetching directly —
    # mirror what metadata.get_streaming_url/get_metadata above provide.
    queue.get_track_url = MagicMock(
        side_effect=lambda track: _coro(f"http://test/{track.track_id}")
    )
    queue.get_track_metadata = MagicMock(side_effect=lambda track: _coro(None))
    player = QobuzPlayer(queue=queue, metadata_service=metadata, backend=backend)
    return player, backend


async def _coro(value):  # type: ignore[no-untyped-def]
    return value


class TestPositionedStart:
    async def test_positioned_start_reports_start_position(self) -> None:
        """Regression for BUG-18: starting at a position must not report 0:00
        to the app (progress bar snapped to zero until the next heartbeat)."""
        player, backend = _make_player()

        await player.play_track(queue_item_id=1, track_id="42", position_ms=60_000)

        assert player._position_value_ms == 60_000
        assert player.current_position_ms >= 60_000


class TestPlaybackSerialization:
    async def test_concurrent_play_track_never_overlaps(self) -> None:
        player, backend = _make_player()
        await player.start()

        # All enqueued synchronously, before the consumer gets a chance to
        # run any of them — coalesce=True (see enqueue()) drops every one
        # but the last before it ever starts.
        track_ids = [f"{i}" for i in range(1, 6)]
        for i, t in enumerate(track_ids):
            player.enqueue(
                functools.partial(player.play_track, queue_item_id=i, track_id=t),
                coalesce=True,
            )
        await player._command_queue.join()
        await player.stop()

        # The critical invariant: backend.play never ran concurrently.
        assert backend.max_active == 1
        # Latest-wins: the final state is the last requested track.
        assert player.current_track is not None
        assert player.current_track.track_id == track_ids[-1]
        assert backend.played[-1] == track_ids[-1]
        # Supersede dropped at least one intermediate request.
        assert len(backend.played) < len(track_ids)

    async def test_coalesced_command_is_skipped_without_running(self) -> None:
        """A coalesce=True command superseded by a newer one before it starts
        never runs at all — the consumer skips it entirely."""
        player, backend = _make_player()
        await player.start()

        # Block the consumer on an unrelated, already-running item so the
        # next two enqueues both land in the queue, neither started yet.
        blocker = asyncio.Event()
        player.enqueue(blocker.wait)
        await asyncio.sleep(0)  # let the consumer pick up the blocker

        player.enqueue(
            functools.partial(player.play_track, queue_item_id=0, track_id="stale"),
            coalesce=True,
        )
        player.enqueue(
            functools.partial(player.play_track, queue_item_id=1, track_id="fresh"),
            coalesce=True,
        )

        blocker.set()
        await player._command_queue.join()
        await player.stop()

        assert backend.played == ["fresh"]


class TestApplyRemoteStateSerialization:
    """A SET_STATE is load+seek+play applied as one atomic unit (apply_remote_state).

    These cover the residual race from the PR review: overlapping SET_STATE
    sequences must never interleave their load/play steps, so the newest one
    always wins as a whole and playback never ends up on a stale track.
    """

    async def test_concurrent_apply_remote_state_never_overlaps(self) -> None:
        player, backend = _make_player()
        await player.start()

        track_ids = [str(i) for i in range(1, 6)]
        for i, t in enumerate(track_ids):
            player.enqueue(
                functools.partial(
                    player.apply_remote_state,
                    track_id=t,
                    queue_item_id=i,
                    position_ms=0,
                    playing_state=2,
                ),
                coalesce=True,
            )
        await player._command_queue.join()
        await player.stop()

        # No interleaving of the load/play steps across SET_STATE sequences.
        assert backend.max_active == 1
        # Newest SET_STATE wins as a unit — never left on a stale track.
        assert player.current_track is not None
        assert player.current_track.track_id == track_ids[-1]
        assert backend.played[-1] == track_ids[-1]

    async def test_newer_set_state_wins_when_queued_behind_older(self) -> None:
        """Reproduce the reviewer's interleave: older sequence is in-flight
        (already running), a newer one queues behind it, and a third (newest)
        supersedes the queued one before it runs. The final track must be
        the newest, and the superseded one must never play."""
        player, backend = _make_player()
        await player.start()

        # Make "A" (the in-flight one) block partway through, so "B" and
        # "C" are guaranteed to still be queued — not started — when C's
        # enqueue coalesces B away.
        release_a = asyncio.Event()
        original_play = backend.play

        async def blocking_play(url, metadata):  # type: ignore[no-untyped-def]
            if metadata.track_id == "A":
                await release_a.wait()
            await original_play(url, metadata)

        backend.play = blocking_play  # type: ignore[method-assign]

        def _apply(track_id: str, queue_item_id: int):  # type: ignore[no-untyped-def]
            return functools.partial(
                player.apply_remote_state,
                track_id=track_id,
                queue_item_id=queue_item_id,
                position_ms=0,
                playing_state=2,
            )

        player.enqueue(_apply("A", 1), coalesce=True)
        await asyncio.sleep(0)  # let A start running and block on release_a

        player.enqueue(_apply("B", 2), coalesce=True)
        player.enqueue(_apply("C", 3), coalesce=True)

        release_a.set()
        await player._command_queue.join()
        await player.stop()

        assert backend.max_active == 1
        # B was superseded by C and must never have played.
        assert "B" not in backend.played
        # Final state is the newest request (C).
        assert player.current_track is not None
        assert player.current_track.track_id == "C"
        assert backend.played[-1] == "C"

    async def test_stale_in_flight_command_skips_device_work_once_superseded(self) -> None:
        """The "newer wins" guarantee above only covers commands still
        waiting to run. Once a command has actually started — e.g. a
        burst of rapid next/previous swipes outpacing how fast the
        renderer's own commands complete — coalescing alone doesn't stop
        it from finishing its own device round trip for a target the user
        has already swiped past by the time it gets there (observed
        directly: rapid swiping visibly landing on a track in the middle
        of the swipe instead of the final one). This checks the in-flight
        command's own cooperative bail-out instead (see
        Player._superseded_by_newer_command): "A" is still loading — not
        yet as far as backend.play() — when B and C arrive and coalesce
        down to just C, so A must never touch the device at all; only C
        should."""
        player, backend = _make_player()
        await player.start()

        # Make "A" block partway through *loading* (before it would ever
        # reach backend.play()), so B and C are guaranteed to already be
        # queued behind it by the time it resumes and checks whether it's
        # been superseded.
        release_a = asyncio.Event()
        original_get_track_url = player.queue.get_track_url

        async def blocking_get_track_url(track):  # type: ignore[no-untyped-def]
            if track.track_id == "A":
                await release_a.wait()
            return await original_get_track_url(track)

        player.queue.get_track_url = blocking_get_track_url  # type: ignore[method-assign]

        def _apply(track_id: str, queue_item_id: int):  # type: ignore[no-untyped-def]
            return functools.partial(
                player.apply_remote_state,
                track_id=track_id,
                queue_item_id=queue_item_id,
                position_ms=0,
                playing_state=2,
            )

        player.enqueue(_apply("A", 1), coalesce=True)
        await asyncio.sleep(0)  # let A start running and block on the load

        player.enqueue(_apply("B", 2), coalesce=True)
        player.enqueue(_apply("C", 3), coalesce=True)

        release_a.set()
        await player._command_queue.join()
        await player.stop()

        # A never reached the device at all — it caught its own
        # supersession before ever calling backend.play().
        assert "A" not in backend.played
        assert "B" not in backend.played
        assert player.current_track is not None
        assert player.current_track.track_id == "C"
        assert backend.played == ["C"]

    async def test_superseded_load_does_not_poison_the_next_commands_auto_continue(
        self,
    ) -> None:
        """Regression (test1.log, 2026-08-28): a command superseded
        mid-load (see the test above) bails out before ever calling
        _play_locked, but _load_track_locked has already dropped
        self._state to STOPPED as a normal part of loading — that's fine
        for the command that gets skipped, but the *next* command must
        not read that transient STOPPED as "nothing was playing" when
        deciding whether to auto-continue (see Player._intended_playing).
        Reproduces exactly what was observed: track A is playing, a rapid
        swipe to B gets superseded mid-load by a swipe to C, and both B
        and C's SET_STATE messages carry no playingState at all (the
        ordinary case — see test_load_only_track_change_while_playing_
        continues_playing) — C must still auto-continue playing, not sit
        loaded-and-silent waiting for a playingState that may not come."""
        player, backend = _make_player()
        await player.start()

        await player.apply_remote_state(
            track_id="A", queue_item_id=1, position_ms=0, playing_state=2
        )
        assert backend.played == ["A"]

        release_b = asyncio.Event()
        original_get_track_url = player.queue.get_track_url

        async def blocking_get_track_url(track):  # type: ignore[no-untyped-def]
            if track.track_id == "B":
                await release_b.wait()
            return await original_get_track_url(track)

        player.queue.get_track_url = blocking_get_track_url  # type: ignore[method-assign]

        def _apply_load_only(track_id: str, queue_item_id: int):  # type: ignore[no-untyped-def]
            # No playingState — the ordinary case for a rapid swipe.
            return functools.partial(
                player.apply_remote_state,
                track_id=track_id,
                queue_item_id=queue_item_id,
                position_ms=None,
                playing_state=None,
            )

        player.enqueue(_apply_load_only("B", 2), coalesce=True)
        await asyncio.sleep(0)  # let B start running and block on the load

        player.enqueue(_apply_load_only("C", 3), coalesce=True)

        release_b.set()
        await player._command_queue.join()
        await player.stop()

        assert "B" not in backend.played
        assert player.state == PlaybackState.PLAYING
        assert player.current_track is not None
        assert player.current_track.track_id == "C"
        assert backend.played[-1] == "C"


class TestBackendAttached:
    """Player.set_backend_attached() — see Speaker.detach()/retarget() and
    the command queue's own hold-while-detached behavior below."""

    async def test_going_detached_while_playing_freezes_to_loading(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING

        await player.set_backend_attached(False)

        assert player.state == PlaybackState.LOADING

    async def test_going_detached_relays_immediately(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING
        reporter = MagicMock()
        reporter.report_now = AsyncMock()
        player._state_reporter = reporter

        await player.set_backend_attached(False)

        reporter.report_now.assert_awaited_once()

    async def test_going_detached_while_not_playing_does_not_change_state(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PAUSED

        await player.set_backend_attached(False)

        assert player.state == PlaybackState.PAUSED

    async def test_reattaching_does_not_relay(self) -> None:
        player, backend = _make_player()
        reporter = MagicMock()
        reporter.report_now = AsyncMock()
        player._state_reporter = reporter

        await player.set_backend_attached(True)  # already attached by default

        reporter.report_now.assert_not_called()

    async def test_redundant_detach_does_not_relay_twice(self) -> None:
        player, backend = _make_player()
        player._state = PlaybackState.PLAYING
        reporter = MagicMock()
        reporter.report_now = AsyncMock()
        player._state_reporter = reporter

        await player.set_backend_attached(False)
        await player.set_backend_attached(False)  # already detached

        reporter.report_now.assert_awaited_once()

    async def test_reattaching_clears_stale_gapless_armed_state(self) -> None:
        """Regression: a retarget (Speaker.retarget()'s sole call site for
        attached=True) wipes the *backend's* own next-track bookkeeping
        (DLNABackend.retarget() — the physical device's queue can't be
        assumed to carry over what was armed on the old one), but nothing
        told Player._gapless_armed the same thing — left True, it
        permanently blocked the ordinary per-position-tick re-arm retry
        (_on_position_update), so a gapless transition the device carried
        through anyway on its own went undetected as gapless and read as
        an external takeover instead (observed directly, test1.log,
        2026-08-28: Cocina->Cuarto move)."""
        player, backend = _make_player()
        player._gapless_armed = True
        player._pending_next_track = {"trackId": "1", "queueItemId": 1}

        await player.set_backend_attached(True)

        assert player._gapless_armed is False
        assert player._pending_next_track is None

    async def test_going_detached_does_not_touch_gapless_armed_state(self) -> None:
        # No position ticks happen while detached (the backend's poll loop
        # is torn down along with the connection), so there's no risk of a
        # stale re-arm attempt to guard against here — only reattachment
        # needs to clear it.
        player, backend = _make_player()
        player._gapless_armed = True
        player._pending_next_track = {"trackId": "1", "queueItemId": 1}

        await player.set_backend_attached(False)

        assert player._gapless_armed is True
        assert player._pending_next_track == {"trackId": "1", "queueItemId": 1}


class TestCommandQueueHoldsWhileDetached:
    """The command queue holds a coalesce=True item at the front while the
    backend is detached (see Player.set_backend_attached), rather than
    dispatching into a per-call wait buried in the backend itself."""

    async def test_coalescable_command_holds_until_reattached(self) -> None:
        player, backend = _make_player()
        await player.start()
        await player.set_backend_attached(False)

        player.enqueue(
            functools.partial(player.play_track, queue_item_id=0, track_id="1"),
            coalesce=True,
        )
        await asyncio.sleep(0.05)  # comfortably less than the attach-wait bound
        assert backend.played == []  # still holding

        await player.set_backend_attached(True)
        await player._command_queue.join()
        await player.stop()

        assert backend.played == ["1"]

    async def test_coalescable_command_dispatches_anyway_once_the_wait_times_out(
        self,
    ) -> None:
        from unittest.mock import patch

        player, backend = _make_player()
        await player.start()
        await player.set_backend_attached(False)

        with patch("qobuz_proxy.playback.player._BACKEND_ATTACH_WAIT_SECONDS", 0.02):
            player.enqueue(
                functools.partial(player.play_track, queue_item_id=0, track_id="1"),
                coalesce=True,
            )
            await player._command_queue.join()

        await player.stop()

        assert backend.played == ["1"]

    async def test_non_coalescable_command_is_not_held_while_detached(self) -> None:
        player, backend = _make_player()
        await player.start()
        await player.set_backend_attached(False)

        ran = asyncio.Event()
        player.enqueue(ran.set)  # coalesce=False (default) — e.g. a volume command

        await player._command_queue.join()
        await player.stop()

        assert ran.is_set()


class TestPlayerDetachAndRetarget:
    """Player.detach()/retarget() — see Speaker.detach()/retarget(), the
    sole callers. Both now run as items on Player's own command queue
    instead of being driven directly from the Sonos discovery manager's
    own task, so they can never overlap a command already talking to the
    device — see docs/playback-concurrency.md, "Suggested order of work"
    step 2."""

    async def test_detach_disconnects_backend_and_marks_not_attached(self) -> None:
        player, backend = _make_player()

        await player.detach()

        assert backend.events == ["disconnect"]
        assert player._backend_attached is False

    async def test_retarget_success_marks_attached_and_clears_gapless(self) -> None:
        player, backend = _make_player()
        player._gapless_armed = True
        player._pending_next_track = {"trackId": "1", "queueItemId": 1}

        result = await player.retarget("10.0.1.31", 1400)

        assert result is True
        assert backend.events == ["retarget"]
        assert player._backend_attached is True
        assert player._gapless_armed is False

    async def test_failed_retarget_does_not_mark_attached(self) -> None:
        player, backend = _make_player()
        backend.retarget = AsyncMock(return_value=False)  # type: ignore[method-assign]
        await player.set_backend_attached(False)

        result = await player.retarget("10.0.1.31", 1400)

        assert result is False
        assert player._backend_attached is False

    async def test_detach_never_overlaps_an_in_flight_command(self) -> None:
        """Regression: Speaker.detach()/retarget() used to call the
        backend directly from the Sonos discovery manager's own task,
        able to run concurrently with whatever the command queue's
        consumer was in the middle of. Both now go through the same
        queue as every other command, so a detach requested mid-command
        waits its turn instead of racing it."""
        player, backend = _make_player()
        await player.start()

        # Deterministically park the consumer "in flight" on an earlier
        # command, the same pattern used above for coalescing tests.
        blocker = asyncio.Event()
        player.enqueue(blocker.wait)
        await asyncio.sleep(0)  # let the consumer pick up the blocker

        detach_task = asyncio.create_task(player.detach())
        await asyncio.sleep(0)  # let detach() enqueue and start waiting its turn
        assert backend.events == []  # still queued behind the blocker — not run yet

        blocker.set()
        await detach_task

        # Assert before stop() — stop() disconnects the backend again as
        # part of its own unconditional shutdown, which would otherwise
        # add a second "disconnect" unrelated to what this test covers.
        assert backend.events == ["disconnect"]
        await player.stop()

    async def test_retarget_never_overlaps_an_in_flight_command(self) -> None:
        player, backend = _make_player()
        await player.start()

        blocker = asyncio.Event()
        player.enqueue(blocker.wait)
        await asyncio.sleep(0)

        retarget_task = asyncio.create_task(player.retarget("10.0.1.31", 1400))
        await asyncio.sleep(0)
        assert backend.events == []

        blocker.set()
        assert await retarget_task is True

        # Assert before stop() — see the equivalent comment in
        # test_detach_never_overlaps_an_in_flight_command above.
        assert backend.events == ["retarget"]
        await player.stop()

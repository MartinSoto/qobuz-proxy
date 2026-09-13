"""Tests for QobuzPlayer.set_volume()'s same-value no-op guard, and for
volume commands being coalesced through the command queue.

Regression for a live-confirmed bug (2026-09-12): while this proxy's
Controller role used a second, synthetic deviceUuid distinct from its
Renderer role, a server confused about which identity owned the
connection echoed our own volume-change report straight back as a fresh
SET_VOLUME command — every report became another command to handle, a
self-sustaining loop (~4000 calls in 27s in one capture) that starved
the single command queue badly enough to also block natural-track-end
detection for the rest of that session. Joining the Controller role
under the Renderer's own deviceUuid fixed the identity confusion this
stemmed from, but a same-value command is always safe to no-op
regardless of cause — nothing is lost since nothing changes — so this
guard stays as a second, independent line of defense against the same
failure mode however it might recur.

That guard alone turned out not to be enough (2026-09-13, same
household, after the identity fix above): the recurring flood cycled
through several recently-seen values (e.g. 10, 11, 12, 13, repeating)
rather than repeating one — never two consecutive commands with the
same value, which is exactly what a same-value check can't catch, and
still enough to starve the single command queue. Volume commands are
now enqueued with coalesce=True under their own group
(VOLUME_COALESCE_GROUP, see speaker.py's registration and
QobuzPlayer.enqueue()) — a burst of any values, cycling or not, only
ever runs its last one, the same flood protection SET_STATE already
had for playback-directing commands.

Coalescing alone still turned out not to be enough (2026-09-13, later
the same day): a live volume spike still reached the physical speaker.
set_volume() had its own emergency backend-forwarding disable
(_VOLUME_BACKEND_DISABLED) as a second, independent safety net while
the actual trigger was tracked down — since replaced (2026-09-13, still
later the same day) by a hard safety ceiling, _VOLUME_HARD_CAP: rather
than stay disabled indefinitely, cap what's ever actually applied — to
the backend, our own bookkeeping, and the app-facing report — at a
known-safe value, so testing below it stays exactly as normal as
before instead of not working at all. Most tests below use volume
values under the cap, so it never engages; TestVolumeHardCap covers the
cap itself directly.
"""

import functools
from unittest.mock import AsyncMock

import qobuz_proxy.playback.player as player_module
from qobuz_proxy.playback.player import VOLUME_COALESCE_GROUP
from tests.playback.test_player_serialization import _make_player


class TestSetVolumeNoOp:
    async def test_same_value_is_a_noop(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 20

        result = await player.set_volume(20)

        assert result == 20
        backend.set_volume.assert_not_awaited()
        report_callback.assert_not_awaited()

    async def test_different_value_still_applies_and_reports(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 20
        player._is_active_renderer = True  # reporting is gated on this

        result = await player.set_volume(30)

        assert result == 30
        assert player._volume == 30
        backend.set_volume.assert_awaited_once_with(30)
        report_callback.assert_awaited_once_with(30)

    async def test_repeated_same_value_command_only_ever_applies_once(self) -> None:
        """Reproduces the actual failure shape: the same value arriving
        again and again (as an echoed command would) must not keep
        re-triggering backend calls or reports."""
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 20
        player._is_active_renderer = True  # reporting is gated on this

        await player.set_volume(23)
        for _ in range(50):
            await player.set_volume(23)

        backend.set_volume.assert_awaited_once_with(23)
        report_callback.assert_awaited_once_with(23)


class TestVolumeReportGatedOnActive:
    """Regression: a stray post-deactivation volume change (observed live —
    "Volume set to 72" 4ms after deactivation, see
    docs/connect-activation-model-hypotheses.md) must not be reported —
    the server has already told us we're not the active renderer and just
    rejects it. Bookkeeping/backend forwarding are unaffected; only the
    app-facing report is gated."""

    async def test_volume_change_while_not_active_is_not_reported(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 20
        player._is_active_renderer = False

        result = await player.set_volume(25)

        assert result == 25
        assert player._volume == 25  # bookkeeping still updates
        backend.set_volume.assert_awaited_once_with(25)  # device still gets it
        report_callback.assert_not_awaited()  # but the app isn't told


class TestVolumeCoalescing:
    """Reproduces the actual live failure shape a same-value check alone
    can't catch: a burst cycling through several *different* values
    rather than repeating one. Uses player.enqueue() directly, the same
    way speaker.py's registration does, rather than calling
    player.set_volume() straight — coalescing happens at the queue, not
    inside set_volume() itself."""

    async def test_rapid_multi_value_burst_only_applies_the_last_one(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        await player.start()

        for value in (10, 11, 12, 13) * 5:
            player.enqueue(
                functools.partial(player.set_volume, value),
                coalesce=True,
                coalesce_group=VOLUME_COALESCE_GROUP,
            )

        await player._command_queue.join()
        await player.stop()

        backend.set_volume.assert_awaited_once_with(13)
        assert player._volume == 13

    async def test_volume_command_does_not_supersede_a_pending_playback_command(
        self,
    ) -> None:
        """Cross-group isolation: an unrelated queued volume adjustment
        must never cancel a playback-directing command already waiting
        to run, or vice versa."""
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        await player.start()

        player.enqueue(
            functools.partial(player.play_track, queue_item_id=0, track_id="1"),
            coalesce=True,
        )
        player.enqueue(
            functools.partial(player.set_volume, 30),
            coalesce=True,
            coalesce_group=VOLUME_COALESCE_GROUP,
        )

        await player._command_queue.join()
        await player.stop()

        assert backend.played == ["1"]
        backend.set_volume.assert_awaited_once_with(30)


class TestVolumeHardCap:
    """_VOLUME_HARD_CAP — a request above it is ceilinged everywhere
    (backend, bookkeeping, app-facing report), not just refused, so a
    request for e.g. 90 ends up looking and behaving exactly like a
    request for the cap value. If this test needs updating, that's a
    deliberate signal the cap value itself is being changed; make sure
    that's really intended."""

    async def test_request_above_the_cap_is_ceilinged_everywhere(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._volume = 10
        player._is_active_renderer = True  # reporting is gated on this

        result = await player.set_volume(90)

        assert result == player_module._VOLUME_HARD_CAP
        assert player._volume == player_module._VOLUME_HARD_CAP
        backend.set_volume.assert_awaited_once_with(player_module._VOLUME_HARD_CAP)
        report_callback.assert_awaited_once_with(player_module._VOLUME_HARD_CAP)

    async def test_request_at_or_below_the_cap_is_unaffected(self) -> None:
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        player._volume = 0

        result = await player.set_volume(player_module._VOLUME_HARD_CAP - 1)

        assert result == player_module._VOLUME_HARD_CAP - 1
        backend.set_volume.assert_awaited_once_with(player_module._VOLUME_HARD_CAP - 1)

    async def test_a_flood_of_different_over_cap_values_collapses_to_one_backend_call(
        self,
    ) -> None:
        """The cap doubles as protection against a repeat of the
        echo/flood bug this exists alongside (docs/... see this module's
        own docstring): different over-cap values all clamp to the same
        capped value, so the existing same-value no-op guard collapses
        them after the first."""
        player, backend = _make_player()
        backend.set_volume = AsyncMock()  # type: ignore[method-assign]
        player._volume = 0

        for value in (40, 41, 42, 43):
            await player.set_volume(value)

        backend.set_volume.assert_awaited_once_with(player_module._VOLUME_HARD_CAP)


class TestOnVolumeUpdate:
    """Player._on_volume_update() — the backend's callback for a volume
    change it didn't itself cause (see DLNABackend._poll_state_loop's
    external-volume-drift detection). Regression for a real, pre-existing
    gap (2026-09-13): changing volume directly on the physical device was
    never noticed or reported to the app at all."""

    async def test_updates_bookkeeping_synchronously(self) -> None:
        player, _backend = _make_player()
        player._volume = 20

        player._on_volume_update(35)

        assert player._volume == 35  # updated immediately, before the queue even runs

    async def test_reports_the_change_once_the_queue_runs(self) -> None:
        player, _backend = _make_player()
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._is_active_renderer = True
        await player.start()

        player._on_volume_update(35)
        await player._command_queue.join()
        await player.stop()

        report_callback.assert_awaited_once_with(35)

    async def test_not_reported_while_not_the_active_renderer(self) -> None:
        """Same gate _report_volume_change already has for every other
        caller — see docs/connect-activation-model-hypotheses.md."""
        player, _backend = _make_player()
        report_callback = AsyncMock()
        player.set_volume_report_callback(report_callback)
        player._is_active_renderer = False
        await player.start()

        player._on_volume_update(35)
        await player._command_queue.join()
        await player.stop()

        report_callback.assert_not_awaited()

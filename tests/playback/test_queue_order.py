"""Tests for QobuzQueue's ordered view (QueueOrder): snapshots set it, deltas
only mark it stale, shuffle is applied from the server's index list.

The index lists and ids below are the real ones from the live capture in
docs/queue-and-shuffle-wire-observations.md (O16, O19), with synthetic track
ids, so the replay tests check what the server really did.
"""

import logging

import pytest

from qobuz_proxy.playback.queue import QobuzQueue, QueueEntry, QueueVersion


def _items(first_id: int, count: int) -> list[tuple[int, str]]:
    return [(first_id + i, f"T{first_id + i}") for i in range(count)]


def _ids(order: list[QueueEntry] | None) -> list[int] | None:
    return None if order is None else [e.queue_item_id for e in order]


@pytest.fixture
def queue() -> QobuzQueue:
    return QobuzQueue()


class TestSnapshot:
    async def test_no_snapshot_means_no_order(self, queue: QobuzQueue) -> None:
        assert await queue.play_order() is None

    async def test_unshuffled_snapshot_is_the_original_order(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(164, 4))

        assert _ids(await queue.play_order()) == [164, 165, 166, 167]
        assert await queue.shuffle_enabled() is False

    async def test_shuffled_snapshot_applies_the_server_indexes(self, queue: QobuzQueue) -> None:
        """O19, 13:30:48: ids 255-265, shuffle on."""
        await queue.apply_snapshot(
            _items(255, 11),
            shuffle_mode=True,
            shuffled_indexes=[0, 7, 1, 4, 6, 10, 5, 9, 8, 3, 2],
        )

        assert _ids(await queue.play_order()) == [
            255, 262, 256, 259, 261, 265, 260, 264, 263, 258, 257,
        ]  # fmt: skip
        assert await queue.shuffle_enabled() is True

    async def test_entries_carry_track_ids(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot([(5, "A"), (6, "B")], shuffle_mode=True, shuffled_indexes=[1, 0])

        assert await queue.play_order() == [QueueEntry(6, "B"), QueueEntry(5, "A")]

    async def test_invalid_indexes_fall_back_to_the_original_order(
        self, queue: QobuzQueue, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A wrong order must never be acted on."""
        with caplog.at_level(logging.WARNING):
            await queue.apply_snapshot(_items(1, 3), shuffle_mode=True, shuffled_indexes=[0, 0, 2])

        assert _ids(await queue.play_order()) == [1, 2, 3]
        assert await queue.shuffle_enabled() is False
        assert "invalid shuffledTrackIndexes" in caplog.text

    async def test_shuffle_without_indexes_falls_back_with_a_warning(
        self, queue: QobuzQueue, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            await queue.apply_snapshot(_items(1, 3), shuffle_mode=True)

        assert _ids(await queue.play_order()) == [1, 2, 3]
        assert await queue.shuffle_enabled() is False
        assert "no usable shuffled order" in caplog.text

    async def test_indexes_ignored_when_shuffle_is_off(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 3), shuffle_mode=False, shuffled_indexes=[2, 1, 0])

        assert _ids(await queue.play_order()) == [1, 2, 3]

    async def test_a_snapshot_also_updates_membership(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 2))

        assert await queue.current_track_ids() == frozenset({"T1", "T2"})

    async def test_replace_queue_tracks_is_an_unshuffled_snapshot(self, queue: QobuzQueue) -> None:
        await queue.replace_queue_tracks(_items(1, 2))

        assert _ids(await queue.play_order()) == [1, 2]


class TestDeltasMarkTheOrderStale:
    async def test_add_makes_the_order_stale_but_keeps_membership(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 2))

        await queue.add_queue_tracks([(3, "T3")])

        assert await queue.play_order() is None
        assert await queue.current_track_ids() == frozenset({"T1", "T2", "T3"})

    async def test_remove_makes_the_order_stale(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 3))

        await queue.remove_queue_tracks([2])

        assert await queue.play_order() is None
        assert await queue.current_track_ids() == frozenset({"T1", "T3"})

    async def test_reorder_and_shuffle_toggle_make_the_order_stale(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 3))

        await queue.mark_order_stale()

        assert await queue.play_order() is None

    async def test_the_next_snapshot_makes_it_fresh_again(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 3))
        await queue.remove_queue_tracks([2])

        await queue.apply_snapshot([(1, "T1"), (3, "T3")])

        assert _ids(await queue.play_order()) == [1, 3]

    async def test_cleared_queue_is_a_known_empty_order(self, queue: QobuzQueue) -> None:
        await queue.apply_snapshot(_items(1, 3))

        await queue.clear_queue_tracks()

        assert await queue.play_order() == []

    async def test_version_going_backward_discards_the_order(self, queue: QobuzQueue) -> None:
        await queue.set_version(QueueVersion(major=8, minor=1))
        await queue.apply_snapshot(_items(1, 3))

        await queue.set_version(QueueVersion(major=1, minor=1))

        assert await queue.play_order() is None

    async def test_a_minor_bump_keeps_the_order(self, queue: QobuzQueue) -> None:
        await queue.set_version(QueueVersion(major=8, minor=1))
        await queue.apply_snapshot(_items(1, 3))

        await queue.set_version(QueueVersion(major=8, minor=2))

        assert _ids(await queue.play_order()) == [1, 2, 3]


class TestReplayOfShuffleWithEdits:
    """O19: shuffle on, then add, play-next insert, remove, shuffle off — the
    snapshot after each step is the truth; the play order must be exactly what
    the server's next pointers showed."""

    async def test_sequence(self, queue: QobuzQueue) -> None:
        # 13:30:48 shuffle on (v21.2), current = 262
        await queue.apply_snapshot(
            _items(255, 11),
            shuffle_mode=True,
            shuffled_indexes=[0, 7, 1, 4, 6, 10, 5, 9, 8, 3, 2],
        )
        # 13:31:11 add 266 (v21.3): appended to both lists
        await queue.add_queue_tracks([(266, "T266")])
        assert await queue.play_order() is None  # stale until the snapshot arrives
        await queue.apply_snapshot(
            _items(255, 12),
            shuffle_mode=True,
            shuffled_indexes=[0, 7, 1, 4, 6, 10, 5, 9, 8, 3, 2, 11],
        )
        order = _ids(await queue.play_order())
        assert order is not None and order[-1] == 266

        # 13:31:19 play next 267 after 262 (v21.4): the upcoming part was re-arranged
        await queue.add_queue_tracks([(267, "T267")])
        original = [255, 256, 257, 258, 259, 260, 261, 262, 267, 263, 264, 265, 266]
        await queue.apply_snapshot(
            [(i, f"T{i}") for i in original],
            shuffle_mode=True,
            shuffled_indexes=[0, 7, 12, 1, 4, 6, 10, 5, 9, 8, 3, 2, 11],
        )
        assert _ids(await queue.play_order()) == [
            255, 262, 266, 256, 259, 261, 264, 260, 263, 267, 258, 257, 265,
        ]  # fmt: skip

        # 13:31:31 remove 266 (v21.6): dropped from both, indexes renumbered
        await queue.remove_queue_tracks([266])
        original = [255, 256, 257, 258, 259, 260, 261, 262, 267, 263, 264, 265]
        await queue.apply_snapshot(
            [(i, f"T{i}") for i in original],
            shuffle_mode=True,
            shuffled_indexes=[0, 7, 1, 4, 6, 10, 5, 9, 8, 3, 2, 11],
        )
        assert _ids(await queue.play_order()) == [
            255, 262, 256, 259, 261, 264, 260, 263, 267, 258, 257, 265,
        ]  # fmt: skip

        # 13:32:00 shuffle off (v21.13): original order back, 266/256 gone
        original = [255, 257, 258, 259, 260, 261, 262, 267, 263, 264, 265]
        await queue.apply_snapshot([(i, f"T{i}") for i in original])
        assert _ids(await queue.play_order()) == original
        assert await queue.shuffle_enabled() is False


class TestAutoplayMode:
    async def test_unknown_until_announced(self, queue: QobuzQueue) -> None:
        assert await queue.autoplay_mode() is None

    async def test_last_announcement_wins(self, queue: QobuzQueue) -> None:
        await queue.set_autoplay_mode(True)
        await queue.set_autoplay_mode(False)

        assert await queue.autoplay_mode() is False

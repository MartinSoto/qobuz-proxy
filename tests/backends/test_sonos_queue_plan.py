"""The queue planner (sonos/queue_plan.py) and entry identity (audio_url.py).

apply_ops encodes what a real Sonos One was measured to do (design doc 9.2 and
the reorder/insert/remove probes of 2026-10-10); plan_reconcile is checked
against it, case by case and on a lot of random edits.
"""

import random

import pytest

from qobuz_proxy.backends.dlna.sonos.audio_url import EntryKey, parse_entry_url
from qobuz_proxy.backends.dlna.sonos.queue_plan import (
    INSERT_BATCH,
    InsertRun,
    MoveBlock,
    PlanError,
    RemoveRange,
    apply_ops,
    plan_cost,
    plan_reconcile,
)


def K(*items: int) -> list[EntryKey]:
    return [EntryKey(f"T{i}", i) for i in items]


def ids(keys: list[EntryKey]) -> list[int]:
    return [k.queue_item_id for k in keys]


class TestParseEntryUrl:
    def test_reads_track_item_and_generation(self) -> None:
        url = "http://192.168.1.5:7120/audio/449427998?format=wav&depth=24&rate=48000&item=449427998_9&s=ab12cd34&v=5&i=9"

        parsed = parse_entry_url(url)

        assert parsed is not None
        assert parsed.key == EntryKey("449427998", 9)
        assert (parsed.session, parsed.major) == ("ab12cd34", 5)

    def test_a_url_without_a_generation_still_has_an_identity(self) -> None:
        parsed = parse_entry_url("http://h:7120/audio/42?format=flac&i=3")

        assert parsed is not None and parsed.key == EntryKey("42", 3)
        assert parsed.session is None and parsed.major is None

    @pytest.mark.parametrize(
        "url",
        [
            "http://h:7120/audio/42?format=flac&item=42",  # from before item ids were in the URL
            "http://h:7120/other/42?i=3",
            "http://h:7120/audio/?i=3",
            "http://h:7120/audio/42?i=notanumber",
            "x-sonos-spotify:spotify%3atrack%3a123",
            "",
        ],
    )
    def test_anything_else_is_not_an_entry_of_ours(self, url: str) -> None:
        assert parse_entry_url(url) is None


class TestApplyOpsMatchesTheDevice:
    """Each expectation was observed on a Sonos One."""

    def test_move_one_forward_lands_before_the_original_target(self) -> None:
        assert ids(apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(1, 1, 4)])) == [2, 3, 1, 4, 5]

    def test_move_to_the_end(self) -> None:
        assert ids(apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(1, 1, 6)])) == [2, 3, 4, 5, 1]

    def test_move_a_block_to_the_front(self) -> None:
        assert ids(apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(2, 2, 1)])) == [2, 3, 1, 4, 5]

    def test_move_a_block_forward(self) -> None:
        assert ids(apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(1, 2, 5)])) == [3, 4, 1, 2, 5]

    def test_a_target_inside_or_next_to_the_block_changes_nothing(self) -> None:
        for insert_before in (2, 3):  # block is position 2 only
            assert ids(apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(2, 1, insert_before)])) == [
                1, 2, 3, 4, 5,
            ]  # fmt: skip
        assert ids(apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(1, 3, 2)])) == [1, 2, 3, 4, 5]

    def test_move_beyond_the_end_and_empty_blocks_are_refused(self) -> None:
        with pytest.raises(PlanError):
            apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(1, 1, 7)])
        with pytest.raises(PlanError):
            apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(1, 0, 3)])
        with pytest.raises(PlanError):
            apply_ops(K(1, 2, 3, 4, 5), [MoveBlock(9, 1, 1)])

    def test_insert_positions(self) -> None:
        base = K(1, 2, 3)
        new = tuple(K(90))
        assert ids(apply_ops(base, [InsertRun(1, new)])) == [90, 1, 2, 3]
        assert ids(apply_ops(base, [InsertRun(2, new)])) == [1, 90, 2, 3]
        assert ids(apply_ops(base, [InsertRun(4, new)])) == [1, 2, 3, 90]
        assert ids(apply_ops(base, [InsertRun(9, new)])) == [1, 2, 3, 90]  # past the end appends

    def test_remove_range_must_fit_exactly(self) -> None:
        assert ids(apply_ops(K(1, 2, 3, 4, 5, 6), [RemoveRange(2, 2)])) == [1, 4, 5, 6]
        assert apply_ops(K(1, 2, 3), [RemoveRange(1, 3)]) == []
        for bad in (RemoveRange(0, 1), RemoveRange(5, 10), RemoveRange(9, 1), RemoveRange(2, 0)):
            with pytest.raises(PlanError):
                apply_ops(K(1, 2, 3, 4, 5, 6), [bad])


class TestPlanReconcile:
    def test_identical_queues_need_nothing(self) -> None:
        assert plan_reconcile(K(1, 2, 3), K(1, 2, 3), anchor=K(2)[0]) == []

    def test_a_removal_is_one_range(self) -> None:
        ops = plan_reconcile(K(1, 2, 3, 4, 5), K(1, 2, 5), anchor=K(1)[0])

        assert ops == [RemoveRange(3, 2)]

    def test_separate_removals_go_last_first(self) -> None:
        ops = plan_reconcile(K(1, 2, 3, 4, 5, 6), K(2, 3, 5), anchor=K(2)[0])

        assert ops == [RemoveRange(6, 1), RemoveRange(4, 1), RemoveRange(1, 1)]

    def test_an_append_is_one_insert(self) -> None:
        ops = plan_reconcile(K(1, 2, 3), K(1, 2, 3, 4), anchor=K(1)[0])

        assert ops == [InsertRun(4, tuple(K(4)))]

    def test_play_next_inserts_right_after_the_playing_entry(self) -> None:
        ops = plan_reconcile(K(1, 2, 3), K(1, 9, 2, 3), anchor=K(1)[0])

        assert ops == [InsertRun(2, tuple(K(9)))]

    def test_a_dragged_entry_is_one_move_and_the_anchor_stays(self) -> None:
        # drag 5 up to right after the playing entry 1
        ops = plan_reconcile(K(1, 2, 3, 4, 5), K(1, 5, 2, 3, 4), anchor=K(1)[0])

        assert ops == [MoveBlock(start=5, count=1, insert_before=2)]

    def test_a_missing_anchor_means_the_queue_has_to_be_reloaded(self) -> None:
        assert plan_reconcile(K(1, 2, 3), K(4, 5), anchor=K(1)[0]) is None
        assert plan_reconcile(K(1, 2, 3), K(1, 2), anchor=K(3)[0]) is None
        assert plan_reconcile(K(1, 2, 3), K(4, 1, 2, 3), anchor=K(9)[0]) is None

    def test_without_an_anchor_anything_may_move(self) -> None:
        ops = plan_reconcile(K(1, 2, 3), K(3, 2, 1), anchor=None)

        assert ops is not None
        assert apply_ops(K(1, 2, 3), ops) == K(3, 2, 1)

    def test_a_fresh_shuffle_rebuilds_around_the_playing_entry(self) -> None:
        current = K(*range(1, 41))
        desired = K(*random.Random(1).sample(range(1, 41), 40))
        anchor = desired[0]

        ops = plan_reconcile(current, desired, anchor=anchor)

        assert ops is not None
        assert apply_ops(current, ops) == desired
        # a rebuild: two removals and the insert runs, a handful of calls
        assert plan_cost(ops) <= 2 + 2 * -(-40 // INSERT_BATCH)

    def test_the_whole_replacement_is_planned_with_no_anchor(self) -> None:
        ops = plan_reconcile(K(1, 2, 3), K(7, 8), anchor=None)

        assert ops is not None and apply_ops(K(1, 2, 3), ops) == K(7, 8)

    def test_an_empty_desired_queue_clears(self) -> None:
        assert plan_reconcile(K(1, 2), [], anchor=None) == [RemoveRange(1, 2)]


def _random_case(rng: random.Random) -> tuple[list[EntryKey], list[EntryKey], EntryKey | None]:
    size = rng.randint(1, 60)
    current_ids = list(range(1, size + 1))
    rng.shuffle(current_ids)
    desired_ids = [i for i in current_ids if rng.random() > 0.2]  # some removals
    for new_id in range(1000, 1000 + rng.randint(0, 12)):  # some insertions
        desired_ids.insert(rng.randint(0, len(desired_ids)), new_id)
    mode = rng.random()
    if mode < 0.3:
        rng.shuffle(desired_ids)  # a fresh shuffle
    elif mode < 0.7 and len(desired_ids) > 1:  # a few drags
        for _ in range(rng.randint(1, 4)):
            item = desired_ids.pop(rng.randrange(len(desired_ids)))
            desired_ids.insert(rng.randint(0, len(desired_ids)), item)
    both = [i for i in current_ids if i in desired_ids]
    anchor = (
        EntryKey(f"T{rng.choice(both)}", rng.choice(both)) if both and rng.random() < 0.85 else None
    )
    if anchor is not None:
        anchor = EntryKey(f"T{anchor.queue_item_id}", anchor.queue_item_id)
    return K(*current_ids), K(*desired_ids), anchor


class TestPlanReconcileOnRandomEdits:
    def test_the_plan_always_reaches_the_desired_queue(self) -> None:
        rng = random.Random(20261010)
        for _ in range(1500):
            current, desired, anchor = _random_case(rng)

            ops = plan_reconcile(current, desired, anchor)

            assert ops is not None, (current, desired, anchor)
            assert apply_ops(current, ops) == desired, (current, desired, anchor, ops)

    def test_the_playing_entry_is_never_removed_or_moved(self) -> None:
        rng = random.Random(7)
        for _ in range(1500):
            current, desired, anchor = _random_case(rng)
            if anchor is None:
                continue
            queue = list(current)

            ops = plan_reconcile(current, desired, anchor)

            assert ops is not None
            for op in ops:
                where = queue.index(anchor) + 1
                if isinstance(op, RemoveRange):
                    assert not op.start <= where < op.start + op.count, (op, current, desired)
                elif isinstance(op, MoveBlock):
                    assert not op.start <= where < op.start + op.count, (op, current, desired)
                queue = apply_ops(queue, [op])

    def test_a_plan_never_costs_more_than_a_rebuild(self) -> None:
        """A rebuild is two removals plus the insert calls; the planner takes the
        cheaper of that and the incremental plan."""
        rng = random.Random(99)
        for _ in range(500):
            current, desired, anchor = _random_case(rng)

            ops = plan_reconcile(current, desired, anchor)

            assert ops is not None
            bound = 2 + 2 * -(-len(desired) // INSERT_BATCH)  # one run per side of the anchor
            assert plan_cost(ops) <= bound, (len(current), len(desired), plan_cost(ops))

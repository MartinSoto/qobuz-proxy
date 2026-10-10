"""A queue "generation" is (session tag, major version): what a stream URL must
match to belong to the current queue (see QobuzQueue.generation and
docs/sonos-queue-handover-design.md, 5.2)."""

from qobuz_proxy.playback.queue import QobuzQueue, QueueVersion, session_tag


class TestSessionTag:
    def test_is_short_and_url_safe(self) -> None:
        tag = session_tag("e626ec9a-1c4b-4cde-9a11-0123456789ab")

        assert len(tag) == 8
        assert all(c in "0123456789abcdef" for c in tag)

    def test_is_stable_for_a_session_and_differs_between_sessions(self) -> None:
        assert session_tag("session-a") == session_tag("session-a")
        assert session_tag("session-a") != session_tag("session-b")

    def test_unknown_session_has_no_tag(self) -> None:
        assert session_tag("") == ""


class TestGeneration:
    async def test_starts_unknown(self) -> None:
        assert QobuzQueue().generation == ("", 0)

    async def test_follows_the_session_and_the_major_version(self) -> None:
        queue = QobuzQueue()

        queue.set_session("session-a")
        await queue.set_version(QueueVersion(major=7, minor=3))

        assert queue.generation == (session_tag("session-a"), 7)

    async def test_a_minor_bump_keeps_the_generation(self) -> None:
        """Shuffle, edits and autoplay reloads must not make entries stale."""
        queue = QobuzQueue()
        queue.set_session("session-a")
        await queue.set_version(QueueVersion(major=7, minor=1))
        before = queue.generation

        await queue.set_version(QueueVersion(major=7, minor=9))

        assert queue.generation == before

    async def test_a_new_load_changes_the_generation(self) -> None:
        queue = QobuzQueue()
        queue.set_session("session-a")
        await queue.set_version(QueueVersion(major=7, minor=1))
        before = queue.generation

        await queue.set_version(QueueVersion(major=8, minor=1))

        assert queue.generation != before

    async def test_a_new_session_changes_the_generation(self) -> None:
        queue = QobuzQueue()
        queue.set_session("session-a")
        await queue.set_version(QueueVersion(major=7, minor=1))
        before = queue.generation

        queue.set_session("session-b")

        assert queue.generation != before


class TestRewrittenQueueKeepsItsEntries:
    """A client re-sending the very same queue moves the major version on
    without changing anything: what was published for the earlier version is
    still right, so its URLs stay valid. A queue that really differs makes the
    earlier ones stale, as ever."""

    async def _queue(self):  # type: ignore[no-untyped-def]
        queue = QobuzQueue()
        queue.set_session("session-a")
        await queue.set_version(QueueVersion(5, 1))
        await queue.apply_snapshot([(0, "10"), (1, "11"), (2, "12")])
        return queue

    async def test_the_same_queue_re_sent_under_a_new_major_keeps_the_old_one_valid(self) -> None:
        queue = await self._queue()
        tag = queue.generation[0]

        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(0, "10"), (1, "11"), (2, "12")])

        assert queue.same_generation(tag, 6)
        assert queue.same_generation(tag, 5)  # re-sent unchanged

    async def test_a_chain_of_identical_re_sends_keeps_every_earlier_one(self) -> None:
        queue = await self._queue()
        tag = queue.generation[0]
        for major in (6, 7, 8):
            await queue.set_version(QueueVersion(major, 1))
            await queue.apply_snapshot([(0, "10"), (1, "11"), (2, "12")])

        assert all(queue.same_generation(tag, m) for m in (5, 6, 7, 8))
        assert not queue.same_generation(tag, 4)

    async def test_a_different_queue_makes_the_earlier_ones_stale(self) -> None:
        queue = await self._queue()
        tag = queue.generation[0]

        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(0, "10"), (1, "99"), (2, "12")])

        assert queue.same_generation(tag, 6)
        assert not queue.same_generation(tag, 5)

    async def test_the_same_tracks_under_other_item_ids_is_a_different_queue(self) -> None:
        queue = await self._queue()
        tag = queue.generation[0]

        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(3, "10"), (4, "11"), (5, "12")])

        assert not queue.same_generation(tag, 5)

    async def test_a_different_queue_after_identical_ones_drops_all_the_earlier(self) -> None:
        queue = await self._queue()
        tag = queue.generation[0]
        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(0, "10"), (1, "11"), (2, "12")])

        await queue.set_version(QueueVersion(7, 1))
        await queue.apply_snapshot([(0, "20")])

        assert queue.same_generation(tag, 7)
        assert not queue.same_generation(tag, 6) and not queue.same_generation(tag, 5)

    async def test_another_session_is_never_the_same(self) -> None:
        queue = await self._queue()
        await queue.set_version(QueueVersion(6, 1))
        await queue.apply_snapshot([(0, "10"), (1, "11"), (2, "12")])
        old_tag = queue.generation[0]

        queue.set_session("session-b")

        assert not queue.same_generation(old_tag, 6)
        assert not queue.same_generation(queue.generation[0], 5)

    async def test_a_minor_bump_is_no_new_major(self) -> None:
        queue = await self._queue()
        tag = queue.generation[0]

        await queue.set_version(QueueVersion(5, 2))

        assert queue.same_generation(tag, 5)

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

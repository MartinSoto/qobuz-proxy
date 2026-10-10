"""The queue item id must reach the backend: a backend that publishes stream
URLs tags them with it (see SonosAudioProxyServer), so the entry can later be
matched to a queue item."""

from unittest.mock import AsyncMock, MagicMock

from qobuz_proxy.backends import PlaybackState

from tests.playback.test_player import _make_player


class TestQueueItemIdReachesTheBackend:
    async def test_current_track_metadata_carries_it(self) -> None:
        player, backend = _make_player()
        backend.play = AsyncMock()
        backend.stop = AsyncMock()
        backend.seek = AsyncMock()
        reporter = MagicMock()
        reporter.note_playing = AsyncMock()
        reporter.note_stopped = AsyncMock()
        player._play_reporter = reporter

        await player.apply_remote_state(
            track_id="222",
            queue_item_id=9,
            position_ms=0,
            playing_state=2,  # PLAYING
            context_uuid=None,
        )

        backend.play.assert_awaited_once()
        assert backend.play.await_args.args[0].queue_item_id == 9

    async def test_next_track_metadata_carries_it(self) -> None:
        player, backend = _make_player(next_track_info={"trackId": "333", "queueItemId": 11})
        player._state = PlaybackState.PLAYING

        await player._prepare_next_track_for_gapless()

        backend.set_next_track.assert_awaited_once()
        metadata, queue_item_id = backend.set_next_track.await_args.args
        assert metadata.queue_item_id == 11
        assert queue_item_id == 11

"""SonosBackend that hands the whole Qobuz queue to the device (queue handover).

The plain SonosBackend sequences the queue itself: the device holds the playing
track and the armed next one, and every transition goes through the player.
This one publishes the *whole* queue to the device's own queue instead
(SonosQueueSync, in the server's play order, shuffled or not) and lets the
device play and advance through it on its own; what is left for this class is
to start playback at the right entry, keep the device's copy in step with the
Qobuz queue (`queue_changed`), and notice which entry the device is playing so
the player can report it (`on_item_changed`). Design and evidence:
docs/sonos-queue-handover-design.md.

Gapless arming is switched off (the next entry is simply the next one in the
device queue), which also retires the bookkeeping that went with it. Everything
else — transport commands, volume, hijack detection — is the inherited backend.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

from qobuz_proxy.backends.types import PlayResult

from ..proxy_server import AudioProxyServerProtocol
from .audio_url import EntryKey, parse_entry_url
from .backend import SonosBackend
from .client import SonosClient
from .entry_builder import ProxyEntryBuilder
from .proxy_server import SonosAudioProxyServer
from .queue_sync import SonosQueueSync
from .queue_sync_runner import QueueSyncRunner

if TYPE_CHECKING:
    from qobuz_proxy.playback.queue import QobuzQueue
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver

logger = logging.getLogger(__name__)

# How long starting playback may take, publishing the first batch included.
START_TIMEOUT_SECONDS = 30.0


class SonosQueueBackend(SonosBackend):
    _poll_uri_every_cycle = True

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._queue: Optional["QobuzQueue"] = None
        self._resolver: Optional["QobuzStreamResolver"] = None
        self._sync: Optional[SonosQueueSync] = None
        self._runner: Optional[QueueSyncRunner] = None
        # The entry the device is (or is about to be) playing.
        self._current_key: Optional[EntryKey] = None
        # The queue generation (QobuzQueue.generation) the device queue was
        # last loaded for; None when it holds nothing of ours that is current.
        self._published_generation: Optional[tuple[str, int]] = None
        self._start_result: Optional["asyncio.Future[bool]"] = None

    @property
    def supports_gapless(self) -> bool:
        """The device advances through its own queue; nothing is armed."""
        return False

    def create_proxy_server(
        self,
        resolver: "QobuzStreamResolver",
        host: str = "0.0.0.0",
        port: int = 7120,
        queue: Optional["QobuzQueue"] = None,
    ) -> AudioProxyServerProtocol:
        self._resolver = resolver
        self._queue = queue
        return super().create_proxy_server(resolver, host=host, port=port, queue=queue)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def disconnect(self, send_device_stop: bool = True) -> None:
        await self._reset_queue_state()
        await super().disconnect(send_device_stop)

    async def retarget(self, ip: str, port: int, description_url: Optional[str] = None) -> bool:
        ok = await super().retarget(ip, port, description_url)
        if ok:
            # What the old device held says nothing about the new one: the next
            # play publishes the queue again.
            await self._reset_queue_state()
        return ok

    async def _reset_queue_state(self) -> None:
        if self._runner is not None:
            await self._runner.stop()
        self._runner = None
        self._sync = None
        self._published_generation = None

    def _ensure_sync(self) -> Optional[QueueSyncRunner]:
        """The publisher for the current device, created on first use (the
        device client and proxy exist only after connect/set_proxy_server)."""
        if self._runner is not None:
            return self._runner
        proxy = self._proxy_server
        if (
            not isinstance(self._client, SonosClient)
            or not isinstance(proxy, SonosAudioProxyServer)
            or self._resolver is None
        ):
            return None
        builder = ProxyEntryBuilder(
            proxy,
            self._resolver.api_client,
            lambda: self._capabilities,
            lambda: self._quality_override,
        )
        self._sync = SonosQueueSync(self._client, builder, name=self.name)
        self._runner = QueueSyncRunner(self._sync, on_error=self._on_sync_error, name=self.name)
        return self._runner

    def _on_sync_error(self, error: Exception) -> None:
        logger.warning(f"[{self.name}] queue handover sync failed: {error}")
        # What the device holds is in doubt: publish afresh at the next play.
        self._published_generation = None
        if self._start_result is not None and not self._start_result.done():
            self._start_result.set_result(False)

    # ------------------------------------------------------------------
    # Starting playback
    # ------------------------------------------------------------------

    async def _desired_queue(self, key: EntryKey) -> list[EntryKey]:
        """The queue to put on the device: the server's play order, plus the
        entry about to play if it is not part of it (an autoplay track, or a
        queue whose snapshot has not arrived yet)."""
        order = await self._queue.play_order() if self._queue is not None else None
        desired = [EntryKey(e.track_id, e.queue_item_id) for e in order] if order else []
        if key not in desired:
            desired.append(key)
        return desired

    async def _start_transport(self, url: str, didl: str) -> bool:
        parsed = parse_entry_url(url)
        runner = self._ensure_sync()
        client = self._client
        if (
            parsed is None
            or runner is None
            or self._queue is None
            or not isinstance(client, SonosClient)
        ):
            # No identity to publish by (or nothing to publish with): the plain
            # single-entry way.
            return await super()._start_transport(url, didl)

        key = parsed.key
        self._current_key = key
        generation = self._queue.generation
        desired = await self._desired_queue(key)

        if self._published_generation == generation and self._sync is not None:
            await runner.wait_idle()  # a publish still in flight finishes first
            if key not in self._sync.mirror:
                runner.request_reconcile(desired, anchor=None)
                await runner.wait_idle()
            if key in self._sync.mirror:
                position = self._sync.mirror.index(key) + 1
                return await client.play_from_queue(position - 1)
            self._published_generation = None  # the device does not hold it: load afresh

        return await self._load_and_start(runner, client, desired, key, generation)

    async def _load_and_start(
        self,
        runner: QueueSyncRunner,
        client: SonosClient,
        desired: list[EntryKey],
        key: EntryKey,
        generation: tuple[str, int],
    ) -> bool:
        """Publish the queue for a new generation and start playing `key` as
        soon as the first batch is on the device (the rest keeps going out in
        the background)."""
        started: "asyncio.Future[bool]" = asyncio.get_running_loop().create_future()
        self._start_result = started

        async def begin(position: int) -> None:
            ok = await client.play_from_queue(position - 1)
            if not started.done():
                started.set_result(ok)

        self._published_generation = generation
        runner.request_load(desired, key, on_start_ready=begin)
        try:
            return await asyncio.wait_for(started, START_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(f"[{self.name}] starting the queue took over {START_TIMEOUT_SECONDS}s")
            self._published_generation = None
            return False

    # ------------------------------------------------------------------
    # Following the Qobuz queue and the device
    # ------------------------------------------------------------------

    async def queue_changed(self) -> None:
        """The queue's order changed within the generation on the device (an
        edit, a shuffle toggle): bring the device's copy up to date around
        the entry that is playing."""
        runner = self._runner
        if (
            runner is None
            or self._queue is None
            or self._published_generation != self._queue.generation
        ):
            return  # nothing of this generation on the device yet
        order = await self._queue.play_order()
        if order is None:
            return  # a delta arrived and its snapshot has not: wait for it
        desired = [EntryKey(e.track_id, e.queue_item_id) for e in order]
        current = self._current_key
        if current is not None and current not in desired:
            desired.append(current)  # an autoplay track keeps playing
        runner.request_reconcile(desired, anchor=current)

    async def _observe_polled_uri(self, current_uri: Optional[str]) -> None:
        """The device reported this entry as current. Another one than the one
        we started means it moved on by itself."""
        if not current_uri or not self._is_own_proxy_url(current_uri):
            return
        parsed = parse_entry_url(current_uri)
        if parsed is None:
            return
        if parsed.key == self._current_key:
            self._current_track_confirmed = True
            return
        if not self._current_track_confirmed:
            return  # still reporting the previous entry after our own switch
        key = parsed.key
        logger.info(
            f"[{self.name}] device moved on to track {key.track_id} (item {key.queue_item_id})"
        )
        self._current_key = key
        self._current_proxy_url = current_uri
        self._position_ms = 0
        self._playback_started_at = time.monotonic()
        result = await self._stream_info_for(key)
        self._notify_item_changed(key.track_id, key.queue_item_id, result)
        self._notify_position_update(0)

    async def _stream_info_for(self, key: EntryKey) -> Optional[PlayResult]:
        """What Qobuz gave for this entry's stream (the blob the play report
        needs). The device just requested it through the proxy, so this is a
        cached lookup; None if it cannot be had."""
        if self._proxy_server is None:
            return None
        try:
            resolved = await self._proxy_server.resolve_track(
                key.track_id,
                self._capabilities,
                forced_format_id=self._quality_override,
                queue_item_id=key.queue_item_id,
            )
        except Exception as e:
            logger.warning(f"[{self.name}] could not resolve stream info for {key}: {e}")
            return None
        return PlayResult(blob=resolved.blob, format_id=resolved.format_id) if resolved else None


__all__ = ["SonosQueueBackend"]

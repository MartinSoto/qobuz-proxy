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
from urllib.parse import urlsplit

from qobuz_proxy.backends.types import (
    AdoptedPlayback,
    BackendTrackMetadata,
    PlaybackState,
    PlayResult,
)

from ..proxy_server import AudioProxyServerProtocol
from .audio_url import EntryKey, parse_entry_url
from .backend import SonosBackend
from .client import SonosClient
from .entry_builder import ProxyEntryBuilder
from .events import EventSource
from .proxy_server import SonosAudioProxyServer
from .queue_sync import SonosQueueSync
from .queue_sync_runner import QueueSyncRunner
from .transport_events import TransportEvent
from .transport_subscription import TransportEventStream

if TYPE_CHECKING:
    from qobuz_proxy.playback.queue import QobuzQueue
    from qobuz_proxy.playback.stream_resolver import QobuzStreamResolver

logger = logging.getLogger(__name__)

# The Qobuz repeat mode in the device's own play modes.
_PLAY_MODES = {"off": "NORMAL", "one": "REPEAT_ONE", "all": "REPEAT_ALL"}

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
        # The Qobuz repeat mode as the device's own play mode (see
        # set_repeat_mode): it plays the queue, so it has to repeat it too.
        self._play_mode = "NORMAL"
        # The server's next pointer as last told, and the autoplay entries
        # (items it names that are not in the queue) published after the
        # queue so the device runs on into them by itself.
        self._pointer: Optional[EntryKey] = None
        self._tail: list[EntryKey] = []
        # Device events (AVTransport LastChange) wake the poll loop; see
        # attach_events. None until given a source (or when there is none).
        self._event_source: Optional[EventSource] = None
        self._events: Optional[TransportEventStream] = None

    @property
    def supports_gapless(self) -> bool:
        """The device advances through its own queue; nothing is armed."""
        return False

    def _is_own_proxy_url(self, uri: str) -> bool:
        """Any of our entries, whichever room's proxy port it names: the ports
        are handed out in the order the rooms come up, so after a restart an
        entry the device holds can name a port another room has now (any of
        our proxies serves it, the URL says everything the stream needs)."""
        proxy = self._proxy_server
        if proxy is None:
            return False
        ours, theirs = urlsplit(proxy.base_url), urlsplit(uri)
        return (
            theirs.scheme == ours.scheme
            and theirs.hostname == ours.hostname
            and theirs.path.startswith("/audio/")
        )

    @property
    def keeps_playing_on_shutdown(self) -> bool:
        """The device runs through its own queue; a restart of the proxy is
        no reason to silence it (the next run carries on, see playing_entry)."""
        return True

    async def set_next_track(
        self, metadata: BackendTrackMetadata, queue_item_id: int = 0
    ) -> Optional[PlayResult]:
        """Nothing is ever armed: the next entry is already in the device
        queue, and arming would append a second copy of it."""
        return None

    async def _arm_next_track(
        self, actual_url: str, didl: str, metadata: BackendTrackMetadata
    ) -> bool:
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

    # ------------------------------------------------------------------
    # Device events
    # ------------------------------------------------------------------

    async def attach_events(self, source: object) -> None:
        if not isinstance(source, EventSource):
            return
        self._event_source = source
        await self._start_events()

    async def _start_events(self) -> None:
        """(Re)subscribe to the AVTransport events of the device we are
        driving now: the coordinator, the only one that reports the queue."""
        source = self._event_source
        if source is None or self._client is None:
            return
        if self._events is None:
            self._events = TransportEventStream(source, self.name, self._on_transport_event)
        await self._events.start(self._ip, self._port)

    async def _stop_events(self) -> None:
        if self._events is not None:
            await self._events.stop()

    async def _on_transport_event(self, event: TransportEvent) -> None:
        """The device says something changed: look now rather than at the next
        tick. What it changed is read the usual way (see _poll_state_loop)."""
        if event.transport_state == "TRANSITIONING":
            # The device is between entries and says nothing yet: a cycle now
            # would read the same, and its spacing would then hold back the
            # cycle the PLAYING event right behind it wants (captured: 0.75 s
            # later). If that never comes, the interval poll still does.
            return
        self.wake_poll()

    async def disconnect(self, send_device_stop: bool = True) -> None:
        await self._stop_events()
        await self._reset_queue_state()
        await super().disconnect(send_device_stop)

    async def retarget(self, ip: str, port: int, description_url: Optional[str] = None) -> bool:
        ok = await super().retarget(ip, port, description_url)
        if ok:
            # What the old device held says nothing about the new one: the next
            # play publishes the queue again, and its events are another's.
            await self._reset_queue_state()
            await self._start_events()
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

    async def set_repeat_mode(self, mode: str) -> None:
        """Relay the repeat mode: the device is the one running through the
        queue, so repeat-one and repeat-all are its own play modes. Shuffle is
        never relayed that way: the published order already is the shuffle."""
        self._play_mode = _PLAY_MODES.get(mode, "NORMAL")
        await self._apply_play_mode()

    async def _apply_play_mode(self) -> None:
        if isinstance(self._client, SonosClient) and not await self._client.set_play_mode(
            self._play_mode
        ):
            logger.warning(f"[{self.name}] could not set the play mode to {self._play_mode}")

    async def _desired_queue(self, key: EntryKey) -> list[EntryKey]:
        """The queue to put on the device: the server's play order, plus the
        entry about to play if it is not part of it (an autoplay track, or a
        queue whose snapshot has not arrived yet)."""
        order = await self._queue.play_order() if self._queue is not None else None
        desired = [EntryKey(e.track_id, e.queue_item_id) for e in order] if order else []
        await self._refresh_tail_state()
        desired.extend(k for k in self._tail if k not in desired)
        if key not in desired:
            desired.append(key)
        return desired

    async def _refresh_tail_state(self) -> None:
        """Recompute the autoplay entries that belong after the queue: those up
        to the one playing now (it is part of what the device holds), plus the
        entry the server's next pointer names if the queue does not hold it."""
        current = self._current_key
        tail = self._tail[: self._tail.index(current) + 1] if current in self._tail else []
        pointer = self._pointer
        if (
            pointer is not None
            and pointer not in tail
            and self._queue is not None
            and await self._queue.contains_item(pointer.queue_item_id) is False
        ):
            tail.append(pointer)  # the server is following its autoplay list (O20)
        self._tail = tail

    async def next_pointer_changed(self, info: Optional[dict]) -> None:
        self._pointer = (
            EntryKey(str(info["trackId"]), int(info["queueItemId"])) if info is not None else None
        )
        before = list(self._tail)
        await self._refresh_tail_state()
        if self._tail != before:
            if self._tail:
                logger.info(f"[{self.name}] following the autoplay list: {self._tail[-1]}")
            await self.queue_changed()

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
        if self._published_generation != generation:
            self._tail = []  # another queue: what followed the old one is gone
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
            if self._play_mode != "NORMAL":
                await self._apply_play_mode()  # a fresh queue must repeat like the last one
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
    # Carrying on under a device that is already playing
    # ------------------------------------------------------------------

    async def playing_entry(self) -> Optional[AdoptedPlayback]:
        """The queue entry of ours the device is playing or has paused right
        now: what a proxy that just (re)started finds when the app rejoins."""
        if not isinstance(self._client, SonosClient):
            return None
        state = await self.get_state()
        if state not in (PlaybackState.PLAYING, PlaybackState.PAUSED):
            return None
        uri = await self._get_current_transport_uri()
        parsed = parse_entry_url(uri) if uri and self._is_own_proxy_url(uri) else None
        if uri is None or parsed is None or parsed.session is None or parsed.major is None:
            return None
        return AdoptedPlayback(
            track_id=parsed.key.track_id,
            queue_item_id=parsed.key.queue_item_id,
            session=parsed.session,
            major=parsed.major,
            position_ms=await self.get_position(),
            paused=state == PlaybackState.PAUSED,
            url=uri,
        )

    async def adopt(self, playing: AdoptedPlayback, duration_ms: int) -> None:
        self._current_key = EntryKey(playing.track_id, playing.queue_item_id)
        self._current_proxy_url = playing.url
        self._current_track_confirmed = True
        self._position_ms = playing.position_ms
        self._duration_ms = duration_ms
        self._playback_started_at = time.monotonic()
        self._state = PlaybackState.PAUSED if playing.paused else PlaybackState.PLAYING
        runner = self._ensure_sync()
        if runner is not None and self._sync is not None and self._queue is not None:
            # The device holds the queue the previous run published: learn it.
            if await self._sync.resync():
                self._published_generation = self._queue.generation
        logger.info(
            f"[{self.name}] carrying on with {self._current_key} "
            f"({'paused' if playing.paused else 'playing'} at {playing.position_ms}ms)"
        )

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
        await self._refresh_tail_state()  # a new snapshot may change what the queue holds
        desired.extend(k for k in self._tail if k not in desired)
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

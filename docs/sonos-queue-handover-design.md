# Sonos queue handover — design and plan

**Status: draft for review, 2026-10-10. Nothing here is implemented.**
Evidence references (`O1`…`O22`) point at
[queue-and-shuffle-wire-observations.md](queue-and-shuffle-wire-observations.md),
which holds the live captures this design is built on. Statements about what
the Sonos device itself can do (AVTransport actions, limits, eventing) come
from general knowledge of the platform, **not** from this repo or from
hardware tests, and were marked *(verify)* until the Phase 0 spike (section 9)
settled them on one Sonos One; items still marked *(verify)* are open.

## 1. Goal

When the Qobuz app plays something on a Sonos room, the proxy currently
sequences it: one track at a time it loads the current track and arms the next
one as the Sonos queue's tail, and the Player state machine decides what comes
after. The goal is to invert that for Sonos:

- The **whole queue** the Qobuz server holds is published to the Sonos device's
  own AVTransport queue, and the device plays and advances through it by itself.
- **Shuffle and repeat** are reflected on the device, so it behaves as the
  server's queue says it should.
- The proxy's role shrinks to: keep the device queue equal to the server's
  queue, report what the device is playing back to the server, and carry
  transport commands (play, pause, seek, volume, skip) across.

Generic DLNA renderers and the local PortAudio backend are out of scope and
keep today's sequencing.

Non-goals: Sonos-native shuffle (see 5.4), edits made in the Sonos app itself
(decision 4, 6.2), S1 hardware.

## 2. What the captures settled (the facts this design stands on)

| Fact | Evidence |
|---|---|
| A load (type 91, or a type 90 on a fresh session) is a new **major** version; item ids are unique only within it; the id base varies (0, 164, 255 …) | O3, O4, O15 |
| Every in-place change — shuffle, insert/add/remove/reorder, autoplay reload — is a **minor** bump; the playing track is never interrupted | O8, O12, O19 |
| Selecting any other track of the loaded queue is a new load (new major), not a jump message | O13 |
| Next/Previous/seek/pause arrive as `SET_STATE`, no version change; Previous restarts the track or steps back, decided by the app | O14 |
| A **snapshot** (type 90, requested with `AskForQueueState`) carries the queue in original order plus `shuffledTrackIndexes`; it answered within 40–50 ms every time and always agreed with the server's next pointer | O16, O19 |
| With shuffle on, reorder/remove act on the shuffled order, add appends, and a "play next" insert re-arranges the upcoming order unpredictably; the wire never announces the result | O18, O19 |
| A new load resets shuffle; each shuffle-on makes a new order | O9, O16 |
| Repeat changes arrive as type 97 with no version bump; the server wraps the next pointer for repeat-all but **not** for repeat-one | O10, O11, O17 |
| After the last item the next pointer is either the no-track sentinel (stop) or an autoplay item that is not in the queue | O20, O21 |
| After the queue ends the player should land on the first item, stopped; a bare PLAY then must start it | O2, O22 |
| The proxy ignores types 96/97/102/103 today, and our proto for 102 is wrong | O10, O21 |

## 3. Design principles

1. **The server snapshot is the truth; deltas are only triggers.** Any edit or
   toggle (types 92–96, and a load) makes the proxy request a snapshot and
   rebuild the desired order from it. No delta semantics are implemented.
2. **A generation is (session, major version).** Everything the device holds
   belongs to one generation. Entries from another generation are stale by
   definition (the user's rule: after the version moves on, old URLs must die).
3. **Never disturb the playing entry** on a minor change; reconcile the rest
   of the device queue around it.
4. **The device sequences; the proxy observes.** The proxy does not decide the
   next track in handover mode. It follows the device and reports upward.
5. **The next pointer stays authoritative for what lies beyond the queue**
   (autoplay tail, or stop).
6. **Every command we cannot honor gets a state report** so the app's
   optimistic UI is corrected (O22).

## 4. Architecture

```
Qobuz server ── WS ──► WsManager ──► PlaybackCommandHandler ──► QobuzQueue (ordered model)
                         ▲  ask snapshot (debounced)                   │ desired play order,
                         │                                              │ repeat, shuffle, version
                         │                                              ▼
                      Player (transport, reporting)  ◄── events ── SonosQueueSync (new)
                                                                      │ reconcile / seek / play mode
                                                                      ▼
                                          SonosBackend → SonosClient → device AVTransport queue
                                                                      ▲
                       Sonos device ── GET /audio/... ──► SonosAudioProxyServer (generation-checked URLs)
```

New or substantially changed units:

| Unit | Change |
|---|---|
| `connect/ws_manager.py` | Production version of the diagnostic: `request_queue_state()` (debounced); route 96/97/102/103 to the handler; expose the session uuid |
| `protos/qconnect_payload.proto` | Fix `SrvrCtrlAutoplayModeSet` (fields: 1 queueVersion, 2 actionUuid, 3 on, 4 loading — O21); regenerate; check the type 96 field 4 name (it is not an item id, O7) |
| `playback/queue.py` | `QobuzQueue` becomes an **ordered model**: original-order track list, shuffle flag and indexes, derived play order, version, session, repeat mode, autoplay tail pointer. Replaces `QueueMembership` (the membership consumer, album-gain grouping, reads from it) |
| `playback/command_handler.py` | Apply snapshots (90/91); treat 92–96 as triggers; handle 97 (repeat) and 102/103; keep SET_STATE handling for transport and pointers |
| `playback/player.py` | A "device-sequenced" mode: gapless arming, transition detection and next-track bookkeeping are bypassed; item changes come from the backend; end-of-queue landing and PLAY-with-nothing-loaded fixed |
| `backends/base.py` | New optional capability, e.g. `QueueSyncBackend` (`publish_queue`, `seek_to_item`, `set_repeat`, item-changed callback); Sonos implements it, others do not |
| `backends/dlna/sonos/queue_sync.py` (new) | The reconciler and its device-queue mirror (section 5.3) |
| `backends/dlna/sonos/client.py` | New AVTransport actions (exist, Phase 0): `AddMultipleURIsToQueue`, `RemoveTrackRange`, `ReorderTracksInQueue`, `SetPlayMode`, queue read (`Browse Q:0` or `GetMediaInfo` for the count) |
| `backends/dlna/sonos/proxy_server.py` | Generation-checked URLs, lazy format resolution at request time (section 5.2) |
| `backends/dlna/sonos/events.py` | An AVTransport GENA subscription alongside topology (works on a standalone room, Phase 0; group behavior *(verify)*); polling remains the fallback |

The user-facing switch is a flag (working name `sonos_queue_handover`,
environment variable `QOBUZPROXY_SONOS_QUEUE_HANDOVER`), default **off** until
the later phases pass on hardware; both paths coexist until then.

## 5. Design

### 5.1 The queue model and the snapshot loop
- On a load (91) or snapshot (90) the model is replaced wholesale: tracks in
  original order, `shuffleMode`, `shuffledTrackIndexes`, version, plus the
  load's `queuePosition` and `contextUuid`. The **play order** is the indexes
  applied to the tracks (identity when shuffle is off).
- On an edit or toggle (92–96) the proxy sends `AskForQueueState` (coalesced
  over ~100–200 ms; edits can arrive 1–2 s apart, O19) and applies the reply.
  A reply older than the model's version is dropped.
- The reply is always followed by a type 97 carrying the current repeat mode
  (O19); that is how repeat state is also read on connect and reconnect.
- The type 90 on a fresh session (O3) goes through the same path; whether it
  also means "play" is decided by the rule in 6.2, decision 3.
- Type 103 (autoplay list) and the autoplay pointer are tracked but never
  published as part of the queue (O20).
- Shuffle belongs to a load: a major bump clears it before the new snapshot is
  applied (O9).

### 5.2 URLs and the proxy server
The Sonos URL becomes

`/audio/{track_id}?s={session8}&v={major}&i={queue_item_id}&fmt={flac|wav|mp3}`
(+ the existing gain and rate parameters where they still apply)

- `s` is a short digest of the Qobuz session uuid, `v` the major version, `i`
  the item id. Item id alone is not enough (O15), the pair `v`/`i` is unique
  within a session, and `s` guards against a restart reusing small numbers.
- The proxy keeps one **current generation** (fed by `QobuzQueue`). A request
  whose `s`/`v` differ gets **410 Gone**; Sonos requests such an entry once and
  skips it (9.2). A minor bump changes nothing: entries stay valid (O8).
- **The container (`fmt`) is chosen at publish time, from track metadata, not
  from a per-track stream request.** Phase 0 showed that the DIDL's MIME type
  is binding: a wildcard is rejected (UPnP 804) and declaring `audio/wav` for
  a FLAC stream made Sonos run through the queue instead of playing it (9.2).
  So the rule, applied when the entry is published:
  `flac` (passthrough) only when the track's `maximum_sampling_rate` and
  `maximum_bit_depth` fit the device (≤ 48 kHz, ≤ 24 bit on Sonos); `mp3` only
  when the device or the track cannot do FLAC; otherwise `wav` (transcode,
  downsampling to the device rate). The proxy then serves **exactly the
  published container** for that URL, whatever tier Qobuz returns at request
  time (a `wav` entry can transcode any source; a `flac` entry only ever
  qualifies when every tier fits). This keeps the existing fits/fallback logic
  but moves its input from `getFileUrl` to metadata.
- The per-track stream request (`getFileUrl`, signed CDN URL, blob) therefore
  still happens, but **when the device asks for the stream**, not for every
  queued entry. `HEAD` probes are not a concern on Sonos (9.2).
- **Metadata comes in batches.** `track/getList` (already used for album gain,
  `get_tracks_audio_info`) returned 15 of 15 tracks in one call with title,
  performer, album title and cover art, duration, `maximum_sampling_rate`,
  `maximum_bit_depth` and `hires_streamable` — everything DIDL-Lite and the
  container rule need. **At most 50 ids per call** (51 returns HTTP 400, measured in Phase 1), so a
  1000-track queue is 20 calls.
- DIDL `res` attributes `sampleFrequency`/`bitsPerSample` can be omitted: on the
  Sonos One the seek offset comes from the WAV header, not the DIDL (9.2). The
  repo's DIDL builder still documents that a renderer trusting wrong values
  garbles seeks, so this is verified for Sonos only. `protocolInfo`'s MIME type
  must be correct (above), and `duration` should be set.
- Reporting needs the stream `blob`; the proxy records what it resolved when it
  served the request and hands it to the Player on item change, falling back to
  a cached resolve.

### 5.3 The device queue and the reconciler (`SonosQueueSync`)
It keeps a **mirror** of the device queue (list of generation-tagged item ids)
and brings it to the desired state with the fewest calls.

- **New generation (a load):** clear, add the whole desired order (current item
  first so playback can start), `Seek TRACK_NR` to the selected item, play.
  Today's `_start_transport` already does clear → add → seek → play for one
  entry; this extends it to N.
- **Same generation (a snapshot):** compute the minimal edit from mirror to
  desired — removals (runs via `RemoveTrackRange`), moves
  (`ReorderTracksInQueue`), inserts (`AddMultipleURIsToQueue` at a position) —
  **never touching the playing entry**. If it is cheaper, rebuild everything
  around it instead (remove all others, re-add), which Sonos allows while
  playing (confirmed in Phase 0, 9.2).
- **Order of publishing:** playing item and what follows first, then the
  earlier items (for Previous), then the rest; metadata and DIDL are filled in
  that priority, so playback never waits on the tail.
- **Tail entry:** if the server's next pointer names an item that is not in the
  queue (autoplay, O20), append it as a one-off entry after the last item; it
  is replaced when the pointer moves.
- **Size:** no cap was found up to 1200 entries (Phase 0); a configurable
  safety cap still applies and larger queues are
  truncated at the end with a log line, never at the playing item.
- **Concurrency:** one reconciler per speaker, one operation at a time, with
  latest-wins coalescing of pending snapshots (the same idea as the Player's
  command queue). Calls go through the existing per-device serialization in
  `DLNAClient`; batches are chunked at **16** (the device's per-call maximum, Phase 0) to stay gentle on weak
  coordinators (the Sonos One issue, noted in project memory).
- **Adoption:** on start, retarget, coordinator change or proxy restart, read
  the device queue (`Browse Q:0`, works, Phase 0), parse entry URLs and adopt it if
  the generation matches; otherwise rebuild.

### 5.4 Shuffle and repeat
- **Shuffle:** publish the queue **in the server's shuffled order** with the
  device play mode left at NORMAL/REPEAT_*. Sonos's native shuffle cannot
  reproduce the server's order or the server's next pointer, and Next/Previous
  must stay identical on both sides (O16). A new shuffle-on, an edit under
  shuffle, and shuffle-off are all just "new snapshot, reconcile".
- **Repeat:** map immediately on type 97 — off → `NORMAL`, all →
  `REPEAT_ALL`, one → `REPEAT_ONE` — independent of the publish step (it is not
  versioned, O10). Repeat-all is also what the server expects (it wraps its
  own next pointer, O17); repeat-one is renderer-side only (O11) and the
  device's native mode covers it.
- Open: shuffle **plus** repeat-all (not captured). The device wraps to the
  first published entry, which is the pivot item; the server's wrap target must
  be checked (Phase 0 / a short capture).

### 5.5 Following the device (tracking and reporting)
- The backend observes the device (polling `GetPositionInfo` as today, plus
  AVTransport eventing, which works, 9.2) and parses the playing entry's
  URL into (session, major, item). The Player gets an **item-changed** event.
- On item change the Player: sets `current_track` from the model, reports the
  new `currentQueueItemId`, duration and version to the server, and does the
  streaming start/end reports (as `_handle_gapless_transition` does today). The
  server's next pointer then follows (O17).
- **Commands:** `SET_STATE` with a current item → `Seek TRACK_NR` to that
  item's index in the mirror (Next/Previous/step-back, O14); restart = seek 0;
  a load → new generation. PLAY/PAUSE/volume as today.
- **Hijack and foreign takeover** are redefined (edits from the Sonos app are
  not a goal, 6.2 decision 4). Playing URL parses as ours with
  a stale generation → we are mid-rebuild or lagging: wait and resync, not a
  takeover. Playing something not ours, or the queue changed outside our
  operations (Sonos app) → takeover, as today's `_is_own_proxy_url` logic
  already treats a foreign URI.
- **End of queue:** device stops (no tail) → the Player lands on the **first
  item of the play order**, stopped, position 0, and reports that real id and
  duration (O21, O22). A later PLAY seeks to index 0 and plays.

### 5.6 What goes away on the Sonos path
In handover mode these are bypassed, not deleted yet: `Player` gapless arming
and transition detection (`_prepare_next_track_*`, `_handle_gapless_transition`,
`_on_next_track_started`, `on_next_track_info_changed`), the backend's
next-track and per-entry bookkeeping (`set_next_track`, `clear_next_track`,
`_next_track_queue_nr`, `_current_track_queue_nr`, `_retire_queue_entry`), and
the stale-queue-entry-replay workaround from the old model. The generic DLNA
path keeps all of it.

## 6. Risks and open decisions

### 6.1 Risks
- **HEAD/transcode cost.** If Sonos probes every queued entry, each transcode
  entry's `HEAD` means a CDN read (exact WAV length). **Phase 0 retired this
  for the device tested: Sonos never sent a HEAD, and queued entries were not
  fetched at all until played or prefetched as next (9.2).** Lazy resolution
  (5.2) is still the right shape because of the per-track API call, not the
  device.
- **Stale-URL behavior.** Retired: Sonos skips an entry that answers 410/404
  after one request and carries on (9.2), so a reconcile race costs a skipped
  track, not a stopped player. The reconciler should still remove stale
  entries before they can be reached.
- **Weak coordinator.** Queue operations are cheap on the Sonos One (1200
  adds in 2.3 s with no visible latency, 9.2); the known weakness is sustained
  polling, so keep the 2 s poll interval and prefer eventing.
- **Seeded insert under shuffle** cannot be predicted (O19): the snapshot loop
  is the only defense, so its latency and failure handling matter.
- **Retarget and coordinator handover** replay the whole queue onto a new
  device (the existing retarget path was written around the current one-track-plus-next model).
- **Metadata volume** for large playlists: `track/getList` takes at most 50
  ids per call (20 calls for 1000 tracks, about 0.5 s each).
- **Publish-time container prediction** can disagree with what the stream
  turns out to be only in the safe direction (a `wav` entry can transcode
  anything; a `flac` entry is published only when every tier fits), but a
  mistaken `maximum_sampling_rate` in Qobuz's data would break one entry.

### 6.2 Decisions (2026-10-10)
1. **Whole queue, chunk by chunk.** The whole queue is published, in chunks and
   in priority order (5.3). A window (current ± N) stays a fallback only if
   Phase 0 shows Sonos probing every entry hard enough to matter.
2. **Follow the server's next pointer past the end of the queue.** An item
   outside the queue (autoplay) is appended as a one-off tail entry; the
   sentinel means stop (O20, O21).
3. **A queue snapshot is followed whenever we are the active renderer — not
   only when idle.** Interpretation to implement and to confirm in review: a
   type 90 or 91 whose **major version is higher than the model's**, received
   while active, is a *load*: adopt the queue, start the item at its
   `queuePosition` (position 0 of the play order when absent, which is also
   what the first-load type 90 of O3 needs), and play. A snapshot with the
   **same** major version (the reply to our own ask, or another client joining)
   only updates the model. Not observed: a type 90 with a higher major that is
   not a load; if one ever shows up, this rule would restart playback
   spuriously, so the first implementation should log every such case.
4. **Edits made in the Sonos app are out of scope for now.** No special
   handling is built; they may happen to work through the existing takeover
   detection (5.5), and are not a goal or a test target.
5. **Flag lifetime is left open** and will be decided from how the phases go.

## 7. Plan

Each phase ends with tests green (`ruff`, `mypy` for the touched code, `pytest`)
and, where noted, a hardware check.

**Phase 0 — Hardware spike (no product code).** Scripts in the scratchpad; the
results go into a new section of this document.
- Confirm the AVTransport actions and their limits: `AddMultipleURIsToQueue`
  batch size, `ReorderTracksInQueue`, `RemoveTrackRange`, `SetPlayMode`,
  `Browse Q:0` contents and its update id, queue maximum.
- Behavior with a 50–200 entry queue of our URLs: does Sonos `HEAD` every
  entry, in what order, at what rate? Cost on the Sonos One coordinator.
- What Sonos does when an entry returns 410/404 at the head, in the middle,
  and as the next entry.
- Mutating the queue around the playing entry (remove, move, insert before and
  after) without a glitch; gapless across mixed passthrough/WAV entries.
- AVTransport eventing: `LastChange` contents for track change, transport
  state; reliability across a group.
- Native `REPEAT_ALL`/`REPEAT_ONE` behavior at the end of a published queue.
- Capture: shuffle + repeat-all wrap target (6.1).

**Phase 1 — Protocol and queue model** (no change to Sonos playback).
- Fix the type 102 proto; add handlers for 96/97/102/103; production
  `request_queue_state()`; remove the TEMP diagnostic.
- Ordered `QobuzQueue` with snapshot application; **replay tests built from the
  captured frames** (sanitized fixtures from the RAW_WS logs) asserting the play
  order against the recorded next pointers (O16, O19).
- Wire repeat (type 97) into the Player for all backends; this also fixes the
  existing ignored-repeat bug (O10) independently of Sonos.

**Phase 2 — URLs and proxy.** Generation-checked URLs and lazy resolution in
`SonosAudioProxyServer`; capabilities handed in once; 410 on stale. Current
single-track playback keeps working on the new URL scheme.

**Phase 3 — Publisher.** `SonosQueueSync` and the new `SonosClient` actions,
tested against an **in-memory fake AVTransport** that models the queue and the
failure modes found in Phase 0; property tests over random edit sequences.
Load publishes the whole queue and starts at the right item.

**Phase 4 — Following the device.** Item-changed events, Player/state/play
reporting, `seek_to_item`, handover mode bypassing gapless, hijack
redefinition. Hardware check: album, playlist, next/previous, tap another
track.

**Phase 5 — Modes and edits.** Repeat mapping, shuffle reconcile, edits under
shuffle, autoplay tail, end-of-queue landing, bare-PLAY fix (O22). Hardware
check: every scenario in the capture log (O9–O22).

**Phase 6 — Hardening.** Retarget/coordinator handover, proxy-restart adoption,
Sonos One load test, logging and metrics, updating `CLAUDE.md`'s architecture
section.

**Phase 7 — Default on, then remove** the Sonos gapless path.

## 8. Test strategy
- Unit: URL build/parse, generation validation, snapshot → play order, repeat
  and shuffle mapping, reconciler diffs.
- Replay: captured traces drive the queue model and a fake backend end to end
  (what the server sent → what the device queue should be).
- Fake device: an in-memory AVTransport honoring the queue actions and
  injectable faults (slow calls, refused connections, 410 handling).
- Hardware: a written checklist per phase, run on the Sonos One and a
  Five/Beam coordinator, since they behave differently.

## 9. Phase 0 results (2026-10-10)

**Subject:** room "Cocina", Sonos One (S18), firmware 97.1-80312, S2, a
standalone group (the same model and firmware as the "weak coordinator" of the
project notes). Idle, empty queue, volume 15 restored after every run. Scripts:
`SOAP` calls straight at the device plus a local quiet-audio server that logs
every request Sonos makes (kept in the session scratchpad, not in the repo).
Audio was a 3 %-amplitude 220 Hz WAV at volume 8.

### 9.1 What the device exposes
- AVTransport lists 42 actions, including `AddMultipleURIsToQueue`,
  `RemoveTrackRangeFromQueue`, `ReorderTracksInQueue`, `SetPlayMode`, `Seek`
  (units `TRACK_NR`, `REL_TIME`, `TIME_DELTA`), `Next`, `Previous`.
- Play modes: `NORMAL, REPEAT_ALL, REPEAT_ONE, SHUFFLE_NOREPEAT, SHUFFLE,
  SHUFFLE_REPEAT_ONE`.
- A separate Sonos `Queue` service exists (`ReplaceAllTracks`, `AddMultipleURIs`,
  `RemoveTrackRange`, `ReorderTracks`, `Browse`, `AttachQueue`, …); an atomic
  `ReplaceAllTracks` might simplify a rebuild but was not needed or tested.
- `AddMultipleURIsToQueue` takes space-separated URIs and a matching
  space-separated list of DIDL documents, plus `DesiredFirstTrackNumberEnqueued`
  (0 = append, otherwise insert at that position) and `UpdateID`.

### 9.2 Measured behavior
| Question | Result |
|---|---|
| Batch size | 1, 8, 16 entries per call work; **17 or more fails with UPnP 402**. Chunk at 16. |
| Does queuing entries make Sonos fetch them? | **No request at all** to the audio server for 1200 queued entries. |
| HEAD probes? | **Never** seen, queued or playing: only plain `GET`, plus `GET` with `Range` on seeks / previous. |
| Queue size | 1200 entries accepted in 2.3 s (75 calls), no refusal (limit not found), probe latency stayed at 10–20 ms. |
| Reading the queue back | `Browse Q:0` returns titles and our URLs unchanged (query strings intact); 1200 entries in 0.26–0.42 s; one request can return 1000 entries (520 KB). `TotalMatches`/`UpdateID` come with it. |
| Editing while playing | Remove a range, reorder, insert before/after the playing entry, remove before it: **playback never interrupted**; track numbers shift (insert before → the playing entry's `Track` goes 1→2), so entries must be tracked by URL, not index. |
| `UpdateID` | `0` is accepted as "don't care"; a wrong value is **rejected (error 1028)**; the correct value works. Each change returns a new `UpdateID`. |
| Bulk edits | Remove 400 entries, reorder a 50-entry block, clear 1200 entries: each 10–20 ms. |
| Refused entries | A `410` or `404` on an entry (next, direct seek target, or the first entry) is requested **once, then skipped**; playback continues with the following entry. |
| `Previous` | Always steps back one entry (no restart-the-track behavior, in contrast to the Qobuz app; restart stays a seek to 0). |
| End of queue, `NORMAL` | Playback **stops with `Track` back at 1** (the first entry) — the "land on the first track, stopped" rule of O22 is the device's own behavior. |
| `REPEAT_ALL` | Wraps 3 → 1 seamlessly. |
| `REPEAT_ONE` | Stays on the entry. |
| Do DIDL `sampleFrequency`/`bitsPerSample` affect a seek into a transcoded WAV? | **No.** Real proxy path (96 kHz FLAC downsampled to 48 kHz/24-bit WAV, 82 MB, 285 s), muted, one queue entry, `Seek REL_TIME` to 60 s and 120 s under four DIDL variants — correct 48000/24, attributes omitted, wrong 96000/24, wrong 44100/16. **All four gave identical Range requests** (`bytes=17268736-` and `bytes=34553856-`), which is the WAV-header-derived offset (about 4 KB aligned, not frame aligned: the proxy aligns it to a frame itself). Reported position afterwards was 1:02 and 2:02. Only a plain `GET` first, then the ranged `GET`; no `HEAD`. |
| Declared MIME vs. actual stream (DIDL `protocolInfo`) | **Binding.** `http-get:*:*:*` is **rejected at enqueue (UPnP 804)**. Declaring `audio/wav` for FLAC entries (while the proxy served `audio/flac`) made Sonos run through the queue in about a second instead of playing; with the correct per-entry MIME the same queue played normally. |
| Gapless across mixed entries (real tracks, muted, 0.2 s polling) | FLAC passthrough → 96 kHz-source WAV transcode → FLAC passthrough: both transitions went `PLAYING` → `PLAYING` with the track number changing and **no `TRANSITIONING`/`STOPPED` state in between**. An audible gap cannot be judged muted. |
| When does Sonos fetch the next entry? | Observed with a seek to 25 s before the end: the next entry's first `GET` came **~18 s before** the end (FLAC → WAV) and **~4–5 s before** it (WAV → FLAC). The lead varies; do not rely on a fixed one. |
| Grouped room (Cocina coordinating Baño) | Queue **writes sent to the member fail (UPnP 800)**; `Browse Q:0` on a member returns an empty queue of its own; a member's `GetPositionInfo` shows `x-rincon:<coordinator>`. Eventing on the **coordinator** carries every transition (6 events for a 3-entry run); the **member** sent a single event (`x-rincon:` source) and nothing per track. So: always target the coordinator, subscribe there only. |
| Queue size, again | **5008 entries accepted** in 8.7 s on the grouped coordinator, no refusal (the loop stopped there). |
| AVTransport eventing (GENA `LastChange`) | Subscription works. Events arrive within ~0.1–0.5 s of a change and carry `TransportState`, `CurrentTrack`, `NumberOfTracks`, `CurrentPlayMode`, `CurrentTrackURI`, `CurrentTransportActions`, `CurrentTrackDuration`. A skipped entry produces no event (track 2 → 4). The initial subscription event arrives immediately. |

### 9.3 Consequences for the design
- Chunk size is fixed at 16; a 1000-track playlist is ~63 calls and a couple of
  seconds. The cost of publishing the whole queue is the **metadata** from the
  Qobuz API, not the device. Decision 1 (whole queue, chunk by chunk) is cheap.
- The reconciler can use `UpdateID` 0 for simplicity, or the returned value for
  safety against concurrent changes (a stale value fails loudly, which is a
  usable conflict signal).
- Entries are identified by URL (generation, item id), never by position; the
  mirror must be re-derived from `Browse` or from the `CurrentTrackURI` events.
- Item-change tracking can use eventing instead of the 2 s poll, which also
  helps the weak coordinator. Polling stays as fallback.
- DIDL `sampleFrequency`/`bitsPerSample` may be omitted (Sonos ignores them for
  seeks, 9.2), but the **MIME type must be right at publish time**, so the
  container is decided then, from `track/getList` metadata (5.2).
- Publishing and eventing go to the **coordinator** only; a member rejects
  queue writes and sends no per-track events.
- A refused or stale entry costs one skipped track, not a stopped player.
- End-of-queue landing is native on the device; the proxy must report it
  (and not gate PLAY on its own `_current_track`, O22).

### 9.4 Still open
- What happens to the queue and to eventing when the **coordinator changes**
  (group reshuffle, Baño taking over): not tested, since it would change the
  household's groups.
- An **audible** gap check across mixed entries (the spike ran muted).
- A queue beyond 5008 entries (no limit found; likely irrelevant).
- Behavior with a real, long playlist end to end (Phases 3–5).

## 10. Progress

### Phase 1 — protocol and queue model (2026-10-10)
Done, each as its own commit, with tests (1175 passing; ruff clean; mypy at its
pre-existing 79 errors, none in the touched code):
- `0c09493` **proto fix**: `SrvrCtrlAutoplayModeSet` is `{queueVersion=1,
  actionUuid=2, autoplayOn=3, flag=4}`; `SrvrCtrlShuffleModeSet` field 4 is the
  shuffle seed, not an item id. Only `qconnect_payload_pb2.py` regenerated.
- `5439c04` **ordered queue model**: `QobuzQueue` keeps the original order and
  the server's shuffled indexes from full snapshots (`QueueOrder`); deltas keep
  membership current and mark the order stale. Replay tests use the real index
  lists of the capture (O16, O19).
- `ddfea70` **handlers**: type 90 snapshots applied (with the shuffled order);
  every edit (92–95) and shuffle toggle (96) triggers a debounced (150 ms)
  `AskForQueueState` via the new `WsManager.request_queue_state()`; type 97
  repeat now reaches the Player after connect for every backend (it was
  ignored, O10); 102 and 103 handled; the TEMP diagnostic is gone.
- `384b52a` **repeat-one on the gapless path**: no next track is armed while
  repeat-one is on, an armed one is cleared when it is switched on and re-armed
  when it is switched off (O11).
- Measured: `track/getList` takes 50 ids per call.

Not done, deliberately:
- **Decision 3's "follow a snapshot with a higher major while active"** is not
  implemented. Interpreted literally it would also fire when a reconnect hands
  us a restored session (our own connect-time ask returns the restored queue at
  a higher major than our empty model), which would start playback unasked.
  The narrow, safe variant that fixes the first-load hang (O3) is: act only when
  the model's previous snapshot was **empty** (or the version still 0) and a
  non-empty one arrives with a higher major while we are the active renderer and
  idle; log every other higher-major snapshot. Needs confirmation before it is
  built; it changes playback start for all backends.
- No change to Sonos playback yet; that starts in Phase 2.

### Phase 2 — URLs and proxy (2026-10-10)
Done, each as its own commit, 1240 tests passing:
- `38eb2b7` **generation-tagged URLs, 410 for stale ones**: every Sonos proxy
  URL carries `&s=<session tag>&v=<major version>&i=<queue item id>` (the
  existing `format`/`depth`/`rate`/`item` parameters are unchanged). A request
  naming an earlier generation gets 410, which the device requests once and
  skips (9.2). Not stale: minor bumps (shuffle, edits, autoplay), URLs without
  `s`/`v` (older entries, or built before the session was known) and any
  request while the current generation is itself unknown (fresh start). The
  player hands the queue item id to the backend through `BackendTrackMetadata`,
  the speaker records the session on the queue when the tokens arrive.
- `22905bc` **explicit loads republish the current track**: `apply_remote_state`
  only loaded a track that differed from the current one, so re-selecting the
  playing track (a new generation) merely seeked it to 0 and left the device on
  a URL the proxy would now refuse on the next ranged request. Type 91 now
  passes `force_reload`. A restart was what the app wanted anyway.
- `5431bb6` **batch metadata**: `get_tracks_metadata` (display data plus
  `max_sample_rate_hz`/`max_bit_depth`) and chunking of every `track/getList`
  lookup at 50 ids. This also fixed an existing latent bug: the Sonos gain
  feature (`get_tracks_audio_info`) sent a whole queue in one request and so
  silently lost its data for queues longer than 50 tracks.
- `9431287` **publish-time container choice**: `choose_container` and
  `SonosAudioProxyServer.publish_track` build an entry's URL from the track's
  metadata with no stream request (blob left empty until the device asks).
  A test requires them to equal `resolve_track`'s result across 6 source formats ×
  downsampling/gain × 4 quality overrides × 4 device types. One deliberate
  difference: a manual FLAC-tier override on a device that cannot play FLAC
  still serves FLAC at request time; publish time gives it MP3.

Nothing in playback uses `publish_track` yet (Phase 3 does). Not done from the
plan: handing the stream `blob` to the player when the device requests an entry
(Phase 4, it is the first thing needed there); "capabilities handed in once" is
unnecessary since `publish_track` takes them as an argument.

### Phase 3 — publisher (2026-10-10)
Done, each as its own commit; 1331 tests passing. Nothing in playback uses it
yet (Phase 4 wires it in behind the flag):
- `30c6978` **client actions** on `SonosClient`: `add_uris_to_queue` (≤16),
  `remove_queue_range`, `move_queue_range`, `set_play_mode`,
  `seek_to_queue_position`, `browse_queue`; typed results carrying the device's
  `UpdateID` and UPnP error code. Every one is sent once and a UPnP answer is
  final (new `retry_upnp_errors` switch): a blind retry of a state-changing
  action could apply it twice.
- `427ae87` **planner** (`queue_plan.py`): the minimal remove/move/insert
  operations that turn the device's queue into the desired one without touching
  the playing entry (longest in-order stretch kept, rest moved next to their
  predecessors, rebuild around the playing entry when cheaper). `apply_ops`
  encodes the measured device semantics. Checked on 1500 random edit scenarios
  each for "reaches the desired queue" and "never removes or moves the playing
  entry".
- `13b1c76` **executor** (`queue_sync.py`, `SonosQueueSync`): `load` publishes a
  generation starting with the batch holding the entry to play and calls back so
  playback starts while the rest goes out; `reconcile` applies the plan, reads
  the queue back and repairs itself (refused step, mismatch, reply lost after
  the change, someone else's edit, cancelled call). Tested against a fake
  device written independently of the planner's reference model; three
  deliberate breakages of the executor were each caught.
- `26bcec7`, `d46bf19` **entry builder** (`entry_builder.py`): URL and DIDL per
  entry from one batch-metadata call (cached), the publish-time container choice
  and the shared DIDL builder (moved to `dlna/didl.py`); no per-track stream
  request.
- `9b4d9c2` **runner** (`queue_sync_runner.py`): one operation at a time per
  speaker; newest reconcile replaces any pending one; a load cancels the running
  operation and absorbs reconciles that arrive meanwhile; failures go to a
  callback.

Hardware validation, real Sonos One (Cocina, muted, 40-entry queue, playing item
20 throughout): the load took 0.99 s with playback starting after the first
batch; play-next insert 1 call / 0.10 s; drag 1 call / 0.04 s; removing three
entries 2 calls / 0.06 s; two fresh shuffles (playing entry first and in the
middle) 5 and 4 calls / 0.25 s; back to original order 6 calls / 0.32 s. After
every step the device queue equalled the desired one and the playing entry had
not restarted (its position kept advancing; only its track number moved).

Findings during the work: removing a range that runs past the end is refused
(UPnP 402, not clamped); `ReorderTracksInQueue` answers with no new `UpdateID`.
Not yet done: the runtime wiring, play-mode (repeat) relay, following the device
(item-change events) and reporting: Phases 4 and 5.


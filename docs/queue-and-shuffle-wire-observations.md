# Queue and shuffle behavior on the wire — live observations

**Status: living document, first entries 2026-10-10.** Everything under
"Observed" below was read directly out of a proxy `RAW_WS` capture; anything
under "Inferred" is a reading of that evidence and is labelled as such. Items
we have not yet seen are listed under "Not yet observed" — do not build on
them as if they were known.

## Why this exists

The next big feature is handing the whole Qobuz queue to the Sonos device and
letting it play and advance on its own (shuffle and repeat included), instead
of this proxy sequencing track by track. That design depends on exactly how
the server announces queues, versions and shuffle, which until now was only
partly known. This doc is where those facts are collected, with the evidence
behind each, so the design can be checked against them and updated as
captures come in. See `connect-activation-model-hypotheses.md` for the
earlier activation/join findings it builds on.

## Method and caveats

- Capture: this proxy's `RAW_WS SEND|RECV <epoch_ms> <base64>` log lines
  (`ws_manager.py`'s `_log_raw_frame`), debug log level, proxy running locally
  (`uv run qobuz-proxy --sonos-auto-discover --hires-downsampling`) with the
  container stopped.
- Decoding: `scripts/qconnect_trace.py --proxy-log` for normalized traces.
  Because it masks ids, the findings below were read with a throwaway
  unnormalized decoder (same proto classes, nothing masked). The ids quoted
  here are real Qobuz track ids and per-load queue item ids, never
  credentials.
- A second capture (2026-10-10 13:13 on) ran on build `b8306e0`, which adds a
  TEMP diagnostic: on every type 96 the proxy sends `ASK_FOR_QUEUE_STATE`
  and logs the reply (`WsManager._observe_shuffle_toggle`; remove once no
  longer needed).
- Setup: Qobuz Android app 10.1.2.0, one Sonos group room ("Baño, Cocina,
  Cuarto, Sala") selected, proxy build `30359d0` on branch `sonos-clean`.
  Single user, single phone, single day — treat each finding as "seen once or
  a few times" unless it says otherwise.
- The user drove the app and noted times; this doc only records what the wire
  showed plus what the user reported seeing in the app.

## Message types seen

| Type | Name | Direction | Notes |
|---|---|---|---|
| 41 | `SrvrRndrSetState` | server → us | Playing state, position, `currentQueueItem`, `nextQueueItem`, `queueVersion` |
| 43 | `SrvrRndrSetActive` | server → us | On join |
| 45 / 46 | `SrvrRndrSetLoopMode` / `SetShuffleMode` | server → us | Seen only at connect, with default (off) values |
| 76 | `CtrlSrvrAskForQueueState` | us → server | Sent on connect, and (TEMP diagnostic) after every type 96 |
| 81 | `SrvrCtrlSessionState` | server → us | On join |
| 82 | `SrvrCtrlRendererStateUpdated` | server → us | Echo of our own state reports |
| 83 | `SrvrCtrlAddRenderer` | server → us | On join |
| 90 | `SrvrCtrlQueueState` | server → us | Full queue snapshot |
| 91 | `SrvrCtrlQueueTracksLoaded` | server → us | Full queue snapshot **and** the play signal |
| 92 | `SrvrCtrlQueueTracksInserted` | server → us | "Play next" insert (O12) |
| 93 | `SrvrCtrlQueueTracksAdded` | server → us | "Add to queue" append (O12) |
| 94 | `SrvrCtrlQueueTracksRemoved` | server → us | Remove by item id (O12) |
| 95 | `SrvrCtrlQueueTracksReordered` | server → us | Move items after an anchor id (O12) |
| 96 | `SrvrCtrlShuffleModeSet` | server → us | Shuffle toggled |
| 97 | `SrvrCtrlLoopModeSet` | server → us | On join, and on every repeat change (O10) |
| 103 | `SrvrCtrlAutoplayTracksLoaded` | server → us | Autoplay suggestions, resent after every queue edit (O12); proxy has no handler |
| 100 | `SrvrCtrlFileAudioQualityChanged` | server → us | Echo of our quality report |
| 1 | `Error` | server → us | See O2 |

## Observed

### O1 — Join on a fresh session
The server answers our `JOIN_SESSION`/`ASK_FOR_QUEUE_STATE` with `SET_ACTIVE`,
a `SET_STATE` whose `currentQueueItem` and `nextQueueItem` are both the
"no track" sentinel (`queueItemId` 2^64−1, `trackId` 2^32−1) at version 0.0,
loop/shuffle off, and a **type 90 with an empty queue and no version**. There
is nothing restored to resume.

### O2 — Connect-time error caused by our own state report
50 ms after joining, our first `RNDR_SRVR_STATE_UPDATED` carried
`currentQueueItemId: 0` and `queueVersion 0.0` with an empty queue. The server
replied `Error code 1: "Current track not found in queue nor autoplay"`.
Zero such errors appear in the 5 earlier connections in the container's
history, which all had restored state; this is new to an empty session.
*Inferred:* the report should not name a current item when nothing is loaded.
Whether this error has any knock-on effect on O3 is unknown. The same
`currentQueueItemId: 0` appears whenever the proxy reports stopped with no
current track, e.g. at 13:43:51 after the queue ended (O21). That one is
*not* wrong in intent: ending on the first track, stopped, is what players do
at the end of an album/playlist, and the server answered by moving its pointer
to item 0 (next = 1). But the proxy only gets there by accident: at the end
`Player._current_track` is set to `None` and the report builder falls back to
the literal `0` (`state_reporter.py`: `current_track.queue_item_id if
current_track else 0`), which only names the first track when the queue's ids
happen to start at 0 (O4). With a base like 164 or 255 the same fallback names
an item that does not exist, exactly the error above; and locally nothing is
loaded afterwards, so a later PLAY has no track to resume (confirmed live,
O22).

### O3 — The first load of an empty session arrived as a type 90, with no play signal
At 12:32:37 the user tapped the first track of a 6-track album. The server sent
only a **type 90** (`queueVersion` 1.1, six tracks, **no `queuePosition`**) and
nothing else for the next ~50 seconds: no type 91, no `SET_STATE`. The proxy
treats type 90 as descriptive (type 91 is the play signal, see
`command_handler.py`), so nothing started and the app sat in an unknown state.
Selecting another track 50 s later produced a normal type 91 and playback.

Related but distinct from the stale-restore case in
`connect-activation-model-hypotheses.md` ("The first track after a fresh start
didn't play"), where the server restored a stale track and a bare PLAY started
it. Here the server restored nothing and the load itself came as a snapshot.
Open: whether this is specific to an empty/new session (version 0 → 1), and
whether O2 is involved.

### O4 — Every later load was a type 91 with these properties
Seen for every load from v2.1 through v13.1 (albums and a playlist):
- The **major** version increments by 1 on every load, the minor is 1.
- `tracks` are listed in play order. Ids are consecutive within a load. In
  loads v1–v9 the first id was 0 (carried as an absent field, which reads as
  0), but **from v10 on loads began at 164** (inferred: the first free id after the
  ~150 ids the autoplay lists had consumed in O12; not confirmed), and a later load (v13)
  started at 164 again. So the base id of a load varies with session history;
  never assume ids start at 0 or equal the list index. Same album loaded twice
  gives the same id→track mapping while the base is unchanged, and id N meant a
  different track in every other queue.
  → a `queueItemId` is only meaningful within one load (one major version).
- `queuePosition` is omitted when it is 0.
- `shuffleMode` is `false` in every load observed, including loads that came
  right after a shuffle-on toggle (see O9 for the explicit test).
- `contextUuid` is a fresh value on every load, even for the identical album.
- Re-tapping a different track in the same, already loaded album is **a new
  load** (new major, new `contextUuid`, `queuePosition` set), not a plain
  `SET_STATE`. Covers scenario 8.

### O5 — `SET_STATE` after a load
After a type 91 the server sends `SET_STATE`s carrying only `queueVersion` and
`nextQueueItem` (the item after the loaded position). The current item comes
from the load's `queuePosition`. Later skips/taps arrive as a `SET_STATE` with
both `currentQueueItem` and `nextQueueItem`, followed by a separate
`playingState: PLAYING`. While playing, the same next-item `SET_STATE` repeats
about every 5 s (it follows our own state report).

### O6 — Server-side shuffle (playlist "Shuffle" button) is invisible on the wire
Offered only on playlists (user report). The 30-track load at v5.1 had
`shuffleMode: false`, no `shufflePivotQueueItemId`, and item ids 0–29 numbered
in play order with track ids in no catalog order. There is no indicator that
the list was shuffled; the queue is simply sent already in play order. The
proxy plays it as given and must not shuffle it again.

### O7 — Toggling shuffle in the player: type 96, minor version bump, no queue resend
Five toggles observed (on the playlist and on two album loads). Each arrives as
a type 96 `SrvrCtrlShuffleModeSet`:
- `queueVersion` **minor** bumps (5.1→5.2, 6.1→6.2→6.3, 7.1→7.2→7.3); the
  **major** never changes.
- Turning on carries `shuffleOn: true`, `shufflePivot` and a fixed32 declared
  as `currentQueueItemId` in our proto. Turning off carries none of these
  (`shuffleOn` is simply absent).
  - `shufflePivot` is the **queueItemId of the current item** (0, 1, 2 in the
    early captures where ids were index-like; 164 in the later ones).
  - The fixed32 in field 4 is **not** an item id: it is a large value that is
    different on every toggle (3047483285, 1358147681, 3227026338, 3219995040,
    3675945687, …) even for the same current item. *Inferred:* it is the
    shuffle seed, and our proto's name for it is wrong. Unconfirmed; not needed
    since the order itself is available (O16).
- No type 90/91 follows, and no renderer-targeted shuffle message (type 46)
  arrives. The current track is not interrupted.
- The only effect the renderer sees is that **`nextQueueItem` changes**.
  With item 1 current and shuffle turned on, next went from 2 to 9; playing on
  gave `1 → 9 → 2`, and Previous from 9 returned to 1. With item 0 current and
  pivot 0, the chain was `0 → 7 → 4 → 8 → 2 → 3`. All consistent with a
  server-held shuffled order that starts at the pivot item.
- The proxy already follows this: each changed next-item re-arms gapless; no
  errors.
- The user reports that tapping the shuffle icon visibly reorders the queue in
  the app.

*Inferred, confirmed in O16:* the server computes and stores the shuffled
order (the `shuffledTrackIndexes` field of a type 90 snapshot) and the renderer
is told only "what's next". The full order is never pushed to us on a toggle;
it has to be asked for.

### O8 — What a version change means
- **Major bump = a new queue was loaded** (O4). Item ids restart, a new track
  is started.
- **Minor bump = an in-place change**: shuffle toggled (O7), tracks inserted,
  added, removed or reordered, autoplay suggestions reloaded (O12). The current
  track keeps playing; only the upcoming order or contents change.
  → "the version moved on, so a new track plays" holds for the major version,
  not the minor.

### O9 — Loading a new queue resets shuffle (shuffle on, then load an album)
Shuffle was switched on while an earlier album was loaded and paused (12:53:57,
type 96, v7.4, `shufflePivot` 2). Seven seconds later an album was started:
a type 91 at v8.1 with `shuffleMode: false`. The next `SET_STATE` named item 1,
the natural successor — i.e. the new queue is **not** shuffled. **No type 96
"off" followed**: the load itself carries the reset. The user saw the app's
shuffle icon turn off by itself once playback started.
*Inferred:* shuffle is a property of one loaded queue, not of the session, and
starting a different queue clears it server-side. Consequence for the proxy:
on every major-version bump, shuffle state must be reset to off (and the
type 91's `shuffleMode` read as the truth for the new queue).

### O10 — Repeat: type 97, no version bump, no renderer-targeted message
Cycled off → all → one → off in the player (12:55:18, 12:55:31, 12:55:34).
Each change arrived as a single type 97 `SrvrCtrlLoopModeSet` carrying only
`mode` (`LOOP_MODE_REPEAT_ALL`, `LOOP_MODE_REPEAT_ONE`, `LOOP_MODE_OFF`).
- **The queue version did not move** (stayed 8.1 throughout) — no major, no
  minor bump. Unlike shuffle (O7), repeat is not part of the versioned queue.
- **No type 45** (`SrvrRndrSetLoopMode`, the renderer-targeted one) arrived;
  that type appears only once, at connect (O1).
- `nextQueueItem` did not change: with repeat-one active it still named item 1,
  not the current item, and with repeat-all it still named the natural
  successor. The server does not encode repeat in the next pointer (at least
  mid-queue); honoring repeat is up to the renderer.
- **The proxy currently ignores these.** `command_handler.py` only handles type
  45 (`MSG_TYPE_SET_LOOP_MODE`); types 96 and 97 log "No handler for message
  type …". So after connect, repeat changes made in the app never reach
  `QobuzQueue`/`Player`, and repeat-one / repeat-all behavior in
  `_handle_track_ended` runs on whatever mode was set at connect.

### O11 — A track ending under repeat-one: the server does nothing; repeat is the renderer's job
Repeat-all was tapped at 12:59:59 and repeat-one at 13:00:02 (type 97, v8.1
unchanged, ignored by the proxy — O10). The user then seeked to 298 s of a
304 s track (a seek arrives as a bare `SET_STATE { currentPosition: … }`) and
let it end.
- **Nothing came from the server when the track ended** — no `SET_STATE`, no
  version change. The renderer is entirely responsible for honoring repeat.
  Here the proxy had not applied it (O10), so Sonos simply advanced into the
  gapless-armed next track (item 1) at 13:00:15, and the proxy reported
  `currentQueueItemId: 1` with the new duration. The user's reading is correct:
  repeat-one has to be done on the renderer side, never by the server.
- **The server's `nextQueueItem` then stayed on item 1 — the track that was
  now current — for at least 8 s** (13:00:15 → 13:00:23, until the user
  paused), even though every one of our reports named item 1 as current. For
  contrast, the previous day's natural album playthrough (container log, repeat
  off) advanced to a *different* next track each time. So with repeat-one
  active the server's pointer did not follow the renderer's reported current
  item (or at least not within 8 s). Reason unknown.
- Side effect in the proxy: it took that next=1 at face value and re-armed the
  already-playing track as the "next" track (`Gapless: next track info changed,
  re-arming` → track 418651525, the one playing). Confirmed at 13:01:41: when
  that track ended, Sonos "transitioned" into 418651525 again — a replay of the
  same track. Accidental, not intended repeat behavior.

### O12 — In-place queue edits: all minor bumps, delta messages, no interruption
Scenarios 6 and 7, on a freshly loaded 12-track album (v9.1, items 0–11, item 0
playing). Every edit bumped only the **minor** version (9.2 … 9.9); the major
stayed 9; the current track kept playing and the proxy never reloaded it.

| Time | Action (user) | Message | Fields |
|---|---|---|---|
| 13:02:30 | "Play next" a track | 92 Inserted, v9.2 | 1 track `queueItemId` 12, `insertAfter: 0` |
| 13:02:37 | "Add to queue" a track | 93 Added, v9.3 | 1 track `queueItemId` 13 (appended at the end) |
| 13:03:04 | Drag item 8 to the end | 95 Reordered, v9.4 | `queueItemIds: [8]`, `insertAfter: 13` |
| 13:03:18 | Remove item 9 | 94 Removed, v9.6 | `queueItemIds: [9]` |
| 13:03:23 | Drag item 7 to the top of "up next" | 95 Reordered, v9.8 | `queueItemIds: [7]`, `insertAfter: 0` |

- **`insertAfter` is a `queueItemId`, not an index** (13 = the just-added last
  item; 0 = the current item, i.e. "right after the playing track"). How "move
  to the very front" is encoded when there is no current item is unknown.
- **New items get fresh ids continuing the sequence** (12, 13 after 0–11), and
  ids are not reused after a removal. Ids are therefore unique within one load.
- **Autoplay noise:** after every edit the server also sends a type 103
  `SrvrCtrlAutoplayTracksLoaded` (own minor bump: 9.5, 9.7, 9.9) with a **new
  set of ~50 suggestions, each given fresh item ids** (14–63, then 64–113, then
  114–…). The earlier suggestions are replaced wholesale. These suggestions are
  not part of the queue proper; the proxy has no handler for type 103.
- **The next pointer follows ~0.5–1 s later** as a `SET_STATE` with only
  `nextQueueItem`: 12 after the insert, unchanged (12) after the append, still 12
  after the first reorder, then 7 after item 7 was moved to the top. The proxy
  re-armed gapless correctly each time (the device's own queue entry for the old
  next was replaced).
- A full snapshot is not resent for edits; the only way to know the whole
  order after a sequence of edits is to apply these deltas to the load's
  snapshot (or to ask for queue state).

### O13 — Tapping another track in the same album (scenario 8)
Three consecutive loads of the same 15-track album: v10.1 (position 0), v11.1
(position 4), v12.1 (position 8). Each is a **full type 91** with the same
track list and the same item ids (164–178), a new `contextUuid`, and
`queuePosition` naming the tapped track (omitted for 0). No in-place "jump"
message exists: selecting a different track of the loaded queue is always a
new major version. After each load the server's next pointer is the item after
the tapped one (165, 169, 173).

### O14 — Next and Previous (scenario 9)
- **Next:** `SET_STATE` carrying `currentQueueItem` (the next item) and
  `nextQueueItem` (the one after), then a separate `playingState: PLAYING`
  ~50 ms later, then the usual next-only echo. Version unchanged (12.1). Seen
  at 13:06:32 (172 → 173) and 13:06:35 (173 → 174).
- **Previous after a few seconds of playback restarts the track:** a bare
  `SET_STATE { currentPosition: 0 }` and a `PLAYING`, in either order (13:06:38,
  13:06:45, 13:06:47). No item change.
- **Previous again right away goes to the previous item:** `SET_STATE` with
  `currentQueueItem` 173 / `nextQueueItem` 174 (13:06:48), one second after the
  last restart. So the app (not the server) decides restart vs. go-back, and
  expresses it either as a position-0 seek or as an item change.
- The proxy handled all of it correctly (seek to 0, track loads, gapless
  re-arm). Neither next nor previous bumps the queue version.

### O15 — Switching between queues (scenario 10)
Three loads ten seconds apart: v13.1 (12-track album), v14.1 (a different,
14-track queue whose track ids are non-consecutive — a playlist), v15.1 (the
first album again, byte-identical track list to v13). Each is a full type 91,
`queuePosition` 0, `shuffleMode: false`, new major, next pointer = item 165.
- **All three queues use the same base id 164**, so `queueItemId` 164 named
  three different tracks in a row (435839227, 434656618, 435839227 again). The
  proxy coped because its gapless keys also contain the track id, but any key,
  URL or cache built from the item id alone would collide here. This is the
  concrete reason item ids must be scoped by major version (and session).
- The base stayed at 164 for every load after v9 rather than moving on, so it
  looks frozen per session by the autoplay activity, not a running counter.
  (Inferred.)
- Nothing special happens when switching queues while playing: no stop, no
  clear message — the new load simply replaces the old queue (and, per O9,
  its shuffle state).

### O16 — The shuffled order is in a type 90 snapshot after the toggle
Build `b8306e0` asks for the queue state right after each type 96. On an
8-track queue (v16.1, item ids 164–171), the replies 40–50 ms later:

| Toggle | Reply | `shuffleMode` | `shuffledTrackIndexes` |
|---|---|---|---|
| 13:15:29 on (v16.2, pivot 164) | type 90, v16.2 | true | `[0, 1, 6, 3, 7, 4, 2, 5]` |
| 13:15:35 off (v16.3) | type 90, v16.3 | absent | empty |
| 13:15:43 on (v16.4, pivot 164) | type 90, v16.4 | true | `[0, 4, 1, 7, 2, 3, 5, 6]` |

- `tracks` stays in the **original order** (ids 164…171); the shuffled order is
  a separate list of **indexes into `tracks`**, in play order, starting with
  the pivot (index 0 = item 164 both times).
- **Every toggle on produces a new, different shuffle.** Toggling off then on
  again does not restore the previous shuffled order.
- **Toggling off** gives a snapshot with no shuffle fields and an empty index
  list: back to the original order. The next pointer after off was the natural
  successor (165).
- **The index list matches the server's next pointers exactly.** With
  `[0, 4, 1, 7, 2, …]` over ids 164…171, i.e. play order
  164, 168, 165, 171, 166, …: current 164 → next 168 (13:15:46.0); current 168 →
  next 165; current 165 → next 171; Previous from 165 went to 168 and again to
  164, each with the matching next. Next/Previous walk this list.
- The snapshot's `actionUuid` is empty and `autoplayMode`/`autoplayTracks` were
  absent, i.e. this is the "ask" form of the snapshot, not a reply to an edit.
- An unrelated early observation from the same capture: reconnecting after a
  proxy restart restored the previous session (paused on item 164, v15.1) and
  the connect-time type 90 carried the full 12-track list with no shuffle
  fields, so a reconnect during an active shuffle is expected to include the
  shuffle state (not yet tested).

### O17 — Repeat-all: the server wraps the next pointer itself
On an 11-track album (v17.1, ids 164–174) repeat-all was set at 13:22:35 (two
type 97, mode 3). The user then tapped the last track (v18.1, position 10,
item 174), seeked to 158.9 s of its 162 s, and let it end.
- **With item 174 (the last) current, the server's `nextQueueItem` was item
  164 — the first track.** The server encodes the wrap in the next pointer,
  unlike repeat-one (O11), where the next pointer stayed the natural successor.
- The proxy never saw the repeat change (O10) and did not need to: it armed
  the "next" track it was given (item 164), Sonos transitioned into it at
  13:22:52.7, the proxy reported `currentQueueItemId: 164` and the server
  replied 40 ms later with next = 165, i.e. the pointer followed the renderer
  normally. The user heard the album start over.
- So **repeat-all needs no renderer-side logic when the renderer simply follows
  the server's next pointer; repeat-one does.** Under the full-queue handover,
  the device sees the whole queue and wraps on its own (REPEAT_ALL), which has
  to agree with what the server expects next (after the wrap: item 165).
- *Inferred, not tested:* with repeat-one on, the server's next pointer does
  not follow the renderer's reported current item (O11), which is why repeat-one
  must be implemented on the renderer.

### O18 — Edits while shuffle is on: the wire is silent about the new order
On a 7-track queue (v20.1, ids 164–170), shuffle was switched on at 13:24:26
(v20.2, pivot 164). The snapshot requested by the diagnostic gave
`shuffledTrackIndexes [0, 3, 2, 1, 4, 6, 5]`, i.e. play order
164, 167, 166, 165, 168, 170, 169, and the server's next pointer was 167.
Then two edits:
- 13:24:31 remove item 170 (type 94, v20.3) — the second-to-last item of the
  shuffled order.
- 13:24:35 move item 166 to after item 169 (type 95, v20.5) — to the end of the
  original-order list.

Each was followed by a type 103 autoplay reload (v20.4, v20.6) and nothing
else. Specifically: **no snapshot, no shuffle message, no new indexes**, and the
next pointer stayed 167 throughout (consistent with both edits, which only
touched later positions). The delta messages are expressed in terms of the
**original-order** list (`insertAfter: 169` is the last item there); how the
shuffled order absorbs a removal or a reorder is not announced. A removal is
unambiguous (drop it from the order); a reorder in the original list may or
may not move the item in the shuffled order — answered in O19: reorders and
removals act on the shuffled order.
- Not captured: insert ("play next") and add ("add to queue") while shuffle is
  on. Those carry a `shuffleSeed` field (types 92/93) which is presumably how
  the server places new tracks into the shuffled order; none was seen here.
- Side observations: (1) the reply to the on-demand snapshot request was
  followed by a type 97 carrying the current loop mode (13:24:26.173, mode 1 =
  off), the same pairing as at connect — a way to read the current repeat mode
  on demand. (2) Type 97 messages arrive twice, ~40 ms apart (13:22:35,
  13:24:07, 13:24:08), probably one per role of our dual join (inferred).
  Loop mode values seen: 1 = off, 2 = repeat one, 3 = repeat all.

### O19 — How the shuffled order reacts to edits (snapshot after every edit)
Build `ad7cf02` asks for a snapshot after every type 92–96. Capture
`capture-proxy-3` (13:30–13:32), shuffle on over an 11-track queue
(v21.1, ids 255–265). Snapshot order = `shuffledTrackIndexes` applied to
`tracks`; "next" = the server's next pointer right after.

| Time | Action | Message | Resulting shuffled order (current = 262) | Next |
|---|---|---|---|---|
| 13:30:48 | shuffle on | 96 (v21.2) | 255, 262, 256, 259, 261, 265, 260, 264, 263, 258, 257 | 262 |
| 13:31:11 | add 266 | 93 (v21.3, seed 4800478) | … 257, **266** (appended at the end) | 256 |
| 13:31:19 | play next 267, anchor 262 | 92 (v21.4, seed 1962102700) | 255, 262, **266**, 256, 259, 261, 264, 260, 263, **267**, 258, 257, 265 | 266 |
| 13:31:29 | move 267 after 267 | 95 (v21.5) | unchanged (a no-op drag) | — |
| 13:31:31 | remove 266 | 94 (v21.6) | 266 simply dropped | 256 |
| 13:31:39 | move 256 after 260 | 95 (v21.7) | 256 moved to just after 260 | 259 |
| 13:31:44 | move 261 after 262 | 95 (v21.8) | 261 placed right after the current item | — |
| 13:31:46 | move 260 after 262 | 95 (v21.9) | 260 placed right after the current item | — |
| 13:31:48 | move 259 after 260 | 95 (v21.10) | 259 placed right after 260 | 260 |
| 13:31:52 | move 256 after 262 | 95 (v21.11) | 256 placed right after the current item | 256 |
| 13:31:54 | remove 256 | 94 (v21.12) | 256 dropped | 260 |
| 13:32:00 | shuffle off | 96 (v21.13) | original order restored | 267 |

What this shows:
- **Reorder and remove while shuffle is on act on the *shuffled* sequence.**
  The `insertAfter` anchor and the moved ids refer to positions in the shuffled
  order (the order the app displays), not the original list. `tracks` stayed
  in the original order throughout (only 266/256 dropped and 267 added), while
  `shuffledTrackIndexes` changed. The indexes are renumbered after a removal.
  (This also explains O18: the reorder there applied to the shuffled order.)
- **Add** appends to the end of both lists.
- **Insert ("play next") while shuffle is on is not a plain insert.** 267 landed
  right after 262 in the *original* list (as the shuffle-off snapshot confirms,
  and as next became 267 after turning shuffle off), but in the *shuffled*
  order it did not become the next track: the upcoming part of the order was
  re-arranged (266 jumped from last to first, 265 went from 4th to last, and
  267 ended up 8th). Looks like a seeded re-shuffle of the upcoming tracks
  (type 92 carries a `shuffleSeed`); the exact rule is unknown and not
  predictable from the delta alone. The server's next pointer after the insert
  (266) agrees with the new snapshot.
- Every snapshot agreed exactly with the server's subsequent next pointers
  (256, 259, 260, 256, 260, 267 …), so the snapshot is authoritative.
- **A reconnect while shuffle is on delivers the shuffle state.** After the
  proxy restart (13:30:24) the connect-time snapshot had `shuffleMode: true` and
  `shuffledTrackIndexes` for the restored queue (v20.6).
- The server sent **no** type 103 autoplay reloads in this capture (they
  followed every edit in O12). Autoplay behavior seems to vary by session or
  setting.

### O20 — The end of the queue runs into the autoplay list, not into silence
Two runs to the end of the queue with repeat off and shuffle off
(capture-proxy-3, 13:37–13:39):
- 13:37:30, v23.1: the last track (item 262, position 7 of 8) was loaded. The
  server's next pointer was **item 213, a track that is not in the queue**
  (queue ids were 255–262); trackId 2839518 (Joshua Redman Quartet). It
  played after the last track, gaplessly, and on entering it the version moved
  to v23.2 and the next pointer to **item 214** (trackId 4298860).
- 13:38:33, v25.1 (a different 11-track queue, last track selected): next =
  **214** (4298860), then v25.2 and next **215** (734936) when it started.
- Items 213, 214, 215 with exactly those trackIds are the first entries of the
  autoplay list (type 103, v20.6, ids 213–254, 42 tracks) the server sent at
  13:24:36 in the previous run. The proxy did not even receive that list in this
  run (it had restarted since); the next pointer alone carries the full track
  reference, so the proxy simply armed and played it.
- **The autoplay list is session state, not queue state**: it survived three
  loads (v21 → v25) and its position advanced across them (213 consumed first,
  then 214, then 215), independent of what the queue contained.
- The user did not recognize the tracks and the app did not show them. They
  report that the end of the queue "used to work and stop playback". *Inferred:*
  autoplay lists get created by queue edits (in the very first capture the
  autoplay lists appeared right after the first insert at 13:02:30, never
  before); a session with no edits has none, so the end of the queue stops. Here
  an edit earlier in the session (13:24) had created one that kept being served
  at every queue end. The queue snapshots never carried `autoplayMode`,
  `autoplayLoading` or `autoplayTracks`, so the state of the app's autoplay
  setting is not visible on the wire so far; type 102 (`SrvrCtrlAutoplayModeSet`)
  has not been seen.
- Implication for any queue handover: the next pointer can name an item that
  is **not in the queue** (an autoplay track) once the last item is playing.
  The server's next pointer stays authoritative for "what comes after the end".

### O21 — Autoplay toggled on then off clears the leftover state; the queue end then stops
The user confirms the app's autoplay setting was **off from the start** of the
whole session, yet O20 shows the server serving autoplay lists after edits and
continuing into them at the queue end. At 13:42:48 the user switched autoplay
on and immediately off again:

| Time | Message | Detail |
|---|---|---|
| 13:42:48.8 | 102 `SrvrCtrlAutoplayModeSet` | v25.3, autoplay **on**, loading |
| 13:42:49.4 | 103 `SrvrCtrlAutoplayTracksLoaded` | v25.4, 50 new suggestions, ids 266–315 (allocated right after the queue's ids 255–265) |
| 13:42:50.3 | 102 | v25.5, autoplay **off** |
| 13:42:54 | `SET_STATE` | current = last item 265; next **still 214**, the stale pointer into the *old* list |
| 13:43:08 | transition | Sonos moved into 214 (stale autoplay track); the server replied `nextQueueItem` = **the no-track sentinel** (`queueItemId` 2^64−1, `trackId` 2^32−1) at v25.5 |
| 13:43:27 | type 91 v26.1 | ids restart at **0** (`[0..7]`) |
| 13:43:32 | type 91 v27.1, last item | next = **sentinel**; the track ended and playback **stopped** as before |
| 13:43:51 | our stopped report | `currentQueueItemId: 0` (the first item here only because the base was 0; see O2); the server answered with next = 1 |

- So the **leftover autoplay pointer survives a toggle** until it is consumed
  once (here the stale item 214 played), then next becomes the sentinel; the end
  of a queue with no autoplay list is **a sentinel next pointer, and the player
  stops**. That is the "used to work and stop" behavior.
- The **id base returned to 0** once autoplay state was reset (base 164 / 255
  earlier = the autoplay id counter's high-water mark, see O4). *Inferred.*
- **Why the server was in an autoplay state with the setting off is unknown**:
  possibilities are a server-side flag left on by an earlier session or client,
  or lists created after edits regardless of the setting. Either way, the end
  of the queue can run into autoplay even though the app shows autoplay off.
- **Wire layout of `SrvrCtrlAutoplayModeSet` (type 102) differed from our
  proto (fixed in `0c09493`).** The raw frames are: field 1 queueVersion, field 2 actionUuid, field 3
  bool (set = on, absent = off), field 4 bool (`1` = loading, set in both
  messages). The old `protos/qconnect_payload.proto` declared a single
  `autoplayOn = 1`, so both messages decoded as empty and were
  indistinguishable (layout inferred from the raw bytes `…1801 2001` for on
  and `…2001` for off; field 4 is now `autoplayFlag`, meaning unknown).
- Type 102 carries a queue version, and each of its changes (on, list load,
  off) bumped the minor: another case of a minor bump with the current track
  untouched.

### O22 — Play after the end of the queue does nothing, and the app is not told
After the queue ended and the proxy reported stopped on "item 0" (O21), the user
pressed play at 13:51:07 (7 minutes later, same session, v27.1). The server
sent a **bare** `SET_STATE { playingState: PLAYING }` — no `currentQueueItem`,
no position — as it always does for a resume. The proxy logged
`Play command, current state: 1` then `WARNING No track to play - nothing
loaded` and returned. **Nothing was sent back**: no state report, no error.
The app, which flips its play button optimistically, therefore kept showing
"playing" with no sound and no correction, and the proxy stayed silent until the
next heartbeat. (Every other failing branch of `Player._play_locked` — e.g.
a rejected resume — pushes the real state with `_send_state_update()`; this
"nothing loaded" branch alone does not.)
Two separate faults: (1) at the end of the queue the proxy keeps no track to
resume (see O2, design implication 10); (2) when PLAY cannot be honored, the
app must be told immediately (report stopped), not left hanging.

## Not yet observed

- ~~A type 91 with `shuffleMode: true` and/or `shufflePivotQueueItemId`.~~
  Dropped (2026-10-10): not produced by switching shuffle on first (O9) nor by
  the playlist "Shuffle" button (O6); the user recalls having seen such a
  "player must shuffle" variant earlier but cannot reproduce it and has decided
  to disregard it. If a load with `shuffleMode: true` ever shows up, the
  safe handling is to treat the load as unshuffled and ask for a snapshot
  (O16), whose `shuffledTrackIndexes` is the truth.
- What the server's next pointer does when the renderer *does* honor repeat-one
  (stays on the same item and reports the same current). O11 only covers the
  case where the proxy did not apply repeat.
- The exact rule by which a seeded insert re-arranges the upcoming order
  (O19). (Removing or reordering the current track is not possible in the
  app's UI, per the user.) Edits with shuffle on at a repeat mode other than
  off.
- Whether the session uuid changes inside a live session (not observed to
  change in this capture).

## Implications for the Sonos queue-handover design (so far)

1. Short-lived, version-scoped URLs are consistent with the data: item ids
   are per-load (O4), so a URL can only be identified by (session, major
   version, item id). A major bump invalidates everything; that matches "a new
   queue means a new track".
2. A minor bump must **not** invalidate the playing entry (O8). Shuffle toggle
   therefore means reordering the device queue around the playing item without
   interrupting it.
3. The shuffled order is not in the load or the toggle (O6, O7) but **is** in
   the type 90 snapshot requested after a toggle (O16): `shuffleMode` plus
   `shuffledTrackIndexes` over the original-order `tracks`. Handing the whole
   shuffled queue to Sonos is therefore possible by publishing it in that
   order (play mode NORMAL/REPEAT_*), which also keeps Sonos's next/previous
   identical to the server's next pointers. Sonos's own SHUFFLE mode could not
   reproduce this order, so it should not be used for Qobuz shuffle. A new
   toggle on always means a new order, so the device queue must be rebuilt
   around the playing item each time.
3b. **The snapshot is the source of truth; deltas are only triggers.** With
   shuffle on, reorders and removals act on the shuffled sequence, adds append,
   and a "play next" insert re-arranges the upcoming order in a way not
   derivable from the delta (O19). Re-asking for the snapshot after every edit
   (a reply came ~40–50 ms later every time, and always agreed with the
   server's next pointer) avoids having to model any of that, in either
   shuffle mode. Delta semantics need not be implemented at all.
4. The queue model must accept a first load that arrives as a type 90 (O3),
   and decide separately what makes it "play".
5. Our connect-time state report should not claim a current item for an empty
   queue (O2).
6. Shuffle belongs to one loaded queue and is reset by the next load (O9), so
   the proxy's shuffle state must be reset on every major bump, with the load's
   own `shuffleMode` as the source of truth.
7. Repeat is session-level, not versioned (O10): it can change at any moment
   without a version bump, so it must be relayed to the device immediately and
   independently of the queue publish step, and the proxy needs a type 97
   handler first (it has none today). The server does not implement repeat
   (O11): honoring it is the renderer's job, which on Sonos means mapping it
   to the device's native play mode (NORMAL / REPEAT_ALL / REPEAT_ONE). Repeat-
   all is the easy case, since the server already wraps its next pointer (O17);
   repeat-one is the one the renderer must implement.
   Today's gapless-armed next track must also be disarmed or retargeted while
   repeat-one is active, or the device will advance past the current track.
8. All in-place edits are minor-version deltas (O12), so the device queue must
   be maintained incrementally from insert/add/remove/reorder messages applied
   to the load's snapshot, without touching the playing entry. The delta
   anchors are item ids, so an ordered model keyed by item id is enough. Autoplay
   reloads (type 103) bump the minor version too, so a minor bump alone does not
   mean the playing queue changed.

9. The next pointer can leave the queue (O20): when the last item plays the
   server names an autoplay item that is not in the snapshot, with a full track
   reference. A handover must either keep following the next pointer for that
   tail (append it to the device queue as a one-off entry) or deliberately stop
   at the last item. With no autoplay state the next pointer is the no-track
   sentinel and playback simply stops (O21); the server's own state, not the
   app's displayed setting (O21), decides which one happens, so following the
   next pointer is the only reliable rule.

10. End of queue: the player should land on the **first track of the play
   order, stopped, position 0** and report that item's real id and duration
   (what players normally do, and what the server evidently expects, O21),
   not "nothing loaded" with a literal 0. This needs the ordered queue model,
   which the handover design builds anyway; today it only works by accident
   when ids start at 0 (O2), and a play press afterwards fails silently (O22).
   Independently of that, every command that cannot be honored must be
   answered with a state report so the app's optimistic UI is corrected.

## Capture log

| # | Scenario | Date | Result |
|---|---|---|---|
| 1 | Play an album from the start | 2026-10-10 | O1–O5 (first attempt hung, see O3) |
| 2 | Server-side shuffle (playlist) | 2026-10-10 | O6 |
| 3, 4 | Toggle shuffle on / off | 2026-10-10 | O7, O8 |
| 8 | Tap another track in the same album | 2026-10-10 | O13 |
| 3b | Shuffle on, then load an album | 2026-10-10 | O9 (load resets shuffle) |
| 3c | Shuffle toggles with the snapshot diagnostic | 2026-10-10 | O16 (order available) |
| 3d | Shuffle on, then remove and reorder | 2026-10-10 | O18 (wire silent about the new order) |
| 3e | Shuffle on, then insert/add/remove/reorder with the snapshot diagnostic | 2026-10-10 | O19 |
| 5 | Repeat modes in sequence | 2026-10-10 | O10 (type 97, no version bump, proxy ignores it) |
| 5b | Repeat-one, let the track end | 2026-10-10 | O11 (server silent; renderer must repeat; next pointer stuck) |
| 5c | Repeat-all at the end of a queue | 2026-10-10 | O17 (server wraps the next pointer) |
| 11 | Repeat off, run to the end of the queue (twice) | 2026-10-10 | O20 (autoplay takes over) |
| 12 | Autoplay on then off, run to the end again | 2026-10-10 | O21 (state cleared; queue end stops) |
| 13 | Press play after the queue ended | 2026-10-10 | O22 (nothing loaded; app not told) |
| 6, 7 | Add to queue / play next / remove / reorder | 2026-10-10 | O12 |
| 9 | Next / previous | 2026-10-10 | O14 |
| 10 | Switch between playlist/albums | 2026-10-10 | O15 |

Scenario list: see the scenario checklist kept with the capture (not in the
repo).

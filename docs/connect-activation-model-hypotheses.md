# Working hypotheses: how Connect renderer activation actually works

**Status: work-in-progress. Checked against one web-player HAR capture
(`qobuz-player.har`), one proxy RAW_WS capture (`qobuz-queue.log`), a
second web-player HAR captured from a genuinely fresh login
(`qobuz-player-full-login.har`), and a third pair capturing a handoff away
from this proxy (`qobuz-player-take-from-proxy.har` + a matching
`qobuz-queue.log`) — all 2026-09-13, via `scripts/qconnect_trace.py`.**
The bug the third capture found (see "Fixes implemented" below) is now
fixed, unit-tested, and confirmed live: tap-to-play, seeking, and album
switching all work with the controller role active. A further, separate
symptom this surfaced (a fresh process's first tap playing an
already-loaded track instead of the newly-tapped one) turned out, on
reflection, not to be a server bug at all — see "The first track after a
fresh start didn't play" below for the full arc, including a first
attempted fix (gating `isActive`) that broke real device activation and
was reverted, and the final correction: the "stale restore" framing was
wrong throughout, and there is no known bug left to fix here. This doc
exists to hold the hypotheses and the evidence for/against each
one steady across sessions, not to declare the investigation finished.

## Context

This proxy joins the Connect session as both Renderer and Controller under
the same deviceUuid (see `ws_manager.py`'s own module-level comment for the
history of getting the *identity* right). Despite that, immediate track
switching only ever worked with the Controller join removed entirely (see
git log around 2026-09-13), and the recurring "Renderer state updated
message received from non active renderer" protocol error was never fully
explained. `scripts/qconnect_trace.py` was built to compare, frame for
frame, what a real client (the web player) does against what this proxy
does, hashed so identical values are still visibly identical without
exposing the real ones (`_RUN_SALT` — see its own comment).

## The hypotheses (as stated, 2026-09-13)

1. There's only one Connect session per user at a time.
2. There's exactly one active renderer at a time, session-wide.
3. When a Sonos group is selected over UPnP (i.e. this proxy's own
   discovery layer picks it up), it joins the Connect session like any
   other controller/renderer — a normal `JOIN_SESSION`.
4. If a controller (the app, the web player, ...) activates one of our
   speakers, it becomes the active renderer in the session — the renderer
   itself does **not** need to separately (re-)activate itself against
   Qobuz. It only needs to (a) actually activate the physical device and
   (b) report its current state.
5. Whenever a renderer is selected, this is notified to *all* controllers.
   We need to watch for that message and activate/deactivate our own
   speakers accordingly.
6. For other actions (volume, play, stop, seek, ...): any controller can
   initiate them; the server notifies all controllers and the active
   renderer.
7. When active, we react to such action messages by driving the physical
   speaker — we don't need to re-notify the action, since it's already
   known session-wide by the time it reaches us.
8. What we *do* need to originate ourselves is state changes that happen
   outside Qobuz's own control — e.g. a volume or playback change made
   directly through the Sonos app.

## First pass against the evidence

### #1 — one session per user

**Already confirmed independently, well before this doc** (2026-09-12
investigation): the phone app and a browser tab were made to coexist on
the same session directly. Nothing new to check here.

### #2 — exactly one active renderer

**Consistent with everything observed, not independently proven.** Every
capture this project has shows at most one `SRVR_RNDR_SET_ACTIVE(true)`
and one `SRVR_CTRL_ACTIVE_RENDERER_CHANGED` per activation event, never
two renderers simultaneously reporting active. No capture exists yet of
two controllers racing to activate different renderers, which would be
the real test.

### #3 — a freshly-discovered Sonos group joins like any other renderer

Not testable from a web-player capture — this is about our own side, and
it's already what `_send_join_session()` does today. No change implied.

### #4 — renderer doesn't self-activate; it reports state once told

**Strongly supported.** The web player's capture never sends
`RNDR_SRVR_JOIN_SESSION` at all — the identity it ends up driving
(`rendererId` X) was already in the very first `SRVR_CTRL_ADD_RENDERER`
dump, before the page had done anything. Activation instead looks like:

```
[controller] SEND CTRL_SRVR_SET_ACTIVE_RENDERER { rendererId: X }
[renderer]   SEND RNDR_SRVR_VOLUME_MUTED
[renderer]   SEND RNDR_SRVR_VOLUME_CHANGED
[renderer]   SEND RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED
[renderer]   SEND RNDR_SRVR_STATE_UPDATED
             RECV SRVR_RNDR_SET_ACTIVE            <- confirmation arrives AFTER
             RECV SRVR_CTRL_ACTIVE_RENDERER_CHANGED
```

Confirmed with the *hashed* rendererId matching exactly between the
`ADD_RENDERER` entry and the `SET_ACTIVE_RENDERER` call (see
`scripts/qconnect_trace.py`'s hashing) — this is the browser's own,
already-known, long-lived identity being reactivated, not a fresh one
being introduced. The renderer side reports its state *before* even
receiving the server's own activation confirmation — it doesn't wait to
be told, it just announces itself once its own controller says so.

This capture only shows the "a client activates *itself*" case, not "a
different controller (the phone app) activates *us*" — but the
`SRVR_RNDR_SET_ACTIVE` arriving on our own renderer connection when the
phone app picks our room has been confirmed, repeatedly, all session —
so the receiving half of this was never in doubt. What's new here is the
*sending* half: a real client never claims `isActive` through its own
`JOIN_SESSION`, only through a separate, explicit `SET_ACTIVE_RENDERER`
Controller command (for a *fresh* identity, isActive on the join itself
may still be how activation works — untested, no capture yet of a truly
brand-new identity's first-ever join).

### #5 — watch for the active-renderer broadcast, act on it

**Partially checked; one real nuance found.** The web player's own
capture does receive `SRVR_CTRL_ACTIVE_RENDERER_CHANGED` once, right
after its own activation. But our own proxy's matching capture
(`qobuz-queue.log`, same day) never received it at all for its own
activation — only the direct `SRVR_RNDR_SET_ACTIVE`. Plausible reading:
the server may skip sending the *broadcast* to the same connection that
already got the *direct* renderer-targeted message, to avoid a redundant
self-notification — since this proxy registers Controller and Renderer
on the same socket, it may never see its own activation as a broadcast,
only as the direct message it already handles. The broadcast would still
matter for the cases this proxy has never handled: noticing *another*
renderer becoming active (so it can deactivate itself) or a different
speaker of ours becoming active while idle. No handler exists for
message type 86 today (`register_handler` is never called for it) — this
needs one regardless of the self-activation nuance above.

### #6, #7 — actions are broadcast; act, don't re-notify

**Consistent with the existing architecture already.** This is how
`command_handler.py`/`volume_handler.py` already behave — receive
`SRVR_RNDR_SET_*`, drive the device, report resulting state via
`RNDR_SRVR_*_CHANGED`/`_STATE_UPDATED`, never echo an explicit ack of the
command itself. Nothing new implied.

### #8 — originate reports only for externally-caused state changes

**Consistent with the existing architecture already** (`state_reporter.py`
polls the physical device and reports drift; volume changes detected from
the device get reported outward). Caveat: the volume-echo-loop bug from
2026-09-12/13 shows this reporting path still has an unresolved
interaction with the Controller role somewhere — fixed defensively
(same-value no-op, coalescing, backend-forwarding disabled) without ever
finding the actual trigger. Worth re-examining once activation itself is
fixed, in case it was a symptom of the same underlying issue.

## A fresh login still sends no JOIN_SESSION at all (2026-09-13, second capture)

`qobuz-player-full-login.har` — a capture starting from actual login, not
just a fresh page load — settles part of #4/#5 further and reopens the
scope of the whole investigation. In this capture:

- `AUTHENTICATE` + `SUBSCRIBE` fire, then `ADD_RENDERER` lists three
  renderers **including "Web Player Chrome" already** — the same
  persistent identity as the other capture, present *before* this login
  session had done anything at all. It survives a real login/logout
  cycle, not just a page reload.
- The web player spends the entire capture acting purely as a
  Controller — repeatedly sending `CTRL_SRVR_ASK_FOR_RENDERER_STATE` and
  receiving state/volume/quality updates for the renderer that was
  *already* active (this proxy's own Sonos group, already playing) — and
  **never sends `RNDR_SRVR_JOIN_SESSION` or `CTRL_SRVR_JOIN_SESSION`
  once, anywhere.**

That's two independent captures now, in two different roles (pure
Controller here; Controller-then-self-activated-Renderer in the other),
neither ever sending `JOIN_SESSION`. `AUTHENTICATE`+`SUBSCRIBE` alone was
sufficient both times to be a fully-functioning participant. This pushes
harder on the open question below about what `JOIN_SESSION` is actually
*for* — increasingly looks like it may not be needed at all for an
identity the server already knows, which is the position every one of
our own Sonos rooms is in from their second use onward (deterministic
`deviceUuid` per `group_id`), yet this proxy sends it unconditionally on
every single connect regardless.

## The other side of a handoff — captured (2026-09-13, third capture)

`qobuz-player-take-from-proxy.har` + a matching proxy RAW_WS capture
(`qobuz-queue.log`, same session): started playback on one of this
proxy's own Sonos rooms first, then explicitly moved playback to the web
player itself, capturing both sides. This resolves the open question
above:

- **The displaced renderer does get an explicit signal.** Our proxy's own
  connection receives a second `SRVR_RNDR_SET_ACTIVE` — the first one
  (at real activation) carries `active: true`; this one arrives with the
  `active` field *absent entirely* (`srvrRndrSetActive {}`), which
  proto2's optional-field semantics — and this codebase's own accessor
  usage — correctly read as `false` with no special-casing needed.
  Confirmed in the readable log: `Renderer set active: False` logs
  correctly, immediately.
- **But a real, distinct, newly-found bug sits right behind it.** Two
  `Protocol error: ... Renderer state updated message received from non
  active renderer` errors follow, ~1.2s and ~5.2s after deactivation,
  before `Playback stopped` finally logs 5.4s after deactivation.

Root cause, confirmed by reading the code, not inferred: `player.py`'s
`deactivate_renderer()` deliberately debounces the *physical device*
stop by `_DEACTIVATION_GRACE_SECONDS` — a real, already-documented fix
for a different problem (a WS-reconnect race that used to interrupt
playback on every token refresh). But `StateReporter`
(`state_reporter.py`) has no `is_active_renderer` gate anywhere in its
send paths (`_send_state_update`/`report_now`/the heartbeat loop) — it
keeps reporting as long as `Player.state == PLAYING`, which it still is
for that whole grace window, since the device hasn't been told to stop
yet. Every report sent in that window is guaranteed rejected, because
the server already stopped listening to this connection the instant it
sent `SRVR_RNDR_SET_ACTIVE` with `active` unset.

This is very plausibly the real, complete explanation for the "non
active renderer" error recurring throughout this *entire* investigation
— not only (or even mainly) the split-identity Controller-join confusion
fixed earlier. Every deactivation, for any reason, leaves reporting
running a few seconds past the point the server stopped listening.

## Fixes implemented

1. **Gate `StateReporter`'s outbound sends on `is_active_renderer`**
   (2026-09-13). `StateReporter._send_state_update()` — the single choke
   point both `report_now()` and the heartbeat watchdog go through — now
   returns immediately, without building or sending a report, whenever
   `Player.is_active_renderer` is false. Deliberately independent of the
   existing (correct, unchanged) device-stop debounce in
   `deactivate_renderer()`: that debounce still lets `Player.state` read
   PLAYING for the whole grace window, on purpose, to protect the
   physical device from the WS-reconnect race it was built for — this is
   a separate gate on top, not a replacement. The skip path still resets
   the heartbeat's `_last_report_at` clock, so the watchdog doesn't
   busy-loop for the rest of that window.

   The same gap existed in two more ad-hoc report paths and got the same
   gate: `Player._report_volume_change()` (the exact "Volume set to 72"
   case observed 4ms after deactivation in the capture above) and both
   `_file_quality_report_callback` call sites (`play_track`'s initial
   report and the gapless-transition report — a transition can itself
   land inside the deactivation grace window, since playback keeps
   running through it by design). `broadcast_current_volume()`'s
   legitimate activation-time re-broadcast is unaffected — verified
   against `command_handler.py`'s `_handle_set_active`, which always sets
   `is_active_renderer = True` before calling it.

   Regression tests: `TestActiveRendererGate` in
   `tests/playback/test_state_reporter.py`,
   `TestVolumeReportGatedOnActive` in `tests/playback/test_player_volume.py`.

   Not yet live-tested against a real household — next step is testing
   track switching with the controller role active, per the user's own
   plan.

## A failed live test (2026-09-13, `qobuz-queue.log`) — most likely just timing

Tried playing from the app after the fix above landed: no sound, Sonos app
showed the player paused, not obviously on the track that was picked.
`qobuz-queue.log` (Cuarto's WS session — the only speaker that shows any
`command_handler` activity in this log; the other 4 rooms were never even
`SET_ACTIVE`d) shows, in order:

1. **Connect (t=0.05s).** The server immediately restores a stale prior
   session: `SET_ACTIVE(true)` + `SET_STATE(PAUSED, currentQueueItem=X,
   position=5629ms, ...)`. We load track X (`"Loading new track: 46554934"`)
   but never start it on the backend (paused, never played) — a genuine
   leftover from a previous run, not this test.
2. **t≈10-30s.** Three `SRVR_CTRL_QUEUE_TRACKS_LOADED` broadcasts arrive
   (Controller-role — we receive these now because of the dual join),
   `queuePosition` advancing 1, then 2 — almost certainly the user
   browsing the app. We don't act on these at all; see
   `queue_diagnostics.py`'s own docstring, already investigating exactly
   this message.
3. **t=41.07s.** We get deactivated (`SET_ACTIVE{}`, active absent) —
   another renderer becomes active. After the grace period,
   `stop_playback()` resets position to 0 and state to STOPPED, but —
   correctly, per `stop_playback`'s own contract — leaves
   `self._current_track` alone, still pointing at stale track X.
4. **t=58.38s.** We get reactivated (`SET_ACTIVE(true)`) — the user
   picked Cuarto again. We claim the device and re-broadcast volume, per
   `_handle_set_active`. Nothing else.
5. **t=74.35s (16s after reactivation).** The *only* command that ever
   arrives: a bare `SET_STATE{playingState: PAUSED}` — no
   `currentQueueItem`, no `currentPosition`. Per the deliberate
   omission-means-"don't touch" semantics in `_handle_set_state`, this
   correctly leaves `_current_track` untouched — but "untouched" here
   means "still the stale track from step 1," at position 0, never
   started. Logged exactly as expected:
   `"Track loaded paused at 0ms (not started on backend)"`.
6. **t=77.6s — 3.3s later.** The user gave up and killed the app.
   Nothing else ever arrived.

**Most likely explanation: not enough time was given.** This matches
[[sonos-load-then-play-app-delay]] exactly in shape — the app splits a
renderer (re)activation across multiple WS messages and has been directly
observed elsewhere taking up to ~17s between a load-only message and the
actual play instruction, unrelated to anything the renderer does. Step 5
already arrived 16s after reactivation; only 3 more seconds passed before
the app was killed — well short of the ~17s worst case seen before. No
code bug is implicated by this capture: `is_active_renderer` gating,
deactivation debounce, and the omission-vs-sentinel handling all behaved
exactly as designed throughout.

**A real, separate, still-unconfirmed question this capture reopens, but
does not answer:** does the app ever resend `currentQueueItem` when
reselecting an already-current queue position on a different renderer, or
does it just expect the renderer to already know "what's current" from
the `SRVR_CTRL_QUEUE_TRACKS_LOADED` broadcasts sent while it was inactive
(step 2)? This capture ended too early (step 6) to tell — the real
"play this" `SET_STATE` may simply not have arrived yet. Don't build
`queue_diagnostics.py`'s scaffolding into real queue-tracking on the
strength of this one capture; retest with more patience first.

**Recommended retest:** activate Cuarto from the app, then wait at least
30-60s (past the ~17s worst case already seen once) before concluding
nothing happened, and capture the log again either way.

## Correction: the real signal was `QUEUE_TRACKS_LOADED`, not timing (2026-09-13)

The "just needs more time" read above was wrong, or at least not the
main story. The user's own read of the timestamps, checked directly
against `qobuz-queue.log`'s real (unmasked) values, holds up:

| time | message | queueVersion | queuePosition | track |
|---|---|---|---|---|
| 19:53:58 (connect) | `SET_STATE` (restore) | 14.1 | — | queueItemId=1, trackId=46554934 |
| 19:54:08.783 | `QUEUE_TRACKS_LOADED` | 15.1 | *(absent → 0)* | trackId=46554933 |
| 19:54:16.997 | `QUEUE_TRACKS_LOADED` | 16.1 | 1 | queueItemId=1, trackId=46554934 |
| 19:54:28.780 | `QUEUE_TRACKS_LOADED` | 17.1 | 2 | queueItemId=2, trackId=46554935 |

Three taps in the app, ~8-12s apart — playing the album from the start,
then picking track 2, then track 3. **`SRVR_RNDR_SET_STATE` (type 41)
never arrives again anywhere in this 33-second window** — the *only*
server→us traffic across all three taps is these three
`QUEUE_TRACKS_LOADED` broadcasts, each bumping `queueVersion` and
`queuePosition`. If track selection were carried by `SET_STATE` the way
`command_handler.py` currently assumes, three taps should have produced
three of them. They didn't.

This resolves the open question the earlier section in this doc
deliberately left unanswered ("does the app ever resend `currentQueueItem`
... or does it expect the renderer to already know 'what's current' from
`QUEUE_TRACKS_LOADED`?") — the second one, corroborated directly.

**It also fully explains the "stuck" symptom, independent of timing.**
`command_handler.py`'s `_handle_set_state` is the *only* place
`queue.set_version()` gets called, from a `SET_STATE`'s `queueVersion`
field. With no `SET_STATE` arriving after the three taps, every
`RNDR_SRVR_STATE_UPDATED` we sent kept echoing the connect-time restore
back verbatim: checked directly, our own report sent at 19:55:13 (after
reactivation, in response to the bare `SET_STATE{playingState: PAUSED}`
from the earlier section) still carried `queueVersion: 14.1` and
`currentQueueItemId` = the *original* restored track — 46+ seconds and
two more taps after the app itself had moved to queueVersion 17.1. We
told the server we were still on track 1 of a queue it had already
advanced to track 3 of. That's a concrete, evidenced reason the server
never sent anything resembling a play instruction, not (or not only) a
timing gap. Matches what [[sonos-queue-version-never-echoed]] flagged as
unconfirmed speculation — now confirmed.

**Implemented (2026-09-13).** `PlaybackCommandHandler._handle_queue_tracks_loaded`
(`command_handler.py`) now handles `SRVR_CTRL_QUEUE_TRACKS_LOADED`
directly: while active, loads `tracks[queuePosition]` (absent reads as
0) and starts it playing immediately via the same `apply_remote_state`
`_handle_set_state` uses — no separate load/play logic — with
`position_ms=0` always (tapping a track plays it from the start, per the
user's own read: "no way to select otherwise, just play"). Echoes
`queueVersion` back the same way `_handle_set_state` does, *before* the
active-renderer check — so our own bookkeeping stays current even while
we're not the one driving playback, ready for whenever we do become
active. Also populates `_next_track_info` (`tracks[queuePosition + 1]`)
for gapless, reusing the exact dict shape `_handle_set_state`'s
`nextQueueItem` handling already produces. Registered in
`get_message_types()`, dispatched in `handle_message()`, and coalesced
in the same (default) group as `SET_STATE` in `speaker.py`'s
registration — a rapid tap burst only ever runs its last one, same
protection `_superseded_by_newer_command` already gives `SET_STATE`.
Unit tests: `tests/playback/test_command_handler_queue_tracks_loaded.py`.

**Confirmed live (2026-09-13):** tap-to-play, seeking, and switching
albums all work as intended with this fix in place — see the next
section for the one remaining gap it exposed.

## The first track after a fresh start didn't play — `isActive` was the cause

Everything above worked immediately except one case: the very first
track selected right after a freshly started proxy process did nothing.
The user's own diagnosis, checked directly against a fresh capture
(`qobuz-queue.log`, 2026-09-13, 20:44 run) and confirmed correct:

`WsManager._send_join_session()` claimed `isActive=true` unconditionally
on *every* `JOIN_SESSION`, including a truly fresh process start with
nothing of our own loaded yet. An `isActive=true` join gets a restore
`SET_STATE` handed straight back: `currentQueueItem` = queueItemId 1 /
trackId 46554934, paused at 0ms — the *literal same* track/queueItemId
every earlier capture that day had already restored. Read in the moment
as proof of genuine server-side staleness — **later reconsidered and
retracted, see the correction further below**: this is at least as
plausibly the account's one real shared listening queue genuinely not
having moved between those captures (nothing else was played on any
device in between, same day, short gap), not evidence of anything wrong.
We loaded it (`"Loading new track: 46554934"`) and sat paused, exactly
as designed.

Then, ~7s later, the user's actual tap arrived — but not as a
`QUEUE_TRACKS_LOADED` the way every later tap in this same session
would. It was a bare `SET_STATE{playingState: PLAYING_STATE_PLAYING}`,
with no track info at all. Reading that as "resume whatever's already
loaded" (correct, general behavior — see the omission-vs-sentinel
handling elsewhere in `_handle_set_state`) meant starting the *stale*
restored track instead: `"Starting playback: track 46554934"`, and
indeed `"Playing: Andy Sheppard - Thirteen"` — a track from hours
earlier, not whatever the user actually tapped. The app's own UI,
seeing the server tell it Cuarto already had a track loaded, apparently
trusted that and sent a plain "play" instead of a fresh queue load —
the same trust that makes every *later* tap in this session correctly
skip resending `currentQueueItem` once we're actually in sync (see the
section above). The bug was self-inflicted: claiming `isActive` for a
session with no real state manufactured a false "already in sync"
belief on both sides right from the start.

### Fix attempted, then reverted (2026-09-13) — the isActive=false experiment broke real device selection entirely

First attempt: `Player.has_resumable_playback()` (`player.py`) — true
only while already believed active *and* actually holding a loaded
track in `PLAYING`/`PAUSED`, false on a fresh start — consulted by
`WsManager._send_join_session()` instead of defaulting straight to
`True`. Landed, unit-tested, then tried live.

**Result: worse than the bug it fixed.** Live test immediately after:
"I simply can't connect to the proxy." A fresh capture
(`qobuz-queue.log`, 2026-09-13, 21:29 run) shows why, directly —
`SRVR_RNDR_SET_ACTIVE` (type 43) **never arrives, not once, anywhere in
the entire session**, for either of the two renderers that joined with
`isActive: false`: the already-known `deviceUuid` for the Sonos group
active at startup, *and* a brand-new, never-before-seen `deviceUuid` for
a room ("Cuarto") that split off from that group mid-session — which the
user then explicitly selected in the app (a fresh `connect-to-qconnect`
POST, "Received connection from app", confirmed in the log). Neither
ever got activated. Compare every single earlier capture in this whole
investigation, where claiming `isActive: true` got `SET_ACTIVE(true)`
back within ~50ms, no separate request needed.

This means `isActive` on `JOIN_SESSION` isn't just a "resume my last
state" courtesy hint that happens to also grant activation as a side
effect — for the phone app's own direct local-network device selection
flow (`connect-to-qconnect`), it looks like *the* activation mechanism
itself. There's no separate confirmation step to wait for instead: not
claiming it doesn't mean "wait for `SRVR_RNDR_SET_ACTIVE`/
`CTRL_SRVR_SET_ACTIVE_RENDERER` shortly after", it means never getting
activated at all. Every fresh `WsManager` start is structurally
preceded by exactly this same `connect-to-qconnect` flow (that's how
`speaker.py` wires `_on_app_connect` → `WsManager.start()` — a
background reconnect is the only case that *isn't*), so "was this
triggered by the app's own request" can't be used to distinguish the
two scenarios post hoc the way the original bug report's framing
suggested — it's true of every fresh join, including the exact one that
went on to load a stale track.

**Reverted in full**: `_send_join_session()` back to unconditionally
claiming `isActive` from `_next_join_claims_active` (still `True` by
default, still forced `False` for one join by `force_reconnect()`'s
hijack-recovery path — that part was never in question). `WsManager`'s
`has_resumable_playback` constructor parameter and
`Player.has_resumable_playback()` removed entirely, along with their
tests — see git history around 2026-09-13 for the full diff if this
needs revisiting.

**The original bug (stale restore state feeding a bare "just play" SET_STATE
a stale track) is real and still unfixed.** Given the above, the right
fix has to leave `isActive` alone and instead make the *renderer*
distrust its own connect-time restore: e.g. track whether `_current_track`
came from the server's `JOIN_SESSION`-response restore versus a genuine
`QUEUE_TRACKS_LOADED`/explicit `currentQueueItem`, and don't let a bare
`SET_STATE{playingState: PLAYING}` with no track info resume a
still-unconfirmed restore snapshot — wait for the real
`QUEUE_TRACKS_LOADED` instead, the same signal every subsequent tap
already relies on correctly. Not attempted yet; needs its own careful
pass rather than a quick follow-up on the heels of this revert.

### A second experiment: explicit `initialState` alongside `isActive: true` (2026-09-13, not yet live-tested)

Rather than the distrust-the-restore rewrite above, a narrower first
try: leave `isActive` unconditional (as reverted), but stop leaving
`JOIN_SESSION`'s `initialState` unset — send an explicit
`RendererState{playingState: STOPPED}` (every other field, all
optional, left unset) on *every* join. The hope: telling the server
outright "I have nothing of my own playing" might change what it hands
back in the restore `SET_STATE`, without touching the activation
mechanism the previous experiment broke.

`RendererState` (`qconnect_common.proto`) — the message type behind
`initialState` — is a smaller, distinct shape from `QueueRendererState`
(the one our regular `RNDR_SRVR_STATE_UPDATED` reports already use):

```protobuf
message RendererState {
  optional PlayingState playingState      = 1;
  optional BufferState  bufferState       = 2;
  optional Position     currentPosition   = 3;
  optional uint32       duration          = 4;
  optional uint32       currentQueueIndex = 5;  // a position, not an item id
  // field 6 unused
  optional int32        nextQueueItemId   = 7;
}
```

Notably: no `currentQueueItemId` and no `queueVersion` field at all —
unlike `QueueRendererState`, it structurally cannot say "the current
track is item X", only a positional index into whatever the server
thinks the queue is. Two commented-out alternates sit right on top of
field 5 in the `.proto` file itself (`queueVersion` and
`currentQueueItemId`), suggesting whoever reverse-engineered this tried
more than one reading before landing on the index interpretation — and
no capture across this entire investigation has ever shown a real
client sending `initialState` at all (every real client capture skips
`JOIN_SESSION` entirely), so there's no live reference to check that
reading against.

Implemented in `WsManager._send_join_session()`: builds
`common_pb2.RendererState()` with only `playingState =
PLAYING_STATE_STOPPED` set, passed as `encode_join_session`'s existing
(previously always-`None`) `initial_state` parameter — on every join,
unconditionally, not gated on any Player state. Test:
`test_join_carries_an_explicit_stopped_initial_state`
(`tests/connect/test_ws_manager.py`).

**Live-tested (2026-09-13), across three proxy restarts, `qobuz-queue.log`
22:06-22:11**: tracks played correctly every time, whether resuming
where playback stopped or picking a fresh one, and real device
activation kept working (confirming the `isActive: true` revert holds).
The user's own account of each session's actions, matched against the
log, pins this down precisely rather than leaving it a guess:

- **Session 1→2**: stopped the proxy mid-track, restarted, reselected
  the room, then simply **pressed play** where playback had stopped —
  no track/album tap, i.e. exactly the original bug's code path (a bare
  `SET_STATE{playingState: PLAYING}` trusting whatever the restore
  loaded). The restore loaded track 417618316 — the *exact* track
  playing when session 1 shut down (`queue_item_id=5`). Accurate, so
  pressing play correctly resumed it. **This is the original bug's exact
  trigger succeeding only because the restore happened to be right.**
- **Session 2→3**: identical setup (stop mid-track, restart, reselect),
  but this time the user picked a **completely new album** instead of
  pressing play. The restore loaded track 421967775
  (`queue_item_id=3`) — confirmed, via the gapless-arm log entry just
  before session 2's shutdown, to have **nothing to do with** what was
  actually last playing (417618316, `queue_item_id=5`, never
  transitioned off). Genuinely stale, not explained by anything either
  side did. It just didn't matter, because picking a new album routed
  through `QUEUE_TRACKS_LOADED` (instant load+play) instead of a bare
  "play", cleanly overriding the wrong restore before it was ever heard.

**Correction (2026-09-13, same day): this was never staleness at all —
the user's own read is the right one.** `SRVR_CTRL_SESSION_STATE`'s
`queueVersion`/`trackIndex` are scoped to the *session*
(`sessionUuid`), not to a specific renderer — there is one shared,
account-level queue (matches hypothesis #1: one Connect session per
user), and a Connect renderer is just one possible *output* for it, not
a separate queue owner. The Qobuz app almost certainly doesn't run two
playback engines, one for local listening and one for "Connect" — its
normal player and its Connect queue are the same underlying object;
selecting a Connect renderer just changes where the audio comes out.
So playing something directly on the phone — no Connect renderer
selected, or a different one — still advances the *same* shared session
queue. When the proxy reconnects and gets reselected, the restore it
receives is the session's genuinely current state, not a stale cache of
*this renderer's* own history — there never was a renderer-scoped
history separate from the session's to begin with. Session 2→3's
"unrelated" restore almost certainly just reflects the user listening
to something else directly on the phone during the ~30s the proxy was
down — invisible to the proxy's own log by construction, since there
was no WS connection during that gap to record anything.

This retracts both "confirmed genuinely stale" claims above (the
20:44 run's restore, and session 2→3's) — recorded as "stale" in real
time without seriously considering this alternative, which fits the
architecture and the evidence at least as well and requires no server
misbehavior at all. **There is no known bug here any more.** Trusting
the restore on a bare "play" is very plausibly *correct* behavior
(resuming the account's real current queue), not a race to guard
against — the "distrust the restore" fix floated above would mean
deliberately ignoring accurate state, and should not be implemented on
the strength of this investigation. `initialState` stays in (harmless,
and its actual effect is still unconfirmed either way), but this
specific line of investigation is closed pending actual counter-evidence
— e.g. a restore that's wrong even when the phone demonstrably touched
nothing in between.

**Deliberately confirmed (2026-09-13, same session), not just
reasoned through.** The user ran exactly that test: stopped the proxy,
played track 4 of Gretchen Parlato's *The Wise Ones* directly on the
phone (no Connect renderer selected), then restarted and reselected the
room. The restore: `queue_item_id=3` (0-indexed — the 4th item),
paused mid-track at 7523ms — a precise, direct match, not just a
plausible one. 6.6s later the same album got explicitly re-picked from
the app (`QUEUE_TRACKS_LOADED selects position 0`, `Playing: Gretchen
Parlato - Capricorn [The Wise Ones]`) — same album, correctly restarted
from track 1, exactly what tapping "play" on an album view does
regardless of an in-progress resume. Both signals were accurate; this
line of investigation is closed, not just deprioritized.

## Open questions / not yet tested

- What does a genuinely **first-ever** join (a brand-new deviceUuid,
  never seen by the server before) actually look like end to end? Still
  no capture has one — every capture so far, three for three, reuses an
  identity the server already knew, for both the web player and this
  proxy's own Sonos rooms.
- Does `SRVR_CTRL_ACTIVE_RENDERER_CHANGED` actually get suppressed for
  the activating connection itself, or was its absence in the proxy
  capture just this one session's timing? Worth a second capture to
  confirm before relying on that reading.
- If we stop claiming `isActive=true` on our own `JOIN_SESSION` and wait
  for `SET_ACTIVE_RENDERER`/`SRVR_RNDR_SET_ACTIVE` instead, does anything
  in this proxy's own code assume activation happens synchronously with
  connect? (`_next_join_claims_active`, the hijack-recovery path in
  `force_reconnect()`, etc. — all built around the current model.)
- Given two-for-two captures never sending `JOIN_SESSION` at all: is it
  possible this proxy could skip sending it too, for a `deviceUuid` the
  server already has on file, and just start authenticating + subscribing
  + reporting/reacting instead? Untested, and a real architectural
  change if it holds up — not something to try without thinking through
  what currently assumes a synchronous join first (see the point above).
- **Can the web player activate one of our rooms that isn't already
  live?** Reasoned answer, not yet live-tested: almost certainly not.
  `discovery.py` sets no CORS headers at all, and across all three web-
  player captures collected so far, not one single request targets a
  local/private IP — the web player's own code never attempts LAN
  contact, only the native app has ever been observed POSTing to a
  room's local `connect-to-qconnect` endpoint. Switching *to* one of our
  rooms from the web player should only work if that room already has a
  live WS session; for any room that doesn't, it should look exactly
  like the stale-entry symptom investigated earlier — visible in the
  picker, silently does nothing when picked. Cheap to confirm live
  whenever convenient, just not yet done.

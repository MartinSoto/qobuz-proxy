# WS token expiry silently killed gapless mid-album — fixed

**Status: theory fully confirmed live (2026-09-05, see "Full confirmation"
below) and fixed** — `WsManager` now refreshes its own control-WebSocket
token directly from the Qobuz API instead of only ever waiting for the
phone app to reconnect and push one (see "The fix" below). Watch for
whether this actually stops recurring in practice.

## Reproduction (reported directly, 2026-09-05)

Leaving the app playing a list/album completely undisturbed for about **30
minutes** reliably reproduces the disconnection. Consistent with the WS
token's short lifetime (~5-10 min observed): once nothing has interacted
with the app for a while, it seemingly stops proactively refreshing the
token, and the control WebSocket falls dark until the app is touched again.

## Also not this: a reconnect-after-token-refresh stop, root-caused and fixed

A third distinct incident, reported directly (2026-09-05, after ~1h of solid
playback): the app disconnected and fell back to local (phone) playback —
consistent with a WS-token-expiry gap like the ones above (confirmed in the
log: `Token expiring soon`/`Token expired` ~4 minutes before). Seeking via
the **Sonos app** worked fine at that point (the physical device and its
queue were healthy). The user then reconnected to the group from the Qobuz
app — which **stopped the currently-playing (already-ours) track outright**.
After that, neither Play nor Seek from the Qobuz app did anything; only
loading a different track and returning to the original one recovered it.

Root cause (fixed, see `git log` for the commit touching
[player.py](../qobuz_proxy/playback/player.py)'s `claim_device`): every
`SET_ACTIVE(true)` — sent not only on a genuinely fresh renderer selection
but also on *every* control-WebSocket reconnect, including a plain token
refresh — triggered `claim_device()`, which unconditionally sent the
physical device a raw stop. Its own docstring's assumption ("nothing of
ours to report — we were never playing anything before this") is only true
for the fresh-selection case; here, our content had been playing the whole
time, gaplessly, underneath a control channel that merely happened to be
down. The stop silenced the device for real, but `Player`'s own
`_state`/`_current_track` were never updated to match (`claim_device`
deliberately doesn't touch them) — so every subsequent command kept
computing against a track it still believed was actively playing, and
nothing worked until a fresh track load resynced the two. Fix: skip the
stop entirely whenever `_current_track` is already set and `_state` isn't
`STOPPED` — the silence-a-shared-device precaution only ever needs to
guard against a genuinely different source having taken over, not our own
already-active session reconnecting.

## Not this: a same-day disconnection that was actually a device dropout

A disconnection reported live on 2026-09-05 (~08:19, `Cocina` room) looked
identical at a glance (`Track ended naturally` mid-playlist) but turned out
to be **unrelated to this doc's theory** — worth recording so a future
"it disconnected again" report gets checked against the right cause first:

- `08:08:51` — WS token expired (as usual) — coincidental timing, turned out
  irrelevant to what follows.
- `08:18:11`-`08:19:33` — every SOAP call to the physical Sonos device
  itself (`GetTransportInfo`/`GetPositionInfo`) starts failing:
  `TimeoutError` then `ClientConnectorError: Cannot connect to host
  192.168.68.54:1400`. This is LAN reachability to the speaker, not the
  control WebSocket to Qobuz.
- `08:19:33` — STOPPED-confirmation fires, `Track ended naturally`, tries to
  auto-advance to the already-armed next track — but every subsequent SOAP
  call keeps failing the same way for another 13 minutes, because the
  device is still unreachable.
- `08:20:16` — Sonos's own topology discovery confirms the device is
  genuinely gone (missing from the printed household topology entirely, not
  just slow to answer HTTP) — a real Wi-Fi/network dropout of that specific
  speaker.
- `08:20:26`-`08:20:47` — clean teardown and rediscovery: the speaker gets
  removed, and the household's other rooms come back at new DHCP-assigned
  IPs shortly after.

The `Gapless: no next-track info` warning never appears anywhere near this
incident — confirming it's a different failure mode (a physical device
dropping off the network) than the WS-control-token theory this doc tracks.
Existing device-departure handling did the right thing (detected sustained
unreachability, gave up cleanly, tore the speaker down, let the others
rediscover at their new addresses). `Cocina` did reappear a few minutes
later (same IP) and got rediscovered, but its DLNA control layer stayed
unresponsive for a while afterward — and, tellingly, even the **official
Sonos app** couldn't control it either at that point. That ruled out a
proxy-specific reachability quirk and pointed at the device itself being
wedged — which raised the real question: could *our* traffic pattern be
what tips an already-flaky device over into fully hanging, rather than the
device failing entirely on its own (playback is otherwise stable for hours
at a time)?

**Root cause found and fixed**: the log showed 12 `Unclosed client session`
errors in that same ~8-minute window — a real bug in
[client.py](../qobuz_proxy/backends/dlna/client.py)'s `reset_session()`,
which had no guard against overlapping callers. The poll loop (every 0.5s)
and any in-flight command (`Stop`/`SetAVTransportURI`/...) each run their
own SOAP calls against the *same* `DLNAClient`, and a flaky device can make
more than one of them fail within milliseconds of each other. Each failing
call blindly closed-and-replaced `self._session` — whichever replacement a
losing racer built was then never referenced again: an orphaned, unclosed
`ClientSession` (and its live `TCPConnector`, potentially still holding a
real socket open against the device). A pile of our own leaked, never-torn-
down connections sitting against an embedded device's small connection
table is a plausible way *we* contributed to it wedging solid enough to lock
out the Sonos app too.

Fixed: `_soap_action_detailed` now captures the session each attempt
actually used and passes it to `reset_session(expected_session=...)`, which
only resets if nothing else has already replaced it since — overlapping
failures converge on one new session instead of racing to leak one each.
Whether this fully explains the wedge (vs. just reducing how often it
happens) isn't confirmed yet — watch for whether `Unclosed client session`
and "even the Sonos app can't control it" stop recurring together.

## Field update (2026-09-05, ~10h container log)

The new warning does fire — 13 times in one overnight run — but it turns out
to be **near-constant and almost always harmless**, not a rare symptom: it
fires on nearly every single gapless transition (the brief window between a
transition and the app's next `SET_STATE` catching up), self-healing within
under a second via the position-tick retry, *regardless of whether the WS
token happens to be healthy at that moment*. It fired at container startup
and steadily all night, unrelated to either of the two real WS-token-expiry
windows this run also produced (one 34 minutes long, one ~7 hours — both
confirmed real: `Token expiring soon`/`Token expired, waiting for refreshed
token` with nothing until the app reconnected). `No next track available -
playback stopped` — the actual permanent-stop message from the two logs this
doc was originally written from — did not occur once in this run.

There was one real interruption (`Trusting STOPPED` → `Track ended
naturally`, 23:28:01), but it does **not** match this theory: it happened 17
minutes *after* the WS had already reconnected, wasn't preceded by the new
warning, and instead coincided with a Sonos-side network blip a few minutes
earlier (other rooms briefly dropped from the household topology, a
`keepalive ping timeout` closed the WS, `SOAP GetTransportInfo failed`
errors). The already-armed next track had been successfully queued on the
device, but it apparently never auto-advanced to it on its own; the existing
10-second STOPPED-confirmation timeout caught that and fell back to
explicitly reloading/restarting the track — a real, audible (non-gapless)
gap, but not a permanent stop, and likely a **different bug** (a missed
on-device auto-advance, possibly triggered by the topology blip) rather than
the WS-token chain below.

So: the WS-token-expiry mechanism itself is confirmed real and can last
hours, but the exact failure chain hypothesized below (warning fires because
the WS is down at that exact moment → nothing gets armed → the last-armed
track ends with nothing next → permanent stop) hasn't reproduced yet. Keep
watching for a warning that (a) is directly preceded by a `Token expired`
line and (b) doesn't self-heal before the currently-armed track ends — that
would be the first direct confirmation.

## Symptom

Reported directly: playback of an album stops partway through, well before
the actual last track, with `Track ended naturally` as the last relevant log
line — the current track is not actually the album's last one. Also observed:
the Qobuz app falling back to local (phone) playback shortly after.

## The two source logs

`sudden-stop.log` and `sudden-stop2.log` (both 2026-08-30, different rooms —
`Cuarto`/Sonos Play:3 and `Cocina`/Sonos One) show the same shape:

1. Gapless plays through several tracks normally — each transition is
   immediately followed by a `Gapless: armed next track: <title>` line.
2. At some point, `qobuz_proxy.connect.ws_manager` logs `Token expiring soon,
   need refresh` then `Token expired, waiting for refreshed token from Qobuz
   app`.
3. The very next gapless transition after that has **no** corresponding
   `Gapless: armed next track` line.
4. Minutes later (once the *currently playing* track — the last one that
   actually got armed before the token died — finishes), the backend reports
   `Trusting STOPPED`, then `Track ended naturally` / `No next track
   available - playback stopped`, even though more tracks remained in the
   album.
5. In `sudden-stop2.log`, the Qobuz app only reconnects (`Received connection
   from app`, `Refreshed WebSocket tokens from Qobuz app`) about six minutes
   later, and playback only resumes because the app re-drives the queue from
   scratch at that point.

## Root cause chain

1. **The control WebSocket's own auth token has a short lifetime** (~5–10 min
   in both logs) and can currently only be replaced by the **phone app**
   re-POSTing to `/streamcore/connect-to-qconnect`
   ([discovery.py:142](../qobuz_proxy/connect/discovery.py#L142)). The proxy
   has no way to request a fresh one itself.
2. Once it expires, `WsManager._connect_and_run()` calls
   `_wait_for_valid_token()`
   ([ws_manager.py:601](../qobuz_proxy/connect/ws_manager.py#L601)) *before*
   even attempting to reconnect. This blocks completely
   (`await self._token_update_event.wait()`) until the app volunteers a new
   token — no reconnect attempts, nothing sent or received, in the meantime.
3. **Gapless's "arm the track after next" step depends entirely on a live
   WebSocket.** `command_handler._next_track_info` is populated only from
   `nextQueueItem` in a `SET_STATE` message received over that same socket
   ([command_handler.py:161](../qobuz_proxy/playback/command_handler.py#L161)).
   Every gapless transition explicitly clears this info and immediately
   tries to re-arm from it again
   ([player.py:1791-1851](../qobuz_proxy/playback/player.py#L1791-L1851))
   — normally a fresh `SET_STATE` arrives in time (the state update sent
   just before it tends to trigger a quick round trip from the app). If the
   WebSocket happens to be down at that exact moment,
   `_get_next_track_callback()` returns `None`, and
   `_prepare_next_track_locked()` used to bail out via a bare
   `if not next_track_info: return` — **no log line at all**, indistinguishable
   from genuinely reaching the end of the queue.
4. There's a retry on every position tick
   ([player.py:1668-1677](../qobuz_proxy/playback/player.py#L1668-L1677),
   every `STATE_POLL_INTERVAL_SECONDS`), but it's powerless if the WebSocket
   never comes back before the currently-armed track finishes playing —
   which is exactly what both logs show.

So `Track ended naturally` / `No next track available` in these logs is very
likely a **false end-of-queue**: the album wasn't actually over, the control
channel was just down at the moment the next arm needed to happen.

## What was added (this pass)

`_prepare_next_track_locked` ([player.py](../qobuz_proxy/playback/player.py))
now logs a warning — once per gap, not once per retry — when it bails because
`next_track_info` isn't available:

```
Gapless: no next-track info available to arm with — either genuinely at the
end of the queue, or the Qobuz app's connection is down and hasn't told us
what's next
```

If the theory above is right, this warning should appear right after the
last successful `Gapless: armed next track` line and persist (re-logged only
once, not spammed) until either the token refreshes or the track ends. If a
future log shows this warning *without* a preceding `Token expired` /
`Token expiring soon` pair, the theory is wrong and the missing
`nextQueueItem` has some other cause.

## Full confirmation (2026-09-05, live)

A clean occurrence with nothing else going on:

```
11:35:41  Token expiring soon / Token expired, waiting for refreshed token
11:37:19  Gapless: transitioning to track 405937069 (already-armed track becomes current)
11:37:20  Gapless: no next-track info available to arm with  ← fires, as always...
          ...but this time no "Gapless: armed next track" follows. No self-heal.
11:42:33  Trusting STOPPED → Track ended naturally → No next track available - playback stopped
```

No WS reconnect anywhere in that 7-minute window. This is the first time all
three pieces lined up: the warning fired, it didn't self-heal, and the
currently-armed track ran out with nothing queued behind it — a real
permanent stop. Reported cause on the phone side: the screen/app had been
left untouched for a while, plausibly suspended by Android in the
background — consistent with "the app doesn't proactively refresh unless
something wakes it up."

## The fix (implemented)

The reference C++ implementation this protocol is modeled on
([StreamCore32](https://github.com/tobiasguyer/StreamCore32),
`stream/qobuz/src/QobuzStream.cpp`) does **not** wait on the controlling app
for WS token refresh — the device refreshes it itself, on a 30s heartbeat,
via a direct Qobuz API call:

```cpp
WSToken QobuzStream::getWSToken() {
  ...
  headers = {
      {"Referer", "https://play.qobuz.com/"},
      {"Content-Type", "application/x-www-form-urlencoded"},
      {"Origin", "https://play.qobuz.com"},
      {"X-App-Id", cfg_.appId},
      {"X-Session-Id", cfg_.XsessionId.token},
  };
  if (!cfg_.userAuthToken.empty())
    headers.push_back({"X-User-Auth-Token", cfg_.userAuthToken});
  else if (!cfg_.api_token.token.empty())
    headers.push_back({"Authorization", "Bearer " + cfg_.api_token.token});

  endpoint = cfg_.ws_token.jwt.empty() ? "createToken" : "refreshToken";
  auto resp = qobuzPost(cfg_.api_base, "qws", endpoint, headers, "jwt=jwt_qws");
  // response: {"jwt_qws": {"jwt": ..., "exp": ..., "endpoint": ...}}
}
```

i.e. `POST {api_base}/qws/createToken` (first token) or `/qws/refreshToken`
(renewal), authenticated with the same user auth token this proxy already
holds, independent of the phone app being reachable at all.

`QobuzAPIClient` ([auth/api_client.py](../qobuz_proxy/auth/api_client.py))
already had most of what this needs — `user_auth_token`, `app_id`, and an
`x_session_id`/`start_session()` pair matching StreamCore32's `X-Session-Id`
almost exactly (turns out `start_session()` was already being called, from
`get_track_url()` — not dead code as first assumed while just reading, only
confirmed by checking).

Implemented: `QobuzAPIClient.get_ws_token(previous_jwt="")` — `POST
{API_BASE}/qws/createToken` (no previous JWT) or `/qws/refreshToken` (given
one), same headers as above. `WsManager._wait_for_valid_token` now tries
this itself first, via a new `_refresh_token_from_api`, before ever falling
back to waiting on the app — retried every 30s while it keeps failing.
Self-refresh only ever applies once there's already a session from a real
app connection (the very first token still has to come from the app); after
that, the control WebSocket never has to depend on the app reconnecting
again for the rest of that session's lifetime. `Speaker` now passes its
shared `QobuzAPIClient` into `WsManager` for this.

Calls an unofficial Qobuz endpoint on a recurring basis (every ~9 minutes at
the observed token lifetime) — worth watching whether it holds up over a
longer stretch, but the mechanism is a straight port of what the reference
device itself already does continuously.

## A second, unrelated device-wedge incident right after this fix shipped

Reported directly (2026-09-05): shortly after the token self-refresh above
was deployed, playback stuttered then stopped, and the speaker (`Sala`, a
Sonos Five this time — a different room/model than the earlier `Cocina`
incident) was bad enough afterward that a physical power cycle was needed
to recover at all. Checked first because of the timing, but the log ruled
out the fix above directly: the WS control token never even got close to
expiring in that session (playback had only been running ~5 minutes, well
under the ~9-11 min observed token lifetime), and `Refreshed WebSocket token
directly from Qobuz API` never appears — that code path never ran.

What the log actually showed: two isolated `SOAP GetTransportInfo` timeouts
(each recovered on retry) over about 2.5 minutes, then the device dropped
out of the household topology entirely and got rediscovered — and then kept
cycling every ~10 minutes for the rest of the session. No `Unclosed client
session` errors this time (the earlier `reset_session` race fix — see
"Not this" above — is holding), so this is a *different* mechanism able to
degrade a device the same way. No single smoking-gun root cause this time;
it looks like sustained 0.5s-interval polling is simply too much for some
real hardware over a long enough session, independent of any specific logic
bug.

**Mitigation implemented**: `DLNAClient.consecutive_failures` (client.py)
now tracks consecutive total communication failures (every retry
exhausted, not just an uncooperative-but-reachable device answering with a
permanent UPnP error); `AudioBackend._poll_interval` (backend.py) backs the
poll loop's own cadence off once that crosses a threshold, capped, snapping
straight back the moment anything succeeds again. This doesn't fix the
underlying tendency for hardware to wedge under continuous polling — it
just means a struggling device gets fewer requests while it's already
struggling instead of the same rate. **The real fix, not yet implemented**:
switch transition/hijack/stop detection from polling to GENA eventing
(subscribe to `AVTransport`'s event channel and react to device-pushed
NOTIFYs), the same approach this codebase already uses for Sonos household
topology (`dlna/sonos/events.py`) — discussed and deliberately deferred as a
separate, larger piece of work rather than rushed alongside an active
production issue. Position tracking would likely still need some polling
(position isn't normally part of `AVTransport`'s evented state), just at a
much more relaxed interval, decoupled from transition/hijack detection.

Also fixed in the same pass: adding the backoff logic surfaced a real
pre-existing hazard in `_poll_state_loop` — an exception raised before the
loop's own `asyncio.sleep` call (which is exactly what a bare-Mock
`_client.consecutive_failures` comparison, or reading a backoff flag never
set on a `DLNABackend` built via `__new__`, both do) skipped straight to
the `except` handler with **no `await` in between**: a synchronous
busy-loop starving the entire event loop, not just this backend, until
something external cancelled the task. Hit immediately by the existing test
suite. Fixed by making `_poll_interval()` defensive and by always awaiting
a sleep in the loop's `except Exception` branch too, so no future exception
source (from this change or otherwise) can reintroduce the same failure
mode.

## A real regression from the token self-refresh: reconnects racing SET_ACTIVE

Reported directly (2026-09-05): after playing stably for over an hour,
Sonos playback suddenly stopped while the app still showed as connected.
Restarting from the Sonos app worked; a later attempt from the Qobuz app
also worked on Sonos, but *also* — separately, confirmed by the user to be
their phone connecting to a paired Bluetooth speaker, unrelated to this
proxy at all — played locally on the phone (matching the already-documented
"app falls back to local playback" behavior from the very first incident in
this doc, just audible through Bluetooth this time instead of the phone's
own speaker).

The Sonos stop itself, though, was real and traced directly to the token
self-refresh fix above. Every one of the four self-refresh reconnects in
that session's log shows the same shape:

```
Renderer set active: True     (our JOIN_SESSION right after reconnecting)
  ↓ (anywhere from ~1ms to several minutes later)
Renderer set active: False
Protocol error: code=1, message=Renderer state updated message received
  from non active renderer
Playback stopped                ← genuinely interrupts playback
  ↓ (seconds to minutes later)
Renderer set active: True       (server flips us back active again)
```

The server appears to race its own "is this renderer active" bookkeeping
against our re-authentication: right after we rejoin, it can still
momentarily think we're inactive, reject the very next thing we send, and
tell us we're inactive — before catching up and reactivating us again
shortly after. This was presumably always a latent protocol quirk, but
harmless while reconnects were rare (only when the app itself reconnected,
i.e. essentially never mid-listen). Self-refreshing every ~10-20 minutes
turned it into a real interruption on almost every refresh cycle throughout
a long session — the "sudden stop" reported here.

**Fixed**: `_handle_set_active`'s `active=False` branch used to call
`Player.stop_playback()` unconditionally. `Player.deactivate_renderer()` now
debounces that stop by 5s (`_DEACTIVATION_GRACE_SECONDS`, matching
`PLAYBACK_START_GRACE_PERIOD_SECONDS`'s own precedent for "a signal that's
meaningful once it settles can still be transiently wrong right after a
transition"); `claim_device()` (the `active=True` handler) cancels any
pending deactivation first, so a reactivation within the grace window —
exactly what this race produces — is a no-op instead of an audible stop. A
genuine deactivation (a different device/session actually taking over)
still stops, just no longer instantly.

**Now also fixed (2026-09-06)**: the "still open" pattern flagged above —
a full state resync (`Seeking to <stale position>` then a genuine
`Playback paused`) immediately after reconnecting — was reported recurring
directly: playback "just stops" mid-track with no other symptom, and
tapping play resumes from the exact same position (the signature of a
real pause, not a track change or connection loss). Root cause: a
purpose-built defense already existed for this
(`_is_stale_pause_snapshot_locked`, ignores a replayed PAUSED snapshot
when the renderer is confirmed still playing further along) but only
triggered when the position gap exceeded `_STALE_SNAPSHOT_THRESHOLD_MS`
(5s) — calibrated for the old world where a reconnect could follow
minutes of WS downtime. With reconnects now taking well under a second,
the app's cached position is barely stale — confirmed directly, one
occurrence arrived with only a ~300ms-old snapshot, nowhere near 5s, so
the guard never fired.

Fixed: `claim_device()` (every `SET_ACTIVE(true)`, i.e. every reconnect)
now records when it last ran; `_is_stale_pause_snapshot_locked` treats a
PAUSED snapshot on the current track as stale if *either* the position
gap is large (the original check) *or* we reconnected within the last
`_RECONNECT_STALE_GRACE_SECONDS` (5s) — catching the fast-reconnect case
directly by the thing we actually know (we just reconnected) instead of
only inferring it from a gap that no longer reliably forms in time.

## Letting the WS session idle out on its own (2026-09-06)

Self-refresh above was deliberately unconditional: it keeps the Connect
session alive indefinitely as long as the process runs, even through a
full night with nothing playing — reported directly ("I left the proxy
running overnight and this morning, the app was still connected and ready
to play. Nice thing, but I wonder if it's just too much."). Harmless
(the app just sees a device that's always ready), but not something worth
doing forever with genuinely nobody listening — it's still relying on us,
rather than the phone's own OS, to keep a background connection alive.

Fixed: `Player.is_worth_keeping_ws_alive()` tracks how long playback has
been continuously `PAUSED` or `STOPPED` (`_non_playing_since`); `PLAYING`
always resets it. `WsManager` is handed this as a callback
(`is_worth_keeping_alive`, wired up in `speaker.py`) and checks it at the
top of `_refresh_token_from_api()` — once idle time passes
`_WS_IDLE_TIMEOUT_SECONDS` (1 hour), it simply stops attempting the
refresh and the current token is left to lapse naturally, exactly the
pre-self-refresh behavior, just deferred an hour instead of triggering
immediately on the first pause.

This deliberately never force-disconnects anything — it only withholds a
renewal that would otherwise happen automatically. A resume from either
side revives it: from the app, normally; from the device's own controls
(e.g. pressing Play directly on a Sonos speaker after the WS session has
already lapsed), `_handle_unprompted_state_change` already flips
`Player`'s state back to `PLAYING` on any device-reported transition into
playback regardless of who asked for it, which is exactly the signal
`is_worth_keeping_ws_alive()` reads — no separate wiring needed for the
device-initiated case, it falls out of machinery that already existed for
an unrelated reason (recovering from a transient unprompted pause/stop).

## Stale Sonos queue entry replayed after an album finished (2026-09-07)

Reported live: after an album played through in full — no repeat, no
autoplay — the speaker spontaneously started streaming a *middle* track
of that same album again, tens of minutes after it had already played
cleanly. The Qobuz app still showed the actual last track as "playing"
throughout, frozen; the Sonos app showed the real (resumed) track.

Confirmed from the container logs: the last track's own audio GET
completed normally (~4m21s, matching its duration) and a
"no next-track info" warning was logged right after it started, exactly
as expected for a genuine end of queue. From that point until the
spontaneous replay ~15 minutes later, there is no state-change,
track-ended, hijack, or "Trusting STOPPED" log line at all — none of the
existing detection paths ever fired. Whatever the device's own state
string was doing internally during that gap couldn't be pinned down
further without DEBUG-level polling logs, which weren't enabled — but
the structural vulnerability that let a *specific, previously-played*
track resurface at all doesn't require knowing that: it requires that
track to still exist in the device's own queue.

Root cause: `SonosBackend`'s queue-based playback (`_start_transport`
adds the current track, `_arm_next_track` appends each gapless "next")
never removed anything from the device's real AVTransport queue once a
track had been consumed — only `clear_next_track()` ever called
`RemoveTrackFromQueue`, and only for an *armed-but-not-yet-played* entry.
A full album session left every already-played track sitting in the
device's own queue indefinitely, fully browsable/selectable there for
the rest of the session (or until the next explicit `play()`, which
clears the whole queue). Whatever nudged the device into position 3 that
day, it could only find something to play there because we'd never
removed it.

Fixed: `DLNABackend` gained a generic `_retire_queue_entry()` hook
(a no-op for a plain SetNextAVTransportURI renderer — nothing to trim)
and now tracks `_current_track_queue_nr` alongside the existing
`_next_track_queue_nr`. A confirmed forward gapless transition retires
the now-superseded track's entry — and re-syncs the recorded position
afterward, since removing an earlier queue slot shifts every later one
down by one, which would otherwise make the *next* retire target the
wrong entry. A genuine full stop (nothing armed, confirmed via the
existing STOPPED-confirmation machinery) retires the last remaining
entry too, including the edge case where a next track had been armed
but the device stopped instead of transitioning to it. `SonosBackend`
overrides the hook to actually call `RemoveTrackFromQueue`, and records
the queue position its own `_start_transport` gets back from
`AddURIToQueue`, mirroring `_arm_next_track`'s existing bookkeeping for
"next". The device's queue is now trimmed to roughly "current + next"
at all times instead of accumulating an entire listening session's
worth of already-played tracks.

Open question, not yet confirmed either way: the ~15-minute silence
before the replay, with *zero* detection activity of any kind, is also
consistent with the device's own `GetTransportInfo` state string simply
never leaving "PLAYING" after the queue ended (a plausible Sonos quirk,
distinct from the already-handled case where the state string goes
STOPPED but the URI doesn't — see `_UNCONFIRMED_STOP_TIMEOUT_SECONDS`
above). If a stuck-PLAYING state was also happening, the Qobuz app
being left showing the last track as still playing (rather than idle)
would be explained by that too, separately from the queue-trim fix
above. Left as a follow-up to watch for — if the app is ever again seen
stuck showing a finished track as playing (without an accompanying
track-jump, now that the queue is trimmed), that's the evidence needed
to pin this down and fix it directly.

## Grouped-speaker stutter at natural track transitions (2026-09-07)

Reported live, shortly after the queue-trimming fix above shipped:
speakers in a Sonos group stuttering and taking a few seconds to resync
specifically when the *queue* naturally advances to its next track (not
on an explicit switch from the app). Coordination within a Sonos group
happens entirely on Sonos's own side — this proxy only ever talks to the
group coordinator, never to individual members — so some baseline resync
cost at every track boundary is expected and outside our control. But
the timing lines up exactly with a plausible aggravating factor of our
own: the queue-trimming fix above retires the just-superseded track's
queue entry (`RemoveTrackFromQueue`) *synchronously, right at the moment
the transition is detected* — a second AVTransport queue-edit call to
the coordinator landing in the same instant it's already busy
renegotiating group sync for the track change, on top of whatever that
already costs.

Not fully confirmed (no direct before/after audio timing evidence), but
low-risk to address regardless: the retire's correctness doesn't depend
on doing it *immediately*, only on doing it before too much queue growth
piles back up — nothing else touches the queue in the couple of seconds
right after a transition.

Fixed: `_retire_queue_entry` for a confirmed forward transition now runs
in a background task delayed by `_QUEUE_RETIRE_DELAY_SECONDS` (2s)
instead of being awaited inline before the poll loop continues, landing
after the group has already settled onto the new track rather than
piling onto the exact moment it's settling. The queue-position
renumbering that has to accompany a successful retire (see the previous
section) moves into that same deferred task, guarded against a later
transition having already moved `_current_track_queue_nr` on to
something else before this one fires (only possible for a track shorter
than the delay itself — essentially never for real audio). The full-stop
and orphaned-armed-next retire paths are untouched: nothing else is
competing for the coordinator's attention at those points.

Worth watching whether this actually resolves the stutter, or whether
Sonos's own group-resync overhead was the whole story regardless of our
own queue-edit traffic.

## Brief silence a few seconds into a track, worse at Hi-Res (2026-09-07)

Reported live, after the two stutter mitigations above didn't change
anything: speakers playing the first few seconds of a track fine, then
going silent for a few seconds before picking back up — much more common
at Hi-Res rates. Since the group-transition-timing fixes above made no
observed difference, this pointed away from Sonos's own group-resync
overhead and toward the audio pipeline itself.

`CDNBlockCache` is one instance per proxy server (per room), shared
across *every* track it serves — including gapless pre-buffering, where
a renderer routinely starts pulling the armed next track's bytes while
the current one is still actively streaming out. That's two tracks'
worth of CDN reads genuinely interleaved against the same cache at once,
not one finishing before the other begins. The cache's one-lingering-
connection optimization (reusing an open HTTP response for the next
sequential block instead of paying for a fresh TLS handshake + Range GET
every time) was keyed on nothing more than "whichever fetch happened
most recently" — every time the two tracks' reads alternated, each one's
read tore down and replaced the *other's* connection. Both tracks paid
full reconnect latency on nearly every block instead of ever getting the
reuse this was built for — a very plausible source of exactly the
reported stall (a couple of seconds of silence while a connection that
just got torn out from under it gets re-established), worse at Hi-Res
since the transcode path's own decode/resample work already runs
tighter against real-time margin than a plain passthrough would.

Fixed: the cache now keeps up to `max_open_connections` (2 by default)
lingering connections at once, one per distinct (track_id, format_id),
evicting only the least-recently-used one once a third distinct track
needs a slot — the routine current+next case no longer shares (and
fights over) a single slot at all. Also bumped `DEFAULT_MAX_CACHE_SIZE`
from 2 MiB to 16 MiB, so each of two concurrent tracks has several
seconds of margin at Hi-Res bitrates instead of the LRU itself evicting
blocks either one still needs.

Not yet confirmed live whether this actually resolves the stalls — the
mechanism is well-reasoned from reading the code (two tracks' worth of
requests really do interleave, and the single-connection design really
did retire the other's connection on every alternation) but wasn't
directly instrumented/measured against the specific reported incident.

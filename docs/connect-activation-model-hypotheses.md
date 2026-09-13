# Working hypotheses: how Connect renderer activation actually works

**Status: work-in-progress, first pass checked against one web-player HAR
capture (`qobuz-player.har`) and one proxy RAW_WS capture (`qobuz-queue.log`,
2026-09-13, same day) via `scripts/qconnect_trace.py`.** Not yet implemented
or live-tested against a real household — this doc exists to hold the
hypotheses and the evidence for/against each one steady across sessions,
not to declare the investigation finished.

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

## Open questions / not yet tested

- What does a genuinely **first-ever** join (a brand-new deviceUuid,
  never seen by the server before) actually look like end to end? No
  capture has one — every capture so far, ours and the web player's,
  reuses an identity the server already knew.
- Does `SRVR_CTRL_ACTIVE_RENDERER_CHANGED` actually get suppressed for
  the activating connection itself, or was its absence in the proxy
  capture just this one session's timing? Worth a second capture to
  confirm before relying on that reading.
- If we stop claiming `isActive=true` on our own `JOIN_SESSION` and wait
  for `SET_ACTIVE_RENDERER`/`SRVR_RNDR_SET_ACTIVE` instead, does anything
  in this proxy's own code assume activation happens synchronously with
  connect? (`_next_join_claims_active`, the hijack-recovery path in
  `force_reconnect()`, etc. — all built around the current model.)

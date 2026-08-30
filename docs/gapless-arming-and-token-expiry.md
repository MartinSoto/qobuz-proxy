# Suspected bug: WS token expiry silently kills gapless mid-album

**Status: diagnosed from two field logs, not yet confirmed live or fixed.**
Logging was added ([player.py](../qobuz_proxy/playback/player.py)'s
`_prepare_next_track_locked`) to make the failure visible; this doc exists so
a future occurrence can be checked against the theory below instead of
re-deriving it.

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

## What a real fix would look like (not implemented)

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
already has most of what this needs — `user_auth_token`, `app_id`, and an
`x_session_id`/`start_session()` pair matching StreamCore32's `X-Session-Id`
almost exactly. Two gaps if this gets picked up later:

- `start_session()` exists but is **never actually called** anywhere in the
  codebase today — dead code.
- There's no `get_ws_token()`/`refresh_ws_token()` method at all; `ws_manager.py`
  would need to call it proactively (e.g. on the same kind of heartbeat,
  or right when `_wait_for_valid_token` would otherwise block) instead of
  only accepting tokens pushed from the app.

This wasn't implemented yet — it calls an unofficial Qobuz endpoint on a
recurring basis, which is worth deciding on deliberately rather than as a
follow-on to a logging fix.

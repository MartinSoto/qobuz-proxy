"""
Qobuz API Client.

Handles authentication, session management, and signed API requests.
"""

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger(__name__)

# track/getList rejects a request naming more than this many ids with HTTP 400
# (measured 2026-10-10: 50 works, 51 fails), so larger lookups go in chunks.
TRACK_GETLIST_MAX_IDS = 50


@dataclass
class TrackAudioInfo:
    """Per-track loudness/album data used for playback gain normalization
    (see backends/dlna/sonos/gain.py) — kept separate from the plain
    dict get_track_metadata returns since it comes from a different
    endpoint (track/getList, batched) and is never shown in the UI.

    gain_db/peak default to "no adjustment" (0 dB, full-scale peak) when
    Qobuz has no audio_info for a track at all — rare, but real catalog
    gaps exist; callers should treat that the same as a track that
    genuinely needs no gain rather than as missing data.
    """

    track_id: str
    gain_db: float = 0.0
    peak: float = 1.0
    album_id: str = ""
    duration_ms: int = 0


class QobuzAPIError(Exception):
    """Qobuz API error."""

    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


class QobuzAPIClient:
    """Qobuz REST API client with request signing."""

    API_BASE = "https://www.qobuz.com/api.json/0.2"

    def __init__(self, app_id: str, app_secret: str):
        """
        Initialize API client.

        Args:
            app_id: Qobuz application ID
            app_secret: Qobuz application secret
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.user_auth_token: Optional[str] = None
        self.user_id: Optional[str] = None
        self.x_session_id: Optional[str] = None
        self.x_session_expires_at: int = 0
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "QobuzAPIClient":
        """Async context manager entry."""
        headers = {
            "User-Agent": "Mozilla/5.0",
            "X-App-Id": self.app_id,
        }
        self._session = aiohttp.ClientSession(headers=headers)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Async context manager exit."""
        if self._session:
            await self._session.close()
            self._session = None

    async def login_with_token(self, user_id: str, auth_token: str) -> bool:
        """Validate a cached user auth token against the Qobuz API.

        Used on startup to check if a previously saved token is still valid.
        On success, stores the (potentially refreshed) token from the response.
        """
        try:
            url = f"{self.API_BASE}/user/login"
            headers = {
                "X-App-Id": self.app_id,
                "X-User-Auth-Token": auth_token,
                "Content-Type": "text/plain;charset=UTF-8",
            }
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, data="extra=partner", headers=headers, timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(
                            f"Login validation failed: HTTP {resp.status} — {body[:200]}"
                        )
                        return False
                    response = await resp.json()

            if response and "user_auth_token" in response:
                self.user_auth_token = response["user_auth_token"]
                self.user_id = user_id
                logger.info(f"Logged in as user {self.user_id}")
                return True

        except Exception as e:
            logger.error(f"Login failed: {e}")

        return False

    async def start_session(self) -> bool:
        """
        Start a Qobuz streaming session.

        Returns:
            True if successful
        """
        now_ms = int(time.time() * 1000)
        if self.x_session_id and self.x_session_expires_at > now_ms + 60000:
            return True  # Session still valid

        try:
            request_ts = f"{time.time():.6f}"
            params = {"profile": "qbz-1"}

            # Build signature: "sessionstart" + sorted key-value pairs + timestamp + secret
            sig_string = "sessionstart"
            for key in sorted(params.keys()):
                sig_string += key + str(params[key])
            sig_string += request_ts + self.app_secret
            signature = hashlib.md5(sig_string.encode()).hexdigest()

            url = f"{self.API_BASE}/session/start"
            body = f"profile=qbz-1&request_ts={request_ts}&request_sig={signature}"

            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "X-App-Id": self.app_id,
            }
            if self.user_auth_token:
                headers["X-User-Auth-Token"] = self.user_auth_token

            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=body, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        response = await resp.json()
                        if "session_id" in response:
                            self.x_session_id = response["session_id"]
                            self.x_session_expires_at = response.get("expires_at", 0) * 1000
                            logger.debug("Session started")
                            return True
                    else:
                        body_text = await resp.text()
                        logger.warning(
                            f"Session start failed: HTTP {resp.status} — {body_text[:200]}"
                        )

        except Exception as e:
            logger.error(f"Failed to start session: {e}")

        return False

    async def get_ws_token(self, previous_jwt: str = "") -> Optional[dict[str, Any]]:
        """
        Obtain (or refresh) a Qobuz Connect WebSocket auth token directly
        from the Qobuz API, independent of the phone app.

        Mirrors the reference StreamCore32 device's own getWSToken(): POST
        {API_BASE}/qws/createToken (no previous_jwt) or /qws/refreshToken
        (given one), authenticated the same way as every other signed call
        here. This is what lets WsManager keep its control WebSocket alive
        indefinitely on its own — see docs/gapless-arming-and-token-expiry.md
        for what used to happen without it: nothing could recover once the
        WS token expired except the app proactively reconnecting, which
        doesn't reliably happen once its own OS suspends it in the
        background.

        Args:
            previous_jwt: The token being refreshed, if any. Empty requests
                a brand-new one (createToken); a real value requests a
                refresh of that specific token instead (refreshToken).

        Returns:
            Dict with 'jwt', 'exp' (epoch seconds), and 'endpoint' (wss://
            URL) keys, or None on failure.
        """
        if not await self.start_session():
            logger.debug("Session start failed; attempting WS token request without session")

        endpoint = "refreshToken" if previous_jwt else "createToken"
        url = f"{self.API_BASE}/qws/{endpoint}"
        headers = {
            "Referer": "https://play.qobuz.com/",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://play.qobuz.com",
            "X-App-Id": self.app_id,
        }
        if self.x_session_id:
            headers["X-Session-Id"] = self.x_session_id
        if self.user_auth_token:
            headers["X-User-Auth-Token"] = self.user_auth_token

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, data="jwt=jwt_qws", headers=headers, timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        body_text = await resp.text()
                        logger.warning(
                            f"WS token {endpoint} failed: HTTP {resp.status} — {body_text[:200]}"
                        )
                        return None
                    data = await resp.json()
        except Exception as e:
            logger.error(f"Failed to get WS token ({endpoint}): {e}")
            return None

        jwt_data = data.get("jwt_qws") if isinstance(data, dict) else None
        if not isinstance(jwt_data, dict) or not jwt_data.get("jwt"):
            logger.warning(f"WS token response missing jwt_qws.jwt: {data}")
            return None

        return {
            "jwt": jwt_data.get("jwt", ""),
            "exp": jwt_data.get("exp", 0),
            "endpoint": jwt_data.get("endpoint", ""),
        }

    async def get_track_url(self, track_id: str, quality: int = 27) -> Optional[dict[str, Any]]:
        """
        Get streaming URL and format info for a track.

        Args:
            track_id: Track ID
            quality: Audio quality (5, 6, 7, or 27)

        Returns:
            Dict with 'url', 'format_id', 'bit_depth', 'sampling_rate',
            'mime_type' keys, or None on failure
        """
        if not await self.start_session():
            logger.debug("Session start failed; attempting track URL without session")

        try:
            request_ts = f"{time.time():.6f}"
            format_id = str(quality)

            # Build signature for track/getFileUrl
            sig_string = (
                f"trackgetFileUrlformat_id{format_id}intentstream"
                f"track_id{track_id}{request_ts}{self.app_secret}"
            )
            signature = hashlib.md5(sig_string.encode()).hexdigest()

            params = {
                "format_id": format_id,
                "intent": "stream",
                "track_id": track_id,
                "request_ts": request_ts,
                "request_sig": signature,
            }

            url = f"{self.API_BASE}/track/getFileUrl?{urlencode(params)}"
            headers: dict[str, str] = {
                "X-App-Id": self.app_id,
            }
            if self.user_auth_token:
                headers["X-User-Auth-Token"] = self.user_auth_token
            if self.x_session_id:
                headers["X-Session-Id"] = self.x_session_id

            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        url_result = data.get("url")
                        if url_result:
                            return {
                                "url": url_result,
                                "format_id": data.get("format_id", quality),
                                "bit_depth": data.get("bit_depth", 0),
                                "sampling_rate": data.get("sampling_rate", 0),
                                "mime_type": data.get("mime_type", ""),
                                # Opaque token needed by track/reportStreamingEndJson.
                                "blob": data.get("blob", ""),
                            }
                        return None
                    else:
                        body = await resp.text()
                        logger.error(f"Failed to get track URL: {resp.status} — {body[:200]}")

        except Exception as e:
            logger.error(f"Failed to get track URL: {e}")

        return None

    def _report_headers(self) -> dict[str, str]:
        """Auth headers shared by the streaming-report endpoints."""
        headers = {"X-App-Id": self.app_id}
        if self.user_auth_token:
            headers["X-User-Auth-Token"] = self.user_auth_token
        if self.x_session_id:
            headers["X-Session-Id"] = self.x_session_id
        return headers

    async def report_streaming_start(self, *, track_id: str, format_id: int) -> bool:
        """Tell Qobuz a track started playing (track/reportStreamingStart).

        This registers the play with Qobuz, which in turn powers listening
        history and Last.fm "now playing"/scrobbling for linked accounts. The
        body mirrors the official client: a form field ``events`` holding a
        one-element JSON array. The call is unsigned; auth is via headers.
        """
        await self.start_session()
        event = {
            "user_id": int(self.user_id) if self.user_id else 0,
            "track_id": int(track_id),
            "format_id": int(format_id),
            "date": int(time.time()),
            "duration": 0,
            "online": True,
            "local": False,
        }
        body = "events=" + json.dumps([event], separators=(",", ":"))
        headers = self._report_headers()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        return await self._post_report(
            f"{self.API_BASE}/track/reportStreamingStart", body, headers, "start"
        )

    async def report_streaming_end(
        self,
        *,
        track_id: str,
        blob: str,
        context_uuid: Optional[str],
        started_at_ms: int,
        played_seconds: int,
    ) -> bool:
        """Tell Qobuz a track finished playing (track/reportStreamingEndJson).

        The end event carries how long the track was actually played, which is
        what Qobuz uses to decide whether the play counts (and scrobbles). The
        ``blob`` comes from the track/getFileUrl response and ``context_uuid``
        from the play queue.
        """
        await self.start_session()
        event: dict[str, Any] = {
            "blob": blob,
            "track_context_uuid": context_uuid or "",
            "start_stream": self._iso8601_ms(started_at_ms),
            "online": True,
            "local": False,
            "duration": int(played_seconds),
        }
        from qobuz_proxy import __version__

        payload = {
            "events": [event],
            "renderer_context": {"software_version": f"qobuz-proxy-{__version__}"},
        }
        body = json.dumps(payload, separators=(",", ":"))
        headers = self._report_headers()
        headers["Content-Type"] = "application/json"
        return await self._post_report(
            f"{self.API_BASE}/track/reportStreamingEndJson", body, headers, f"end track {track_id}"
        )

    async def _post_report(self, url: str, body: str, headers: dict[str, str], what: str) -> bool:
        """POST a streaming-report body; log and swallow failures (best-effort)."""
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=body, headers=headers, timeout=timeout) as resp:
                    # Qobuz answers reportStreamingStart with 201 Created (body
                    # still says success), so accept any 2xx.
                    if 200 <= resp.status < 300:
                        logger.debug(f"Streaming report ({what}) ok: HTTP {resp.status}")
                        return True
                    text = await resp.text()
                    logger.warning(
                        f"Streaming report ({what}) failed: HTTP {resp.status} — {text[:200]}"
                    )
                    return False
        except Exception as e:
            logger.warning(f"Streaming report ({what}) error: {type(e).__name__}: {e}")
            return False

    @staticmethod
    def _iso8601_ms(epoch_ms: int) -> str:
        """Format epoch milliseconds as ISO8601 UTC with millis and a Z suffix."""
        dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    async def get_track_metadata(self, track_id: str) -> Optional[dict[str, Any]]:
        """
        Get track metadata.

        Args:
            track_id: Track ID

        Returns:
            Track metadata dict or None
        """
        try:
            params = {"track_id": track_id, "app_id": self.app_id}
            response = await self._request_signed("track", "get", params=params)

            if not response:
                return None

            return self._flat_track_metadata(response)

        except Exception as e:
            logger.error(f"Failed to get track metadata: {e}")

        return None

    async def _get_track_list_items(self, track_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch the raw track/getList items for any number of ids, in chunks
        of TRACK_GETLIST_MAX_IDS. An id Qobuz does not recognize is simply
        absent from the result; a chunk that fails is logged and skipped, so
        the rest still come back (callers treat missing as "no data")."""
        items: list[dict[str, Any]] = []
        for start in range(0, len(track_ids), TRACK_GETLIST_MAX_IDS):
            chunk = track_ids[start : start + TRACK_GETLIST_MAX_IDS]
            try:
                body = json.dumps({"tracks_id": [int(t) for t in chunk]})
                response = await self._request_signed(
                    "track", "getList", params={}, method="POST", body=body
                )
            except Exception as e:
                logger.error(f"track/getList failed for {len(chunk)} id(s): {e}")
                continue
            if response:
                items.extend(response.get("tracks", {}).get("items", []))
        return items

    async def get_tracks_audio_info(self, track_ids: list[str]) -> dict[str, TrackAudioInfo]:
        """
        Batch-fetch per-track ReplayGain-style loudness data and album id
        (track/getList) — the data backends/dlna/sonos/gain.py uses to
        apply track gain, or an approximated album gain when a queue's
        tracks all share one album_id. Confirmed live (2026-09-15,
        qobuz-player.har): responses carry audio_info.
        {replaygain_track_gain, replaygain_track_peak} and album.id per
        track, the same shape album/get returns — Qobuz has no
        replaygain_album_gain anywhere, which is exactly why the caller
        has to approximate one.

        Args:
            track_ids: Qobuz track IDs to fetch, any number (sent in chunks
                of TRACK_GETLIST_MAX_IDS). Empty input returns {} without
                making a request.

        Returns:
            Mapping of track_id -> TrackAudioInfo for every id Qobuz
            actually returned data for. An id it doesn't recognize is
            just absent, not an error; a request failure drops only the
            affected chunk — the caller falls back to no adjustment for
            what is missing.
        """
        if not track_ids:
            return {}
        result: dict[str, TrackAudioInfo] = {}
        for item in await self._get_track_list_items(track_ids):
            track_id = str(item.get("id", ""))
            if not track_id:
                continue
            audio_info = item.get("audio_info") or {}
            album = item.get("album") or {}
            gain = audio_info.get("replaygain_track_gain", 0.0)
            if gain is None:
                # Qobuz sends the keys with null for tracks it has no loudness
                # data for (seen on real tracks); one such track must not take
                # the rest of the chunk down with a float(None). Absent here
                # means "no data" to the caller: no adjustment for this one.
                continue
            peak = audio_info.get("replaygain_track_peak", 1.0)
            result[track_id] = TrackAudioInfo(
                track_id=track_id,
                gain_db=float(gain),
                peak=float(peak) if peak is not None else 1.0,
                album_id=str(album.get("id", "")),
                duration_ms=int(item.get("duration", 0)) * 1000,
            )
        return result

    async def get_tracks_metadata(self, track_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Batch-fetch display metadata for many tracks (track/getList).

        Each value has the same flat shape get_track_metadata returns
        (title, artist, album, album_art_url, duration_ms) plus the track's
        best available format, ``max_sample_rate_hz`` and ``max_bit_depth``
        (0 when Qobuz gives none) — everything a queue publisher needs for
        DIDL-Lite and for choosing the container, in one call per 50 tracks.

        Returns:
            Mapping of track_id -> metadata for every id Qobuz returned; an
            unknown id or a failed chunk is just absent.
        """
        if not track_ids:
            return {}
        return {
            str(item["id"]): self._flat_track_metadata(item)
            for item in await self._get_track_list_items(track_ids)
            if item.get("id") is not None
        }

    @staticmethod
    def _flat_track_metadata(response: dict[str, Any]) -> dict[str, Any]:
        """Flatten a Qobuz track object (track/get or an item of track/
        getList) into the metadata dict the rest of the app uses."""
        metadata: dict[str, Any] = {
            "title": response.get("title", ""),
            "artist": "",
            "album": "",
            "album_art_url": "",
            "duration_ms": int(response.get("duration", 0)) * 1000,
            # maximum_sampling_rate is in kHz (44.1, 96, 88.2 ...)
            "max_sample_rate_hz": int(
                round(float(response.get("maximum_sampling_rate") or 0) * 1000)
            ),
            "max_bit_depth": int(response.get("maximum_bit_depth") or 0),
        }

        performer = response.get("performer")
        if performer and isinstance(performer, dict):
            metadata["artist"] = performer.get("name", "")

        album = response.get("album")
        if album and isinstance(album, dict):
            metadata["album"] = album.get("title", "")
            image = album.get("image")
            if image and isinstance(image, dict):
                metadata["album_art_url"] = image.get("large") or image.get("small") or ""

        return metadata

    async def _request_signed(
        self,
        obj: str,
        action: str,
        params: Optional[dict[str, Any]] = None,
        method: str = "GET",
        body: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Make a signed API request."""
        if params is None:
            params = {}

        request_ts = f"{time.time():.6f}"

        # Build signature
        sig_string = obj + action
        for key in sorted(params.keys()):
            sig_string += key + str(params[key])
        sig_string += request_ts + self.app_secret
        signature = hashlib.md5(sig_string.encode()).hexdigest()

        params["request_ts"] = request_ts
        params["request_sig"] = signature

        url = f"{self.API_BASE}/{obj}/{action}?{urlencode(params)}"

        try:
            session = self._session
            close_session = False
            if session is None:
                headers: dict[str, str] = {
                    "X-App-Id": self.app_id,
                    "User-Agent": "Mozilla/5.0",
                }
                if self.user_auth_token:
                    headers["X-User-Auth-Token"] = self.user_auth_token
                session = aiohttp.ClientSession(headers=headers)
                close_session = True

            timeout = aiohttp.ClientTimeout(total=10)
            try:
                if method == "POST":
                    async with session.post(url, data=body, timeout=timeout) as resp:
                        if resp.status == 200:
                            result: dict[str, Any] = await resp.json()
                            return result
                        else:
                            logger.warning(f"API request failed: {resp.status}")
                else:
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            return result
                        else:
                            logger.warning(f"API request failed: {resp.status}")
            finally:
                if close_session:
                    await session.close()

        except Exception as e:
            logger.error(f"API request error: {e}")

        return None

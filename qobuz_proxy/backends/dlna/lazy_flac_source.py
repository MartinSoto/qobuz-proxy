"""
Lazy, HTTP-Range-backed file-like source for FLAC decoding.

The point of this module: let a decoder (soundfile/libFLAC) seek within a
remote FLAC file — using its *own* seeking logic (a SEEKTABLE lookup, or a
binary search over frame headers when there's none) — without us
downloading the whole file first, and without us knowing anything about
FLAC's own structure at all.

``soundfile.SoundFile`` accepts any object implementing ``read()``,
``seek()`` and ``tell()`` in place of a real file. libsndfile drives that
object exactly like a local file: it calls ``seek()``/``read()`` wherever
*it* decides it needs bytes from — including, mid-seek, several small
probing reads while it narrows down a frame boundary. Each of those calls
becomes a real HTTP Range request here. A small read-ahead cache keeps
routine forward reads (the common case: sequential decode) from turning
into one HTTP request per tiny libsndfile read.

Synchronous by design — libsndfile's I/O callbacks are synchronous, and
this is only ever meant to be handed to soundfile from inside a worker
thread (``asyncio.to_thread``), never called from the event loop directly.
"""

from __future__ import annotations

import logging
import os
import urllib.error
import urllib.request
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

DEFAULT_CHUNK_SIZE = 256 * 1024  # read-ahead size for a single upstream fetch
DEFAULT_TIMEOUT_SECONDS = 30.0
# Qobuz signed URLs are time-limited (see proxy_server.py's
# DEFAULT_URL_MAX_AGE_SECONDS) — a track long enough to still be streaming
# once the URL dies mid-playback hits one of these.
EXPIRED_URL_STATUS_CODES = (401, 403, 410)


class LazyHttpFlacSource:
    """A read/seek/tell file-like object backed by Range requests to a URL.

    Nothing here understands FLAC — all seeking intelligence stays inside
    libFLAC (via soundfile), driven through this object's read()/seek()
    exactly as it would drive a real local file handle.
    """

    def __init__(
        self,
        url: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        refresh_url: Optional[Callable[[], Optional[str]]] = None,
    ) -> None:
        """
        Args:
            url: Initial signed streaming URL.
            refresh_url: Called (synchronously — see module docstring) when
                a fetch fails with an expired-URL status, to get a
                replacement URL to retry with. Returning None or raising
                means give up. Optional: without it, an expired URL just
                fails the read, same as before this existed.
        """
        self._url = url
        self._chunk_size = chunk_size
        self._timeout = timeout
        self._refresh_url = refresh_url
        self._pos = 0
        self._total_size = self._fetch_total_size()

        # Read-ahead cache: the most recent fetch, so sequential reads (the
        # common case) don't issue a fresh HTTP request per libsndfile read.
        self._buf = b""
        self._buf_start = 0

        # Diagnostics only — not used for correctness. Lets a caller (and
        # tests) confirm this genuinely avoided a full download.
        self.bytes_fetched = 0
        self.request_count = 0

    # -- Python file-like protocol -------------------------------------

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            n = self._total_size - self._pos
        end = min(self._pos + n, self._total_size)
        if end <= self._pos:
            return b""
        data = self._read_range(self._pos, end)
        self._pos = end
        return data

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            new_pos = offset
        elif whence == os.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == os.SEEK_END:
            new_pos = self._total_size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        self._pos = max(0, min(new_pos, self._total_size))
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        # Nothing held open between calls — every fetch is a fresh request.
        pass

    @property
    def closed(self) -> bool:
        return False

    # -- Internals --------------------------------------------------------

    def _read_range(self, start: int, end: int) -> bytes:
        """Return bytes [start, end), fetching (and caching) more from
        upstream if the current read-ahead buffer doesn't already cover it."""
        if self._buf_start <= start and end <= self._buf_start + len(self._buf):
            return self._buf[start - self._buf_start : end - self._buf_start]

        fetch_len = max(self._chunk_size, end - start)
        fetch_end = min(start + fetch_len, self._total_size)
        data = self._http_get_range(start, fetch_end - 1)
        self._buf = data
        self._buf_start = start
        return data[: end - start]

    def _http_get_range(self, start: int, end_inclusive: int) -> bytes:
        def _do() -> bytes:
            req = urllib.request.Request(
                self._url, headers={"Range": f"bytes={start}-{end_inclusive}"}
            )
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                data: bytes = resp.read()
            return data

        data = self._with_url_refresh(_do)
        self.bytes_fetched += len(data)
        self.request_count += 1
        logger.debug(
            f"LazyHttpFlacSource: fetched bytes {start}-{end_inclusive} "
            f"({len(data)} bytes, request #{self.request_count})"
        )
        return data

    def _fetch_total_size(self) -> int:
        def _do() -> int:
            req = urllib.request.Request(self._url, method="HEAD")
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                content_length = resp.headers.get("Content-Length")
            if content_length is None:
                raise OSError(f"Upstream did not report Content-Length for {self._url}")
            return int(content_length)

        return self._with_url_refresh(_do)

    def _with_url_refresh(self, do_request: Callable[[], _T]) -> _T:
        """Run do_request (a closure reading self._url); on an expired-URL
        status, refresh self._url and retry once — do_request rebuilds its
        Request from self._url each call, so a refresh in between takes
        effect on the retry automatically."""
        try:
            return do_request()
        except urllib.error.HTTPError as e:
            if e.code not in EXPIRED_URL_STATUS_CODES or self._refresh_url is None:
                raise
            logger.info(
                f"LazyHttpFlacSource: upstream {e.code} (URL likely expired) — "
                "refreshing and retrying once"
            )
            fresh = self._refresh_url()
            if not fresh:
                raise
            self._url = fresh
            return do_request()


__all__ = ["LazyHttpFlacSource"]

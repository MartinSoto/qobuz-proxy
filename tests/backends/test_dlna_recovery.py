"""Tests for DLNA SOAP error reporting and connection/transport recovery."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from qobuz_proxy.backends.dlna import client as client_module
from qobuz_proxy.backends.dlna.client import DLNAClient


class _RaisingPost:
    """Async context manager whose entry raises the given exception."""

    def __init__(self, exc: BaseException):
        self._exc = exc

    async def __aenter__(self):  # type: ignore[no-untyped-def]
        raise self._exc

    async def __aexit__(self, *args):  # type: ignore[no-untyped-def]
        return False


class _FakeSession:
    """Minimal stand-in for aiohttp.ClientSession that always fails post()."""

    def __init__(self, exc: BaseException):
        self._exc = exc

    def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return _RaisingPost(self._exc)


class _FakeResponse:
    """Async context manager standing in for an aiohttp response with a
    fixed status/body."""

    def __init__(self, status: int, body: str):
        self.status = status
        self._body = body

    async def __aenter__(self):  # type: ignore[no-untyped-def]
        return self

    async def __aexit__(self, *args):  # type: ignore[no-untyped-def]
        return False

    async def text(self) -> str:
        return self._body


class _FakeResponseSession:
    """Minimal stand-in for aiohttp.ClientSession that always answers
    post() with a fixed status/body — no exception, a real response."""

    def __init__(self, status: int, body: str = ""):
        self._status = status
        self._body = body

    def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return _FakeResponse(self._status, self._body)


class TestSoapErrorReporting:
    async def test_timeout_logs_exception_type_and_resets_session(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A connection/timeout failure must log a non-empty detail and reset the session."""
        # Avoid real retry sleeps.
        monkeypatch.setattr(client_module, "RETRY_DELAY_SECONDS", 0)

        client = DLNAClient(ip="1.2.3.4")
        # TimeoutError stringifies to an empty string — the old code logged nothing useful.
        client._session = _FakeSession(asyncio.TimeoutError())  # type: ignore[assignment]
        reset_mock = AsyncMock()
        monkeypatch.setattr(client, "reset_session", reset_mock)

        with caplog.at_level("WARNING"):
            result = await client._soap_action_detailed(
                "http://1.2.3.4/ctrl",
                client_module.UPNP_AV_TRANSPORT,
                "Play",
                {"InstanceID": "0"},
            )

        assert result.success is False
        # The log must identify the exception type even though str(e) is empty.
        assert "TimeoutError" in caplog.text
        # Session is recreated once per failed attempt so retries get a fresh socket.
        assert reset_mock.await_count == client_module.MAX_RETRIES


class TestConsecutiveFailures:
    """DLNAClient.consecutive_failures — what AudioBackend._poll_interval
    reads to back its own poll cadence off a struggling device instead of
    continuing to hammer it at the same rate (see backend.py)."""

    async def _call(self, client: DLNAClient):  # type: ignore[no-untyped-def]
        return await client._soap_action_detailed(
            "http://1.2.3.4/ctrl", client_module.UPNP_AV_TRANSPORT, "GetTransportInfo", {}
        )

    async def test_increments_when_every_retry_is_exhausted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(client_module, "RETRY_DELAY_SECONDS", 0)
        client = DLNAClient(ip="1.2.3.4")
        client._session = _FakeSession(asyncio.TimeoutError())  # type: ignore[assignment]
        # Real reset_session() would replace the fake session with a real
        # aiohttp.ClientSession after the first failed attempt, sending
        # later retries out over the real network — mock it out, same as
        # TestSoapErrorReporting above, to keep every attempt hitting the
        # fake.
        monkeypatch.setattr(client, "reset_session", AsyncMock())

        await self._call(client)
        assert client.consecutive_failures == 1

        await self._call(client)
        assert client.consecutive_failures == 2

    async def test_resets_on_a_successful_response(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(client_module, "RETRY_DELAY_SECONDS", 0)
        client = DLNAClient(ip="1.2.3.4")
        client._session = _FakeSession(asyncio.TimeoutError())  # type: ignore[assignment]
        monkeypatch.setattr(client, "reset_session", AsyncMock())
        await self._call(client)
        assert client.consecutive_failures == 1

        client._session = _FakeResponseSession(status=200, body="<ok/>")  # type: ignore[assignment]
        result = await self._call(client)

        assert result.success is True
        assert client.consecutive_failures == 0

    async def test_resets_on_a_permanent_upnp_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A device answering "Invalid Action" is still reachable and
        healthy as far as backoff cares — only a total communication
        failure counts."""
        monkeypatch.setattr(client_module, "RETRY_DELAY_SECONDS", 0)
        client = DLNAClient(ip="1.2.3.4")
        client._session = _FakeSession(asyncio.TimeoutError())  # type: ignore[assignment]
        monkeypatch.setattr(client, "reset_session", AsyncMock())
        await self._call(client)
        assert client.consecutive_failures == 1

        client._session = _FakeResponseSession(  # type: ignore[assignment]
            status=500, body="<errorCode>401</errorCode>"
        )
        result = await self._call(client)

        assert result.success is False
        assert result.error_code == 401
        assert client.consecutive_failures == 0


class _TrackedSession:
    """Minimal stand-in for aiohttp.ClientSession that counts close() calls."""

    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class TestResetSessionConcurrency:
    async def test_overlapping_resets_of_the_same_session_converge_on_one_replacement(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two SOAP calls (e.g. the poll loop and a command) can each hit a
        connection error against the same session within milliseconds of
        each other on a flaky device — observed directly in production as
        repeated "Unclosed client session" errors. Both must converge on
        one replacement instead of each blindly building (and orphaning)
        their own — see reset_session's own docstring."""
        client = DLNAClient(ip="1.2.3.4")
        original = _TrackedSession()
        client._session = original  # type: ignore[assignment]

        built: list[_TrackedSession] = []

        def _build() -> _TrackedSession:
            session = _TrackedSession()
            built.append(session)
            return session

        monkeypatch.setattr(client, "_build_session", _build)

        # Both callers observed `original` as the session they were using.
        await asyncio.gather(
            client.reset_session(expected_session=original),  # type: ignore[arg-type]
            client.reset_session(expected_session=original),  # type: ignore[arg-type]
        )

        assert original.close_calls == 1  # closed exactly once, not twice
        assert len(built) == 1  # only one replacement built, not two orphaned ones
        assert client._session is built[0]
        assert built[0].close_calls == 0  # the surviving session was never closed

    async def test_reset_without_expected_session_forces_it_unconditionally(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The explicit external recovery call (backend.py's own transport
        recovery) has no prior session to compare against — it must always
        reset, regardless of what's currently installed."""
        client = DLNAClient(ip="1.2.3.4")
        original = _TrackedSession()
        client._session = original  # type: ignore[assignment]
        monkeypatch.setattr(client, "_build_session", _TrackedSession)

        await client.reset_session()

        assert original.close_calls == 1
        assert client._session is not original


class _GatedResponse:
    """Async context manager whose entry blocks on an external event —
    lets a test deterministically control when one call's SOAP response
    "arrives" relative to another call queued behind it."""

    def __init__(self, status: int, body: str, gate: asyncio.Event):
        self.status = status
        self._body = body
        self._gate = gate

    async def __aenter__(self):  # type: ignore[no-untyped-def]
        await self._gate.wait()
        return self

    async def __aexit__(self, *args):  # type: ignore[no-untyped-def]
        return False

    async def text(self) -> str:
        return self._body


class _GatedSession:
    """Minimal stand-in for aiohttp.ClientSession whose post() doesn't
    resolve until the given gate is set."""

    def __init__(self, gate: asyncio.Event, status: int = 200, body: str = "<ok/>"):
        self._gate = gate
        self._status = status
        self._body = body

    def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return _GatedResponse(self._status, self._body, self._gate)


class _CountingSession:
    """Minimal stand-in for aiohttp.ClientSession that always answers
    immediately (200 OK), tracking how many post() calls are concurrently
    inside their response context at once."""

    def __init__(self) -> None:
        self.max_concurrent = 0
        self._concurrent = 0

    def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return self._response()

    @asynccontextmanager
    async def _response(self):  # type: ignore[no-untyped-def]
        self._concurrent += 1
        self.max_concurrent = max(self.max_concurrent, self._concurrent)
        try:
            await asyncio.sleep(0)  # yield control, giving a race a chance to happen
            yield _FakeResponse(200, "<ok/>")
        finally:
            self._concurrent -= 1


class TestCallSerialization:
    """DLNAClient._call_lock — every SOAP call to one device, retries
    included, runs to completion before the next caller's call even
    starts, regardless of which independent task (poll loop, a command,
    volume change) issued it. See its own docstring for the device-
    overload reasoning."""

    async def _call(self, client: DLNAClient):  # type: ignore[no-untyped-def]
        return await client._soap_action_detailed(
            "http://1.2.3.4/ctrl", client_module.UPNP_AV_TRANSPORT, "GetTransportInfo", {}
        )

    async def test_concurrent_calls_never_actually_overlap(self) -> None:
        client = DLNAClient(ip="1.2.3.4")
        session = _CountingSession()
        client._session = session  # type: ignore[assignment]

        results = await asyncio.gather(self._call(client), self._call(client), self._call(client))

        assert all(r.success for r in results)
        assert session.max_concurrent == 1

    async def test_a_second_caller_waits_for_the_first_to_finish(self) -> None:
        client = DLNAClient(ip="1.2.3.4")
        gate = asyncio.Event()
        client._session = _GatedSession(gate)  # type: ignore[assignment]
        order: list[str] = []

        async def _first():  # type: ignore[no-untyped-def]
            await self._call(client)
            order.append("first")

        async def _second():  # type: ignore[no-untyped-def]
            await self._call(client)
            order.append("second")

        first_task = asyncio.create_task(_first())
        await asyncio.sleep(0)  # let the first call actually start and block on the gate
        second_task = asyncio.create_task(_second())
        await asyncio.sleep(0)  # let the second call try to acquire the lock and queue

        assert order == []  # first is gated, second is queued behind it — neither done

        gate.set()  # release the first call's response
        await asyncio.gather(first_task, second_task)

        assert order == ["first", "second"]

    async def test_a_slow_retrying_call_keeps_a_second_caller_waiting_throughout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The lock covers the whole retry loop, not just one attempt — a
        caller queued behind a call that's still failing and retrying
        must not sneak its own connection in between retries."""
        monkeypatch.setattr(client_module, "RETRY_DELAY_SECONDS", 0)
        client = DLNAClient(ip="1.2.3.4")
        client._session = _FakeSession(asyncio.TimeoutError())  # type: ignore[assignment]
        monkeypatch.setattr(client, "reset_session", AsyncMock())
        order: list[str] = []

        async def _failing():  # type: ignore[no-untyped-def]
            await self._call(client)
            order.append("failing")

        async def _second():  # type: ignore[no-untyped-def]
            await self._call(client)
            order.append("second")

        await asyncio.gather(_failing(), _second())

        # The failing call (all MAX_RETRIES attempts) must fully finish
        # before the second one, queued behind it, ever gets a turn.
        assert order == ["failing", "second"]
        assert client.consecutive_failures == 2  # each call exhausted its own retries

    async def test_device_description_fetch_shares_the_same_lock(self) -> None:
        """_fetch_device_description (connect()'s one-time call) goes
        through the same self._call_lock as every other call — a poll or
        command racing a fresh connect()/retarget must queue behind it
        too, not open a second connection to a device that may still be
        settling in from just being contacted for the first time."""

        class _GatedGetSession:
            def __init__(self, gate: asyncio.Event) -> None:
                self._gate = gate

            def get(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                return _GatedResponse(200, "<root/>", self._gate)

            def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                return _GatedResponse(200, "<ok/>", self._gate)

        client = DLNAClient(ip="1.2.3.4", description_url="http://1.2.3.4/desc.xml")
        gate = asyncio.Event()
        client._session = _GatedGetSession(gate)  # type: ignore[assignment]
        order: list[str] = []

        async def _describe():  # type: ignore[no-untyped-def]
            try:
                await client._fetch_device_description()
            except Exception:
                pass  # the fake XML won't parse into a full DLNADeviceInfo — irrelevant here
            order.append("describe")

        async def _soap_call():  # type: ignore[no-untyped-def]
            await self._call(client)
            order.append("soap")

        describe_task = asyncio.create_task(_describe())
        await asyncio.sleep(0)  # let the fetch start and block on the gate
        soap_task = asyncio.create_task(_soap_call())
        await asyncio.sleep(0)  # let the SOAP call try to acquire the lock and queue

        assert order == []

        gate.set()
        await asyncio.gather(describe_task, soap_task)

        assert order == ["describe", "soap"]


class TestTransportRecovery:
    async def _make_backend(self):  # type: ignore[no-untyped-def]
        """Build a DLNABackend with a mocked client, bypassing real connection."""
        from qobuz_proxy.backends.dlna.backend import DLNABackend

        backend = DLNABackend.__new__(DLNABackend)
        # Minimal attributes used by _play_via_transport / _try_transport_sequence.
        backend._client = AsyncMock()
        backend._notify_playback_error = lambda msg: None  # type: ignore[assignment]
        return backend

    async def test_recovers_after_first_seturi_failure(self) -> None:
        """First SetAVTransportURI fails; after reset+stop the retry succeeds."""
        backend = await self._make_backend()
        client = backend._client
        # set_av_transport_uri: fail first, succeed on retry. play: succeed.
        client.set_av_transport_uri = AsyncMock(side_effect=[False, True])
        client.play = AsyncMock(return_value=True)

        ok = await backend._play_via_transport("http://proxy/1.flac", "<didl/>")

        assert ok is True
        # Recovery path ran: session reset + transport stop before the retry.
        client.reset_session.assert_awaited_once()
        client.stop.assert_awaited_once()
        assert client.set_av_transport_uri.await_count == 2

    async def test_reports_error_when_recovery_also_fails(self) -> None:
        """If the retry also fails, a playback error is reported (no silent hang)."""
        backend = await self._make_backend()
        client = backend._client
        client.set_av_transport_uri = AsyncMock(return_value=False)
        client.play = AsyncMock(return_value=True)

        errors = []
        backend._notify_playback_error = lambda msg: errors.append(msg)  # type: ignore[assignment]

        ok = await backend._play_via_transport("http://proxy/1.flac", "<didl/>")

        assert ok is False
        assert errors == ["Failed to set transport URI"]
        client.reset_session.assert_awaited_once()

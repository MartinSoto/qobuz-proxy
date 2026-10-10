"""wake_poll(): a device event runs the next poll cycle now, spaced from the
previous one; with no event the loop waits out its interval as before."""

import asyncio
import time

from qobuz_proxy.backends.dlna import backend as backend_module
from qobuz_proxy.backends.dlna.backend import DLNABackend


def make(interval: float) -> DLNABackend:
    backend = DLNABackend.__new__(DLNABackend)
    backend._poll_interval = lambda: interval  # type: ignore[method-assign]
    return backend


class TestSleepUntilNextCycle:
    async def test_without_a_wake_the_whole_interval_passes(self) -> None:
        backend = make(0.1)

        started = time.monotonic()
        await backend._sleep_until_next_cycle()

        assert time.monotonic() - started >= 0.09

    async def test_a_wake_cuts_the_wait_short(self) -> None:
        backend = make(5.0)
        backend._last_cycle_at = 0.0  # long ago: no spacing to make up

        asyncio.get_running_loop().call_later(0.02, backend.wake_poll)
        started = time.monotonic()
        await backend._sleep_until_next_cycle()

        assert time.monotonic() - started < 1.0

    async def test_a_wake_is_spaced_from_the_previous_cycle(self, monkeypatch) -> None:
        monkeypatch.setattr(backend_module, "MIN_EVENT_CYCLE_GAP_SECONDS", 0.2)
        backend = make(5.0)
        backend._last_cycle_at = time.monotonic()  # a cycle just ran

        backend.wake_poll()
        started = time.monotonic()
        await backend._sleep_until_next_cycle()

        assert time.monotonic() - started >= 0.18

    async def test_a_burst_of_wakes_is_one_cycle(self) -> None:
        backend = make(0.05)
        backend._last_cycle_at = 0.0

        backend.wake_poll()
        backend.wake_poll()
        backend.wake_poll()
        await backend._sleep_until_next_cycle()  # consumes the wake

        started = time.monotonic()
        await backend._sleep_until_next_cycle()  # nothing left: the interval again

        assert time.monotonic() - started >= 0.04

"""slow_step: a step that takes long is named in the log, a quick one is not."""

import asyncio
import logging

import pytest

from qobuz_proxy.backends.dlna import client as client_module
from qobuz_proxy.backends.dlna.client import DLNAClient, SoapResult
from qobuz_proxy.timing import slow_step

logger = logging.getLogger("test.timing")


class TestSlowStep:
    def test_a_quick_step_says_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger="test.timing"):
            with slow_step(logger, "quick", after=5.0):
                pass

        assert "Slow step" not in caplog.text

    def test_a_slow_step_is_named_with_its_time(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger="test.timing"):
            with slow_step(logger, "stopping the device", after=0.0):
                pass

        assert "Slow step: stopping the device took" in caplog.text

    def test_a_step_that_fails_is_still_reported(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger="test.timing"):
            with pytest.raises(ValueError):
                with slow_step(logger, "failing", after=0.0):
                    raise ValueError("boom")

        assert "Slow step: failing took" in caplog.text


class TestSlowSoap:
    async def test_a_call_waiting_for_the_device_is_named(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(client_module, "SLOW_SOAP_SECONDS", 0.05)
        client = DLNAClient("10.0.0.7")

        async def slow(*args, **kwargs):  # type: ignore[no-untyped-def]
            await asyncio.sleep(0.15)
            return SoapResult(success=True, body="<x/>")

        client._soap_action_locked = slow  # type: ignore[method-assign]

        with caplog.at_level(logging.INFO, logger=client_module.__name__):
            # The second call waits for the first: serialized per device.
            await asyncio.gather(
                client._soap_action_detailed("u", "s", "Stop", {}),
                client._soap_action_detailed("u", "s", "Play", {}),
            )

        assert "Slow step: SOAP Stop to 10.0.0.7" in caplog.text
        assert "SOAP Play to 10.0.0.7 waited" in caplog.text

    async def test_a_quick_call_says_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        client = DLNAClient("10.0.0.7")

        async def quick(*args, **kwargs):  # type: ignore[no-untyped-def]
            return SoapResult(success=True, body="<x/>")

        client._soap_action_locked = quick  # type: ignore[method-assign]

        with caplog.at_level(logging.INFO, logger=client_module.__name__):
            await client._soap_action_detailed("u", "s", "Stop", {})

        assert "Slow step" not in caplog.text

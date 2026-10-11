"""Saying where the time went when a step is slower than it should be."""

import logging
import time
from contextlib import contextmanager
from typing import Iterator

SLOW_STEP_SECONDS = 1.0


@contextmanager
def slow_step(
    logger: logging.Logger, what: str, after: float = SLOW_STEP_SECONDS
) -> Iterator[None]:
    """Log `what` at info level if the block takes longer than `after` seconds
    (whatever way it ends). Costs nothing otherwise."""
    started = time.monotonic()
    try:
        yield
    finally:
        took = time.monotonic() - started
        if took > after:
            logger.info(f"Slow step: {what} took {took:.1f}s")

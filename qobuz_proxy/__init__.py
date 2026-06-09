"""
QobuzProxy - Headless Qobuz music player service.

A Qobuz Connect renderer that streams to DLNA devices.
"""

import os
import subprocess
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    __version__ = version("qobuz-proxy")
except PackageNotFoundError:
    __version__ = "0.0.0+dev"


def _detect_commit() -> str:
    """Resolve a short git commit hash, or empty string if unavailable.

    Order: $QOBUZPROXY_COMMIT (set at Docker build time) > local `git`
    invocation (works in dev checkouts) > "".
    """
    env = os.environ.get("QOBUZPROXY_COMMIT", "").strip()
    if env:
        return env[:7]
    try:
        repo_root = Path(__file__).resolve().parent.parent
        if not (repo_root / ".git").exists():
            return ""
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=2,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return ""


__commit__ = _detect_commit()

from .app import QobuzProxy
from .config import Config, load_config, ConfigError

__all__ = [
    "__version__",
    "__commit__",
    "QobuzProxy",
    "Config",
    "load_config",
    "ConfigError",
]

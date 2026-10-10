"""A room keeps its audio proxy port across restarts.

The queue a Sonos holds (queue handover) names the proxy port its entries are
served from. Ports used to be handed out in the order the rooms came up, so after
a restart a room could get another port (or, with fewer speakers, a port no one
listens on), and the device ran through its whole queue with every entry refused
and stopped. The port a coordinator device was given is remembered in a small
file and given back to the same device next time.
"""

import json
import logging
from pathlib import Path
from typing import Iterable, Optional

from qobuz_proxy.config import DEFAULT_PROXY_PORT

logger = logging.getLogger(__name__)

FILE_NAME = "proxy-ports.json"


class PortRegistry:
    def __init__(self, path: Optional[Path]) -> None:
        self._path = path
        self._ports: dict[str, int] = {}
        if path is not None:
            try:
                data = json.loads(path.read_text())
                self._ports = {str(k): int(v) for k, v in data.items()}
            except FileNotFoundError:
                pass
            except (OSError, ValueError, AttributeError) as e:
                logger.warning(f"Ignoring unreadable port registry {path}: {e}")

    def port_for(self, key: str, taken: Iterable[int]) -> int:
        """The port remembered for `key` if no running speaker holds it, else
        the lowest port no one holds and no other device has been promised.
        Remembers the result."""
        in_use = {p for p in taken if p}
        remembered = self._ports.get(key)
        if remembered is not None and remembered not in in_use:
            return remembered
        promised = {p for k, p in self._ports.items() if k != key}
        port = DEFAULT_PROXY_PORT
        while port in in_use or port in promised:
            port += 1
        self._ports[key] = port
        self._save()
        return port

    def _save(self) -> None:
        if self._path is None:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(self._ports, indent=1, sort_keys=True))
        except OSError as e:
            logger.warning(f"Could not save the port registry {self._path}: {e}")


__all__ = ["FILE_NAME", "PortRegistry"]

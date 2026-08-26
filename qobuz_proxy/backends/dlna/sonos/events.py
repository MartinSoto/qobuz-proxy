"""
GENA (UPnP General Event Notification Architecture) subscription client for
Sonos's ZoneGroupTopology service.

Reduces household-topology change detection latency from "up to one poll
interval" to "near-instant": rather than periodically re-fetching
GetZoneGroupState, this SUBSCRIBEs once to a household member's
ZoneGroupTopology event channel and the device pushes a NOTIFY — carrying
the same ZoneGroupState XML a SOAP GetZoneGroupState response would — the
moment anything actually changes.

This is deliberately a thin, low-level GENA client only: subscribe, renew,
unsubscribe, and hand a NOTIFY body to a callback. It knows nothing about
topology diffing or Speaker lifecycle — SonosDiscoveryManager composes this
with polling (kept as a fallback/safety net; see its own module docstring)
and the existing topology parsing in sonos_topology.py, which already
accepts a bare ZoneGroupState-bearing XML document regardless of what
wraps it (a SOAP envelope for polling, a GENA propertyset here).

Subscription lifecycle, per the GENA spec:
- SUBSCRIBE with CALLBACK/NT/TIMEOUT headers -> 200 response with a SID and
  the (possibly server-adjusted) TIMEOUT actually granted.
- Renewal: SUBSCRIBE again with just SID/TIMEOUT (no CALLBACK/NT) before
  the granted timeout elapses.
- UNSUBSCRIBE with SID when done.

Verifying this actually reduces real-world detection latency, and that the
subscribed device's NOTIFYs reliably reach this process on a given
network, both require testing against real hardware — this module can only
be exercised against a fake local HTTP responder in tests.
"""

import logging
import re
import socket
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

import aiohttp
from aiohttp import web

from ..discovery import DiscoveredDevice

logger = logging.getLogger(__name__)

ZONE_GROUP_TOPOLOGY_EVENT_PATH = "/ZoneGroupTopology/Event"

# What we request; the device may grant something else (or "infinite"),
# which we respect via the response's own TIMEOUT header.
REQUESTED_TIMEOUT_SECONDS = 1800
# Fallback for a response with a missing/unparseable TIMEOUT header.
DEFAULT_TIMEOUT_SECONDS = 1800
# Renew this long before the granted timeout actually elapses.
RENEWAL_MARGIN_SECONDS = 120
# "Second-infinite" is technically legal GENA; treat it as this many
# seconds so a renewal loop always has *some* future check-in point.
INFINITE_TIMEOUT_FALLBACK_SECONDS = 24 * 3600

REQUEST_TIMEOUT_SECONDS = 10.0

# NotifyCallback receives the raw NOTIFY body text.
NotifyCallback = Callable[[str], Awaitable[None]]


def _is_sonos(manufacturer: str) -> bool:
    return "sonos" in manufacturer.lower()


def _parse_timeout_header(value: str) -> int:
    """Parse a GENA TIMEOUT header ("Second-1800", "Second-infinite")."""
    match = re.match(r"Second-(\d+|infinite)", value.strip(), re.IGNORECASE)
    if not match:
        return DEFAULT_TIMEOUT_SECONDS
    token = match.group(1)
    if token.lower() == "infinite":
        return INFINITE_TIMEOUT_FALLBACK_SECONDS
    return int(token)


def get_local_ip() -> Optional[str]:
    """Best-effort local outbound IP, for building a callback URL Sonos can reach."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        finally:
            s.close()
        return str(ip)
    except Exception as e:
        logger.debug(f"Could not determine local IP for GENA callback: {e}")
        return None


@dataclass
class GenaSubscription:
    """An active GENA subscription to one device's ZoneGroupTopology service."""

    sid: str
    ip: str
    port: int
    timeout_seconds: int
    subscribed_at: float  # time.monotonic()

    @property
    def expires_at(self) -> float:
        return self.subscribed_at + self.timeout_seconds

    @property
    def needs_renewal(self) -> bool:
        return time.monotonic() >= self.expires_at - RENEWAL_MARGIN_SECONDS


class SonosEventSubscriber:
    """
    Low-level GENA client for Sonos's ZoneGroupTopology event channel.

    Its aiohttp route must be registered before the Application starts
    serving (aiohttp freezes the router once AppRunner.setup() runs) — but
    what should actually happen with a NOTIFY is only known once a
    SonosDiscoveryManager exists, which happens later (after login).
    `on_notify` is therefore a mutable slot, not fixed at construction: the
    caller creates one persistent subscriber early (registering its route
    then), and whichever component currently wants events sets/clears
    `on_notify` as it starts/stops. A NOTIFY arriving with no `on_notify`
    set (or a mismatched SID) is answered 412 and otherwise ignored.

    Usage:
        subscriber = SonosEventSubscriber()
        subscriber.register_route(web_app)  # once, before the app starts serving
        ...later, once something wants events...
        subscriber.on_notify = my_callback
        await subscriber.subscribe(devices, callback_url)
        ...
        if subscriber.subscription and subscriber.subscription.needs_renewal:
            await subscriber.renew()
        ...
        await subscriber.unsubscribe()
        subscriber.on_notify = None
    """

    def __init__(self, callback_path: str = "/sonos-events") -> None:
        self.on_notify: Optional[NotifyCallback] = None
        self.callback_path = callback_path
        self.subscription: Optional[GenaSubscription] = None

    def register_route(self, app: web.Application) -> None:
        """Register the NOTIFY handler on a running aiohttp Application."""
        app.router.add_route("NOTIFY", self.callback_path, self._handle_notify_request)

    async def subscribe(
        self, devices: list[DiscoveredDevice], callback_url: str
    ) -> Optional[GenaSubscription]:
        """
        Try each discovered Sonos device until one accepts a subscription.

        Returns the new subscription, or None if none could be reached.
        """
        sonos_devices = [d for d in devices if _is_sonos(d.manufacturer)]
        if not sonos_devices:
            return None

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for device in sonos_devices:
                sub = await self._subscribe_to(session, device.ip, device.port, callback_url)
                if sub is not None:
                    self.subscription = sub
                    logger.info(
                        f"Subscribed to Sonos topology events on {device.ip} "
                        f"(sid={sub.sid}, timeout={sub.timeout_seconds}s)"
                    )
                    return sub

        logger.debug("No Sonos device accepted a GENA subscription")
        return None

    async def _subscribe_to(
        self, session: aiohttp.ClientSession, ip: str, port: int, callback_url: str
    ) -> Optional[GenaSubscription]:
        url = f"http://{ip}:{port}{ZONE_GROUP_TOPOLOGY_EVENT_PATH}"
        headers = {
            "CALLBACK": f"<{callback_url}>",
            "NT": "upnp:event",
            "TIMEOUT": f"Second-{REQUESTED_TIMEOUT_SECONDS}",
        }
        try:
            async with session.request("SUBSCRIBE", url, headers=headers) as response:
                if response.status != 200:
                    logger.debug(f"SUBSCRIBE to {ip} returned {response.status}")
                    return None
                sid = response.headers.get("SID")
                if not sid:
                    logger.debug(f"SUBSCRIBE to {ip} succeeded but returned no SID")
                    return None
                timeout_s = _parse_timeout_header(
                    response.headers.get("TIMEOUT", f"Second-{DEFAULT_TIMEOUT_SECONDS}")
                )
                return GenaSubscription(
                    sid=sid,
                    ip=ip,
                    port=port,
                    timeout_seconds=timeout_s,
                    subscribed_at=time.monotonic(),
                )
        except Exception as e:
            logger.debug(f"SUBSCRIBE to {ip} failed: {e}")
            return None

    async def renew(self) -> bool:
        """Renew the current subscription. Returns False (and clears the
        subscription) if renewal failed — the caller should treat that the
        same as never having subscribed, and try a fresh subscribe."""
        sub = self.subscription
        if sub is None:
            return False

        url = f"http://{sub.ip}:{sub.port}{ZONE_GROUP_TOPOLOGY_EVENT_PATH}"
        headers = {"SID": sub.sid, "TIMEOUT": f"Second-{REQUESTED_TIMEOUT_SECONDS}"}
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request("SUBSCRIBE", url, headers=headers) as response:
                    if response.status != 200:
                        logger.debug(f"Renewal for {sub.ip} returned {response.status}")
                        self.subscription = None
                        return False
                    timeout_s = _parse_timeout_header(
                        response.headers.get("TIMEOUT", f"Second-{DEFAULT_TIMEOUT_SECONDS}")
                    )
                    self.subscription = GenaSubscription(
                        sid=response.headers.get("SID", sub.sid),
                        ip=sub.ip,
                        port=sub.port,
                        timeout_seconds=timeout_s,
                        subscribed_at=time.monotonic(),
                    )
                    logger.debug(f"Renewed Sonos topology event subscription on {sub.ip}")
                    return True
        except Exception as e:
            logger.debug(f"Renewal for {sub.ip} failed: {e}")
            self.subscription = None
            return False

    async def unsubscribe(self) -> None:
        """Best-effort UNSUBSCRIBE. Always clears local subscription state."""
        sub = self.subscription
        self.subscription = None
        if sub is None:
            return

        url = f"http://{sub.ip}:{sub.port}{ZONE_GROUP_TOPOLOGY_EVENT_PATH}"
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await session.request("UNSUBSCRIBE", url, headers={"SID": sub.sid})
        except Exception as e:
            logger.debug(f"UNSUBSCRIBE to {sub.ip} failed (harmless — it will just expire): {e}")

    async def _handle_notify_request(self, request: web.Request) -> web.Response:
        """aiohttp handler for incoming NOTIFY requests."""
        sid = request.headers.get("SID", "")
        if self.on_notify is None or self.subscription is None or sid != self.subscription.sid:
            # No current subscriber (feature not in use, or between
            # start/stop), or a foreign/stale subscription (e.g. from a
            # dropped/renewed-elsewhere subscription) — 412 Precondition
            # Failed is the GENA-spec-correct response; harmless either way.
            return web.Response(status=412)

        body = await request.text()
        try:
            await self.on_notify(body)
        except Exception as e:
            logger.warning(f"Error handling Sonos topology NOTIFY: {e}")
        return web.Response(status=200)


__all__ = [
    "GenaSubscription",
    "SonosEventSubscriber",
    "get_local_ip",
]

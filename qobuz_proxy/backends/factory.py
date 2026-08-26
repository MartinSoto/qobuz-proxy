"""
Backend factory and registry.

Provides factory methods to instantiate backends by type name.
"""

import logging
from typing import Optional

from qobuz_proxy.config import Config

from .base import AudioBackend
from .dlna import DLNABackend

logger = logging.getLogger(__name__)


class BackendNotFoundError(Exception):
    """Raised when requested backend type is not available."""

    pass


class BackendRegistry:
    """
    Registry of available backend types.

    Backends register themselves here with their type name.
    Factory uses this to instantiate backends.
    """

    _backends: dict[str, type[AudioBackend]] = {}

    @classmethod
    def register(cls, type_name: str, backend_class: type[AudioBackend]) -> None:
        """Register a backend class."""
        cls._backends[type_name] = backend_class
        logger.debug(f"Registered backend type: {type_name}")

    @classmethod
    def get(cls, type_name: str) -> Optional[type[AudioBackend]]:
        """Get backend class by type name."""
        return cls._backends.get(type_name)

    @classmethod
    def available_types(cls) -> list[str]:
        """Get list of registered backend type names."""
        return list(cls._backends.keys())


class BackendFactory:
    """
    Factory for creating audio backend instances.

    Usage:
        backend = await BackendFactory.create_from_config(config)
    """

    @classmethod
    async def create_from_config(cls, config: Config) -> AudioBackend:
        """Create a backend based on configuration."""
        backend_type = config.backend.type

        # Check if type is available
        backend_class = BackendRegistry.get(backend_type)
        if not backend_class:
            available = BackendRegistry.available_types()
            raise BackendNotFoundError(
                f"Backend type '{backend_type}' not available. Available types: {available}"
            )

        # Dispatch to type-specific factory method
        if backend_type == "dlna":
            ip = config.backend.dlna.ip
            port = config.backend.dlna.port or 1400
            description_url = config.backend.dlna.description_url or None
            if not description_url:
                description_url = await cls._discover_description_url(ip, port)
            dlna_backend_class = await cls._select_dlna_backend_class(ip, port, description_url)
            return await cls.create_dlna(
                ip=ip,
                port=port,
                description_url=description_url,
                hires_downsampling=config.backend.dlna.hires_downsampling,
                backend_class=dlna_backend_class,
            )
        elif backend_type == "local":
            return await cls.create_local(
                device=config.backend.local.device,
                buffer_size=config.backend.local.buffer_size,
            )
        else:
            # Generic instantiation for registered backends
            return backend_class(name=f"{backend_type} Backend")

    @classmethod
    async def create_dlna(
        cls,
        ip: str,
        port: int = 1400,
        fixed_volume: bool = False,
        name: Optional[str] = None,
        description_url: Optional[str] = None,
        hires_downsampling: bool = False,
        backend_class: Optional[type[DLNABackend]] = None,
    ) -> AudioBackend:
        """
        Create a DLNA backend.

        Args:
            ip: DLNA device IP address
            port: DLNA device port (default 1400 for Sonos)
            fixed_volume: If True, ignore volume commands
            name: Display name (auto-detected if not provided)
            description_url: Full URL to UPnP device description XML
            hires_downsampling: Experimental, opt-in hi-res-with-on-the-fly-
                downsampling for capable devices — see DLNABackend.__init__.
            backend_class: Concrete DLNABackend subclass to instantiate.
                Defaults to the plain generic DLNABackend — pass
                sonos.SonosBackend for a device already known to be Sonos
                (see create_from_config's manufacturer probe). A direct
                caller that doesn't need that distinction (tests, a
                caller that already knows what it wants) can just omit it.

        Returns:
            Connected DLNABackend instance

        Raises:
            BackendNotFoundError: If connection fails
        """
        cls_ = backend_class or DLNABackend
        backend = cls_(
            ip=ip,
            port=port,
            fixed_volume=fixed_volume,
            name=name,
            description_url=description_url,
            hires_downsampling=hires_downsampling,
        )
        if await backend.connect():
            return backend
        raise BackendNotFoundError(f"Failed to connect to DLNA device at {ip}:{port}")

    @classmethod
    async def _select_dlna_backend_class(
        cls, ip: str, port: int, description_url: Optional[str]
    ) -> type[DLNABackend]:
        """
        Probe the device's manufacturer to decide whether to build a plain
        DLNABackend or a sonos.SonosBackend.

        This is a separate, throwaway connect from the real one
        create_dlna() makes right after — one extra lightweight
        HTTP GET (the device description XML) per speaker at startup or
        retarget, accepted so the *class* is right from construction
        instead of a runtime manufacturer flag living inside one shared
        class.

        Falls back to plain DLNABackend (never raises) — a probe failure
        here isn't fatal, the real connect right after will surface it
        properly.
        """
        from .dlna.client import DLNAClient
        from .dlna.sonos import SonosBackend

        probe = DLNAClient(ip, port, description_url=description_url)
        try:
            device_info = await probe.connect()
        except Exception as e:
            logger.debug(f"Could not probe manufacturer for {ip}:{port}: {e}")
            return DLNABackend
        finally:
            await probe.disconnect()

        if "sonos" in (device_info.manufacturer or "").lower():
            return SonosBackend
        return DLNABackend

    @classmethod
    async def _discover_description_url(cls, target_ip: str, target_port: int) -> Optional[str]:
        """Run SSDP discovery and find the description URL for a device by IP.

        Uses a short timeout since we only need to match a specific device.

        Returns:
            SSDP LOCATION URL if found, None otherwise.
        """
        from qobuz_proxy.backends.dlna.discovery import DLNADiscovery

        try:
            discovery = DLNADiscovery()
            devices = await discovery.discover(timeout=3.0)
            for device in devices:
                if device.ip == target_ip and device.location:
                    logger.info(
                        f"Auto-discovered description URL for {target_ip}: {device.location}"
                    )
                    return device.location
            logger.debug(f"SSDP discovery did not find device at {target_ip}")
        except Exception as e:
            logger.debug(f"SSDP discovery failed: {e}")
        return None

    @classmethod
    async def create_local(
        cls,
        device: str = "default",
        buffer_size: int = 2048,
        name: Optional[str] = None,
    ) -> AudioBackend:
        """Create a local audio backend."""
        # Lazy import to avoid requiring sounddevice for DLNA users
        from qobuz_proxy.backends.local import LocalAudioBackend

        backend = LocalAudioBackend(
            device=device,
            buffer_size=buffer_size,
            name=name or "Local Audio",
        )
        if await backend.connect():
            return backend
        raise BackendNotFoundError("Failed to initialize local audio backend")

    @classmethod
    def list_available_backends(cls) -> list[str]:
        """List available backend types."""
        return BackendRegistry.available_types()


# Register backends
BackendRegistry.register("dlna", DLNABackend)

# Register local backend (lazy - import only when used)
try:
    from qobuz_proxy.backends.local import LocalAudioBackend

    BackendRegistry.register("local", LocalAudioBackend)
except ImportError:
    pass  # sounddevice not installed

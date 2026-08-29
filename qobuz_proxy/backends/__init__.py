"""
Audio backends module.

Provides abstract interface and factory for audio output backends.
"""

from .base import (
    AudioBackend,
    BufferStatusCallback,
    NextTrackStartedCallback,
    PlaybackErrorCallback,
    PositionUpdateCallback,
    StateChangeCallback,
    TrackEndedCallback,
)
from .factory import (
    BackendFactory,
    BackendNotFoundError,
    BackendRegistry,
)
from .types import (
    BackendInfo,
    BackendTrackMetadata,
    BufferStatus,
    PlaybackState,
    PlayResult,
)
from .dlna import (
    DLNABackend,
    DLNAClient,
    DLNAClientError,
    DLNADeviceInfo,
    AudioProxyServer,
    RegisteredTrack,
    ResolvedTrack,
)

try:
    from .local import LocalAudioBackend
except ImportError:
    pass  # sounddevice/numpy not installed

__all__ = [
    # Types
    "BackendInfo",
    "BackendTrackMetadata",
    "BufferStatus",
    "PlaybackState",
    "PlayResult",
    # Base class
    "AudioBackend",
    # Callback types
    "BufferStatusCallback",
    "NextTrackStartedCallback",
    "PlaybackErrorCallback",
    "PositionUpdateCallback",
    "StateChangeCallback",
    "TrackEndedCallback",
    # Factory
    "BackendFactory",
    "BackendNotFoundError",
    "BackendRegistry",
    # DLNA backend
    "DLNABackend",
    "DLNAClient",
    "DLNAClientError",
    "DLNADeviceInfo",
    # Audio proxy
    "AudioProxyServer",
    "RegisteredTrack",
    "ResolvedTrack",
    # Local backend
    "LocalAudioBackend",
]

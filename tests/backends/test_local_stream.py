"""Tests for AudioOutputStream pause/start semantics."""

import numpy as np

from qobuz_proxy.backends.local.ring_buffer import RingBuffer
from qobuz_proxy.backends.local.stream import AudioOutputStream


def _stream_with_audio() -> AudioOutputStream:
    buf = RingBuffer(44100, 2)
    buf.write(np.random.rand(8192, 2).astype(np.float32) + 0.1)
    # No sounddevice stream is opened; _audio_callback is driven directly.
    return AudioOutputStream(device_index=0, ring_buffer=buf)


class TestPausedStateReset:
    def test_start_clears_lingering_pause(self) -> None:
        """Regression for BUG-04: starting a new track while paused must play
        audio, not leave the callback emitting silence forever (pause →
        select different track → frozen silent 'playing' state)."""
        stream = _stream_with_audio()
        stream.pause()

        stream.start()  # what LocalAudioBackend.play() calls for the new track

        out = np.zeros((1024, 2), dtype=np.float32)
        stream._audio_callback(out, 1024, None, None)
        assert np.abs(out).sum() > 0  # audio flowed

    def test_paused_callback_outputs_silence(self) -> None:
        stream = _stream_with_audio()
        stream.pause()

        out = np.ones((1024, 2), dtype=np.float32)
        stream._audio_callback(out, 1024, None, None)
        assert np.abs(out).sum() == 0

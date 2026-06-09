from __future__ import annotations

import os
import socket
import threading
import time

_CUSTOM_EPOCH_MS = 1704067200000  # 2024-01-01 00:00:00 UTC
_SEQUENCE_BITS = 12
_WORKER_BITS = 10
_MAX_SEQUENCE = (1 << _SEQUENCE_BITS) - 1
_MAX_WORKER_ID = (1 << _WORKER_BITS) - 1
_WORKER_ID = (hash(socket.gethostname()) ^ os.getpid()) & _MAX_WORKER_ID

_lock = threading.Lock()
_last_timestamp = -1
_sequence = 0


def _current_millis() -> int:
    return time.time_ns() // 1_000_000


def generate_snowflake_id() -> int:
    """Generate a positive 64-bit integer ID in-process."""
    global _last_timestamp, _sequence

    with _lock:
        timestamp = _current_millis()
        if timestamp < _last_timestamp:
            timestamp = _last_timestamp

        if timestamp == _last_timestamp:
            _sequence = (_sequence + 1) & _MAX_SEQUENCE
            if _sequence == 0:
                while timestamp <= _last_timestamp:
                    timestamp = _current_millis()
        else:
            _sequence = 0

        _last_timestamp = timestamp

        return (
            ((timestamp - _CUSTOM_EPOCH_MS) << (_WORKER_BITS + _SEQUENCE_BITS))
            | (_WORKER_ID << _SEQUENCE_BITS)
            | _sequence
        )

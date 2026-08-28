from __future__ import annotations

import threading
import time
from typing import Any


class ProcessorMetrics:
    """Thread-safe, lightweight operational counters for the prototype."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._window_started_at = time.monotonic()
        self._messages_in_window = 0
        self._last_messages_per_second = 0.0
        self._last_processing_latency_ms = 0.0
        self._processed_messages = 0
        self._malformed_events_dropped = 0
        self._broadcast_failures = 0
        self._consumer_lag: int | None = None

    def record_processed(self, processing_latency_ms: float) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._window_started_at
            if elapsed >= 1:
                self._last_messages_per_second = self._messages_in_window / elapsed
                self._messages_in_window = 0
                self._window_started_at = now
            self._messages_in_window += 1
            self._processed_messages += 1
            self._last_processing_latency_ms = processing_latency_ms

    def record_malformed_event(self) -> None:
        with self._lock:
            self._malformed_events_dropped += 1

    def record_broadcast_failure(self) -> None:
        with self._lock:
            self._broadcast_failures += 1

    def set_consumer_lag(self, lag: int | None) -> None:
        with self._lock:
            self._consumer_lag = lag

    def snapshot(self, websocket_clients: int) -> dict[str, Any]:
        with self._lock:
            elapsed = time.monotonic() - self._window_started_at
            current_rate = self._messages_in_window / elapsed if elapsed > 0 else 0.0
            return {
                "messages_per_second": round(current_rate or self._last_messages_per_second, 2),
                "kafka_consumer_lag": self._consumer_lag,
                "last_processing_latency_ms": round(self._last_processing_latency_ms, 3),
                "processed_messages": self._processed_messages,
                "websocket_clients": websocket_clients,
                "malformed_events_dropped": self._malformed_events_dropped,
                "websocket_broadcast_failures": self._broadcast_failures,
                "events_throttled": 0,
            }

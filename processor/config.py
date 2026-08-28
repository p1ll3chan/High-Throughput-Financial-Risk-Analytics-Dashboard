from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    kafka_auto_offset_reset: str
    kafka_poll_timeout_seconds: float
    sliding_window_size: int
    analytics_log_interval_seconds: float

    @classmethod
    def from_environment(cls) -> "Settings":
        settings = cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "market-ticks"),
            kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "risk-analytics-processor"),
            kafka_auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
            kafka_poll_timeout_seconds=float(os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", "1")),
            sliding_window_size=int(os.getenv("SLIDING_WINDOW_SIZE", "100")),
            analytics_log_interval_seconds=float(os.getenv("ANALYTICS_LOG_INTERVAL_SECONDS", "30")),
        )
        if settings.sliding_window_size <= 0:
            raise ValueError("SLIDING_WINDOW_SIZE must be greater than zero")
        if settings.kafka_poll_timeout_seconds <= 0:
            raise ValueError("KAFKA_POLL_TIMEOUT_SECONDS must be greater than zero")
        if settings.analytics_log_interval_seconds < 0:
            raise ValueError("ANALYTICS_LOG_INTERVAL_SECONDS must be zero or greater")
        return settings

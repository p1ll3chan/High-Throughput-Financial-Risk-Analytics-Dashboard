from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from processor.analytics import RiskAnalyticsEngine
from processor.config import Settings
from processor.logging_config import LOGGER
from processor.metrics import ProcessorMetrics
from processor.models import parse_market_tick


BroadcastCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class KafkaAnalyticsConsumer:
    """Owns the blocking Kafka consumer in one thread and forwards updates to asyncio."""

    def __init__(
        self,
        settings: Settings,
        analytics_engine: RiskAnalyticsEngine,
        broadcast: BroadcastCallback,
        metrics: ProcessorMetrics,
    ) -> None:
        self._settings = settings
        self._analytics_engine = analytics_engine
        self._broadcast = broadcast
        self._metrics = metrics
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._last_analytics_log_at = 0.0
        self._last_lag_measurement_at = 0.0

    def start(self, event_loop: asyncio.AbstractEventLoop) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._event_loop = event_loop
        self._thread = threading.Thread(target=self._consume, name="kafka-analytics-consumer", daemon=True)
        self._thread.start()
        LOGGER.info(
            "Kafka analytics consumer started",
            extra={"details": {"topic": self._settings.kafka_topic}},
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, self._settings.kafka_poll_timeout_seconds + 5)
            if self._thread.is_alive():
                LOGGER.warning("Kafka analytics consumer did not stop before timeout")
            else:
                LOGGER.info("Kafka analytics consumer stopped")

    def _consumer_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "group.id": self._settings.kafka_consumer_group,
            "auto.offset.reset": self._settings.kafka_auto_offset_reset,
            "enable.auto.commit": True,
            "error_cb": self._on_kafka_error,
        }

    def _on_kafka_error(self, error: KafkaError) -> None:
        LOGGER.error(
            "Kafka client error",
            extra={
                "details": {
                    "error": str(error),
                    "retriable": error.retriable(),
                    "fatal": error.fatal(),
                }
            },
        )

    def _consume(self) -> None:
        reconnect_delay_seconds = 1.0
        while not self._stop_event.is_set():
            consumer: Consumer | None = None
            try:
                consumer = Consumer(self._consumer_config())
                consumer.subscribe([self._settings.kafka_topic])
                LOGGER.info(
                    "Kafka analytics consumer subscribed",
                    extra={"details": {"topic": self._settings.kafka_topic}},
                )
                reconnect_delay_seconds = 1.0

                while not self._stop_event.is_set():
                    message = consumer.poll(self._settings.kafka_poll_timeout_seconds)
                    if message is None:
                        continue
                    if message.error() is not None:
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            LOGGER.error(
                                "Kafka consume error",
                                extra={"details": {"error": str(message.error())}},
                            )
                        continue

                    try:
                        tick = parse_market_tick(message.value())
                    except (KeyError, TypeError, UnicodeDecodeError, ValueError) as error:
                        self._metrics.record_malformed_event()
                        LOGGER.warning(
                            "Ignoring malformed Kafka market tick",
                            extra={"details": {"error": str(error)}},
                        )
                        continue

                    started_at = time.perf_counter()
                    analytics = self._analytics_engine.process_tick(tick)
                    self._metrics.record_processed((time.perf_counter() - started_at) * 1000)
                    self._schedule_broadcast(analytics)
                    self._log_analytics_sample(analytics)
                    self._measure_lag_if_due(consumer)
            except KafkaException as error:
                LOGGER.exception(
                    "Kafka analytics consumer error; reconnecting",
                    extra={"details": {"error": str(error), "retry_in_seconds": reconnect_delay_seconds}},
                )
            except Exception:
                LOGGER.exception(
                    "Kafka analytics consumer error; reconnecting",
                    extra={"details": {"retry_in_seconds": reconnect_delay_seconds}},
                )
            finally:
                if consumer is not None:
                    consumer.close()

            if not self._stop_event.is_set():
                self._stop_event.wait(reconnect_delay_seconds)
                reconnect_delay_seconds = min(reconnect_delay_seconds * 2, 30.0)

    def _schedule_broadcast(self, analytics: dict[str, Any]) -> None:
        if self._event_loop is None or self._event_loop.is_closed():
            return
        try:
            future = asyncio.run_coroutine_threadsafe(self._broadcast(analytics), self._event_loop)
        except RuntimeError:
            return
        future.add_done_callback(self._log_broadcast_result)

    def _log_broadcast_result(self, future: Future[Any]) -> None:
        try:
            future.result()
        except Exception as error:
            self._metrics.record_broadcast_failure()
            LOGGER.error(
                "Analytics WebSocket broadcast failed",
                extra={"details": {"error": str(error)}},
            )

    def _log_analytics_sample(self, analytics: dict[str, Any]) -> None:
        now = time.monotonic()
        if now - self._last_analytics_log_at >= self._settings.analytics_log_interval_seconds:
            LOGGER.info("Analytics update sample", extra={"details": analytics})
            self._last_analytics_log_at = now

    def _measure_lag_if_due(self, consumer: Consumer) -> None:
        now = time.monotonic()
        if now - self._last_lag_measurement_at < 5:
            return
        self._last_lag_measurement_at = now
        try:
            assignments = consumer.assignment()
            positions = consumer.position(assignments)
            lag = 0
            for partition in positions:
                _, high_offset = consumer.get_watermark_offsets(partition, timeout=1.0)
                if partition.offset >= 0:
                    lag += max(high_offset - partition.offset, 0)
            self._metrics.set_consumer_lag(lag)
        except KafkaException as error:
            LOGGER.warning("Unable to measure Kafka consumer lag", extra={"details": {"error": str(error)}})

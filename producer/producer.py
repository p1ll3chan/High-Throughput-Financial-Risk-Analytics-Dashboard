#!/usr/bin/env python3
"""Ingest Binance BTCUSDT trades and publish normalized ticks to Kafka."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException


DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_WEBSOCKET_BASE_URL = "wss://stream.binance.com:9443/ws"


class JsonFormatter(logging.Formatter):
    """Small structured formatter that keeps service logs machine-readable."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": int(record.created * 1000),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        details = getattr(record, "details", None)
        if details is not None:
            payload["details"] = details
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), handlers=[handler])
    return logging.getLogger("market-data-producer")


LOGGER = configure_logging()


@dataclass(frozen=True)
class Settings:
    symbols: tuple[str, ...]
    websocket_base_url: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_client_id: str
    kafka_linger_ms: int
    reconnect_initial_delay_seconds: float
    reconnect_max_delay_seconds: float
    sample_log_interval_seconds: float
    shutdown_flush_timeout_seconds: float

    @classmethod
    def from_environment(cls) -> "Settings":
        settings = cls(
            symbols=tuple(symbol.strip().upper() for symbol in os.getenv("MARKET_SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",") if symbol.strip()),
            websocket_base_url=os.getenv("MARKET_DATA_WEBSOCKET_BASE_URL", DEFAULT_WEBSOCKET_BASE_URL).rstrip("/"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "market-ticks"),
            kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "market-data-producer"),
            kafka_linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),
            reconnect_initial_delay_seconds=float(os.getenv("RECONNECT_INITIAL_DELAY_SECONDS", "1")),
            reconnect_max_delay_seconds=float(os.getenv("RECONNECT_MAX_DELAY_SECONDS", "30")),
            sample_log_interval_seconds=float(os.getenv("SAMPLE_LOG_INTERVAL_SECONDS", "30")),
            shutdown_flush_timeout_seconds=float(os.getenv("SHUTDOWN_FLUSH_TIMEOUT_SECONDS", "10")),
        )
        if not settings.symbols or len(set(settings.symbols)) != len(settings.symbols):
            raise ValueError("MARKET_SYMBOLS must contain one or more unique symbols")
        if settings.kafka_linger_ms < 0:
            raise ValueError("KAFKA_LINGER_MS must be zero or greater")
        if settings.reconnect_initial_delay_seconds <= 0:
            raise ValueError("RECONNECT_INITIAL_DELAY_SECONDS must be greater than zero")
        if settings.reconnect_max_delay_seconds < settings.reconnect_initial_delay_seconds:
            raise ValueError(
                "RECONNECT_MAX_DELAY_SECONDS must be at least RECONNECT_INITIAL_DELAY_SECONDS"
            )
        if settings.shutdown_flush_timeout_seconds <= 0:
            raise ValueError("SHUTDOWN_FLUSH_TIMEOUT_SECONDS must be greater than zero")
        return settings

    def websocket_url_for(self, symbol: str) -> str:
        return f"{self.websocket_base_url}/{symbol.lower()}@trade"


def normalize_trade_event(message: str | bytes, expected_symbol: str | None = None) -> dict[str, object]:
    """Map a Binance trade event to the service's stable internal schema."""

    if isinstance(message, bytes):
        message = message.decode("utf-8")

    event = json.loads(message)
    symbol = str(event["s"]).upper()
    if expected_symbol is not None and symbol != expected_symbol:
        raise ValueError(f"Unexpected symbol received: {symbol}")

    price = float(event["p"])
    quantity = float(event["q"])
    timestamp = int(event["T"])
    if not math.isfinite(price) or price <= 0:
        raise ValueError("Trade price must be a positive finite number")
    if not math.isfinite(quantity) or quantity <= 0:
        raise ValueError("Trade quantity must be a positive finite number")
    if timestamp <= 0:
        raise ValueError("Trade timestamp must be a positive Unix epoch in milliseconds")

    # Keep this schema exact: no Binance-specific fields are published downstream.
    return {
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp,
    }


class KafkaPublisher:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": settings.kafka_client_id,
                "linger.ms": settings.kafka_linger_ms,
            }
        )
        self._last_sample_log_at = 0.0

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            LOGGER.error(
                "Kafka delivery failed",
                extra={
                    "details": {
                        "error": str(error),
                        "topic": message.topic() if message is not None else self._settings.kafka_topic,
                    }
                },
            )

    async def publish(self, tick: dict[str, object]) -> None:
        payload = json.dumps(tick, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                self._producer.produce(
                    self._settings.kafka_topic,
                    key=str(tick["symbol"]).encode("utf-8"),
                    value=payload,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                LOGGER.warning(
                    "Kafka producer queue is full; waiting to retry",
                    extra={"details": {"topic": self._settings.kafka_topic}},
                )
                self._producer.poll(0)
                await asyncio.sleep(0.1)

        now = time.monotonic()
        if now - self._last_sample_log_at >= self._settings.sample_log_interval_seconds:
            LOGGER.info("Normalized market tick", extra={"details": tick})
            self._last_sample_log_at = now

    async def poll_delivery_reports(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self._producer.poll(0)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.1)
            except TimeoutError:
                pass

    async def flush(self) -> None:
        remaining = await asyncio.to_thread(
            self._producer.flush, self._settings.shutdown_flush_timeout_seconds
        )
        if remaining:
            LOGGER.error(
                "Kafka flush timed out",
                extra={"details": {"undelivered_messages": remaining}},
            )
        else:
            LOGGER.info("Kafka producer flushed pending messages")


async def wait_for_stop_or_timeout(stop_event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass


async def ingest_market_data(
    settings: Settings, publisher: KafkaPublisher, symbol: str, stop_event: asyncio.Event
) -> None:
    reconnect_delay = settings.reconnect_initial_delay_seconds

    while not stop_event.is_set():
        try:
            LOGGER.info(
                "Connecting to market data WebSocket",
                extra={"details": {"url": settings.websocket_url_for(symbol), "symbol": symbol}},
            )
            async with connect(
                settings.websocket_url_for(symbol),
                open_timeout=20,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as websocket:
                LOGGER.info("Connected to market data WebSocket", extra={"details": {"symbol": symbol}})
                reconnect_delay = settings.reconnect_initial_delay_seconds

                while not stop_event.is_set():
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    except TimeoutError:
                        continue

                    try:
                        tick = normalize_trade_event(raw_message, expected_symbol=symbol)
                    except (KeyError, TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
                        LOGGER.warning(
                            "Ignoring malformed market data event",
                            extra={"details": {"error": str(error)}},
                        )
                        continue
                    await publisher.publish(tick)

        except asyncio.CancelledError:
            raise
        except (OSError, TimeoutError, WebSocketException) as error:
            LOGGER.warning(
                "Market data WebSocket disconnected",
                extra={"details": {"error": str(error), "retry_in_seconds": reconnect_delay}},
            )
        except Exception:
            LOGGER.exception(
                "Unexpected market data ingestion error",
                extra={"details": {"retry_in_seconds": reconnect_delay}},
            )

        if not stop_event.is_set():
            await wait_for_stop_or_timeout(stop_event, reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, settings.reconnect_max_delay_seconds)


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        LOGGER.info("Shutdown requested")
        stop_event.set()

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, request_shutdown)
        except NotImplementedError:
            signal.signal(signal_name, lambda *_: request_shutdown())


async def run() -> None:
    settings = Settings.from_environment()
    LOGGER.info(
        "Starting market data producer",
        extra={
            "details": {
                "symbols": settings.symbols,
                "kafka_bootstrap_servers": settings.kafka_bootstrap_servers,
                "kafka_topic": settings.kafka_topic,
            }
        },
    )
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    publisher = KafkaPublisher(settings)
    poll_task = asyncio.create_task(publisher.poll_delivery_reports(stop_event))

    try:
        await asyncio.gather(
            *(ingest_market_data(settings, publisher, symbol, stop_event) for symbol in settings.symbols)
        )
    finally:
        stop_event.set()
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        await publisher.flush()
        LOGGER.info("Market data producer stopped")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        LOGGER.info("Market data producer interrupted")
    except Exception:
        LOGGER.exception("Market data producer failed to start")
        raise


if __name__ == "__main__":
    main()

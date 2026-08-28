from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from processor.analytics import RiskAnalyticsEngine
from processor.config import Settings
from processor.kafka_consumer import KafkaAnalyticsConsumer
from processor.logging_config import LOGGER
from processor.metrics import ProcessorMetrics
from processor.positions import positions_from_environment
from processor.websocket_manager import AnalyticsWebSocketManager


def create_app() -> FastAPI:
    settings = Settings.from_environment()
    positions = positions_from_environment()
    analytics_engine = RiskAnalyticsEngine(settings.sliding_window_size, positions)
    websocket_manager = AnalyticsWebSocketManager()
    metrics = ProcessorMetrics()
    kafka_consumer = KafkaAnalyticsConsumer(settings, analytics_engine, websocket_manager.broadcast, metrics)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        kafka_consumer.start(asyncio.get_running_loop())
        try:
            yield
        finally:
            await kafka_consumer.stop()

    app = FastAPI(title="Risk Analytics Processor", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def processor_metrics() -> dict[str, object]:
        return metrics.snapshot(websocket_manager.connection_count)

    @app.websocket("/ws/analytics")
    async def analytics_websocket(websocket: WebSocket) -> None:
        await websocket_manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            websocket_manager.disconnect(websocket)

    LOGGER.info(
        "Risk analytics application configured",
        extra={
            "details": {
                "kafka_topic": settings.kafka_topic,
                "sliding_window_size": settings.sliding_window_size,
                "configured_positions": len(positions),
            }
        },
    )
    return app


app = create_app()

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import WebSocket

from processor.logging_config import LOGGER


class AnalyticsWebSocketManager:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.add(websocket)
        LOGGER.info("Analytics WebSocket client connected", extra={"details": self._connection_details()})

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)
        LOGGER.info("Analytics WebSocket client disconnected", extra={"details": self._connection_details()})

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    async def broadcast(self, analytics: dict[str, Any]) -> None:
        connections = tuple(self._connections)
        if not connections:
            return

        results = await asyncio.gather(
            *(websocket.send_json(analytics) for websocket in connections), return_exceptions=True
        )
        for websocket, result in zip(connections, results, strict=True):
            if isinstance(result, Exception):
                LOGGER.warning(
                    "Removing unavailable Analytics WebSocket client",
                    extra={"details": {"error": str(result)}},
                )
                self._connections.discard(websocket)

    def _connection_details(self) -> dict[str, int]:
        return {"connected_clients": len(self._connections)}

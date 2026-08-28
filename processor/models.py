from __future__ import annotations

import json
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class MarketTick:
    symbol: str
    price: float
    quantity: float
    timestamp: int


def parse_market_tick(payload: bytes | str) -> MarketTick:
    """Validate the Phase 2 market-tick contract before analytics consumes it."""

    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    event = json.loads(payload)

    expected_fields = {"symbol", "price", "quantity", "timestamp"}
    if set(event) != expected_fields:
        raise ValueError("Market tick does not match the required schema")

    symbol = str(event["symbol"]).upper()
    if not symbol:
        raise ValueError("Market tick symbol must not be empty")

    price = float(event["price"])
    quantity = float(event["quantity"])
    timestamp = int(event["timestamp"])
    if not math.isfinite(price) or price <= 0:
        raise ValueError("Market tick price must be a positive finite number")
    if not math.isfinite(quantity) or quantity <= 0:
        raise ValueError("Market tick quantity must be a positive finite number")
    if timestamp <= 0:
        raise ValueError("Market tick timestamp must be a positive Unix epoch in milliseconds")

    return MarketTick(symbol=symbol, price=price, quantity=quantity, timestamp=timestamp)

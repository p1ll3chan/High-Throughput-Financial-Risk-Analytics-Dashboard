from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Position:
    symbol: str
    quantity: float
    average_entry_price: float


def positions_from_environment() -> dict[str, Position]:
    """Load optional local portfolio positions from PORTFOLIO_POSITIONS_JSON."""

    raw_positions = os.getenv("PORTFOLIO_POSITIONS_JSON", "[]")
    try:
        items = json.loads(raw_positions)
    except json.JSONDecodeError as error:
        raise ValueError("PORTFOLIO_POSITIONS_JSON must be valid JSON") from error
    if not isinstance(items, list):
        raise ValueError("PORTFOLIO_POSITIONS_JSON must be a JSON array")

    positions: dict[str, Position] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Each portfolio position must be an object")
        try:
            symbol = str(item["symbol"]).upper()
            quantity = float(item["quantity"])
            average_entry_price = float(item["average_entry_price"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("Each position needs symbol, quantity, and average_entry_price") from error
        if not symbol or not math.isfinite(quantity) or quantity == 0:
            raise ValueError("Position symbol must be set and quantity must be a non-zero finite number")
        if not math.isfinite(average_entry_price) or average_entry_price <= 0:
            raise ValueError("Position average_entry_price must be a positive finite number")
        positions[symbol] = Position(symbol, quantity, average_entry_price)
    return positions

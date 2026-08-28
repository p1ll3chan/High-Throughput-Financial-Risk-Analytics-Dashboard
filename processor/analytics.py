from __future__ import annotations

from collections import deque
from typing import Any

import numpy as np

from processor.models import MarketTick
from processor.positions import Position


SMA_WINDOW = 30
VAR_95_Z_SCORE = 1.645


class RiskAnalyticsEngine:
    """Calculates per-symbol analytics over independent bounded tick windows."""

    def __init__(self, window_size: int = 100, positions: dict[str, Position] | None = None) -> None:
        if window_size <= 0:
            raise ValueError("window_size must be greater than zero")
        self._window_size = window_size
        self._windows: dict[str, deque[MarketTick]] = {}
        self._positions = positions or {}
        self._latest_prices: dict[str, float] = {}
        self._latest_volatility: dict[str, float | None] = {}

    def process_tick(self, tick: MarketTick) -> dict[str, Any]:
        window = self._windows.setdefault(tick.symbol, deque(maxlen=self._window_size))
        window.append(tick)
        self._latest_prices[tick.symbol] = tick.price
        prices = np.fromiter((item.price for item in window), dtype=float)

        volatility = self._volatility(prices)
        sma_30 = float(np.mean(prices[-SMA_WINDOW:])) if len(prices) >= SMA_WINDOW else None

        # This is a one-period percentage-loss metric, not currency VaR.
        # Position value and horizon scaling can be applied by a future portfolio layer.
        var_95 = VAR_95_Z_SCORE * volatility if volatility is not None else None
        self._latest_volatility[tick.symbol] = volatility
        position = self._position_analytics(tick.symbol)

        return {
            "symbol": tick.symbol,
            "timestamp": tick.timestamp,
            "current_price": tick.price,
            "sma_30": sma_30,
            "volatility": volatility,
            "var_95": var_95,
            "position": position,
            "portfolio_var_95": self._portfolio_var_95(),
            "observations": len(window),
        }

    def window_length(self, symbol: str) -> int:
        return len(self._windows.get(symbol, ()))

    @staticmethod
    def _volatility(prices: np.ndarray) -> float | None:
        if len(prices) < 3:
            return None
        percentage_returns = np.diff(prices) / prices[:-1]
        return float(np.std(percentage_returns, ddof=1))

    def _position_analytics(self, symbol: str) -> dict[str, float] | None:
        position = self._positions.get(symbol)
        if position is None:
            return None
        current_price = self._latest_prices[symbol]
        position_value = position.quantity * current_price
        exposure = abs(position_value)
        volatility = self._latest_volatility.get(symbol)
        return {
            "quantity": position.quantity,
            "average_entry_price": position.average_entry_price,
            "position_value": position_value,
            "unrealized_pnl": position.quantity * (current_price - position.average_entry_price),
            "position_var_95": exposure * VAR_95_Z_SCORE * volatility if volatility is not None else None,
        }

    def _portfolio_var_95(self) -> float | None:
        position_vars = []
        for symbol, position in self._positions.items():
            price = self._latest_prices.get(symbol)
            volatility = self._latest_volatility.get(symbol)
            if price is not None and volatility is not None:
                position_vars.append(abs(position.quantity * price) * VAR_95_Z_SCORE * volatility)
        return float(sum(position_vars)) if position_vars else None

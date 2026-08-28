import math
import unittest

from processor.analytics import RiskAnalyticsEngine, VAR_95_Z_SCORE
from processor.models import MarketTick


def tick(symbol: str, price: float, timestamp: int) -> MarketTick:
    return MarketTick(symbol=symbol, price=price, quantity=1.0, timestamp=timestamp)


class RiskAnalyticsEngineTests(unittest.TestCase):
    def test_sma_30_uses_the_latest_thirty_prices(self) -> None:
        engine = RiskAnalyticsEngine(window_size=100)
        for price in range(1, 31):
            analytics = engine.process_tick(tick("BTCUSDT", float(price), price))

        self.assertEqual(analytics["sma_30"], 15.5)
        self.assertEqual(analytics["current_price"], 30.0)
        self.assertEqual(analytics["observations"], 30)

    def test_volatility_uses_percentage_returns_and_var_is_normalized(self) -> None:
        engine = RiskAnalyticsEngine(window_size=100)
        engine.process_tick(tick("BTCUSDT", 100.0, 1))
        engine.process_tick(tick("BTCUSDT", 110.0, 2))
        analytics = engine.process_tick(tick("BTCUSDT", 99.0, 3))

        expected_volatility = math.sqrt(0.02)
        self.assertAlmostEqual(analytics["volatility"], expected_volatility)
        self.assertAlmostEqual(analytics["var_95"], VAR_95_Z_SCORE * expected_volatility)

    def test_windows_are_bounded_and_independent_per_symbol(self) -> None:
        engine = RiskAnalyticsEngine(window_size=3)
        for timestamp, price in enumerate((1.0, 2.0, 3.0, 4.0), start=1):
            analytics = engine.process_tick(tick("BTCUSDT", price, timestamp))
        eth_analytics = engine.process_tick(tick("ETHUSDT", 100.0, 5))

        self.assertEqual(engine.window_length("BTCUSDT"), 3)
        self.assertEqual(engine.window_length("ETHUSDT"), 1)
        self.assertEqual(analytics["current_price"], 4.0)
        self.assertEqual(eth_analytics["current_price"], 100.0)
        self.assertIsNone(eth_analytics["volatility"])


if __name__ == "__main__":
    unittest.main()

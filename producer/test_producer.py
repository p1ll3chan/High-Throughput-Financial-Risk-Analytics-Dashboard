import json
import unittest

from producer.producer import normalize_trade_event


class NormalizeTradeEventTests(unittest.TestCase):
    def test_normalizes_binance_trade_to_the_internal_schema(self) -> None:
        trade = json.dumps({"s": "BTCUSDT", "p": "104500.25", "q": "0.0012", "T": 1_725_000_000_123})

        tick = normalize_trade_event(trade)

        self.assertEqual(
            tick,
            {
                "symbol": "BTCUSDT",
                "price": 104500.25,
                "quantity": 0.0012,
                "timestamp": 1_725_000_000_123,
            },
        )
        self.assertEqual(set(tick), {"symbol", "price", "quantity", "timestamp"})

    def test_rejects_an_event_for_an_unexpected_symbol(self) -> None:
        trade = json.dumps({"s": "ETHUSDT", "p": "1", "q": "1", "T": 1})

        with self.assertRaises(ValueError):
            normalize_trade_event(trade, expected_symbol="BTCUSDT")


if __name__ == "__main__":
    unittest.main()

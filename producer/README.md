# Market Data Producer

This independently runnable service consumes Binance's public BTCUSDT, ETHUSDT,
and SOLUSDT trade streams, normalizes every trade, and publishes it to Kafka's
`market-ticks` topic.

## Prerequisites

Start the Phase 1 Kafka infrastructure from the repository root:

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

## Install and run

From the repository root, create and activate a Python 3.11+ virtual
environment, then install the producer dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r producer/requirements.txt
python producer/producer.py
```

Press `Ctrl+C` to stop the service. It will stop reading the WebSocket and
flush pending Kafka messages before exiting.

The service emits JSON logs. A `Normalized market tick` sample is logged at
startup and then at most every 30 seconds; individual trade events are not
logged.

## Verify messages are arriving

Keep the producer running, then start this consumer in another terminal:

```bash
docker compose -f infrastructure/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic market-ticks \
  --group "market-ticks-verify-$(date +%s)" \
  --property print.key=true \
  --property key.separator=:
```

New records should appear in this format (the key precedes the colon):

```text
BTCUSDT:{"symbol":"BTCUSDT","price":104500.25,"quantity":0.0012,"timestamp":1725000000123}
```

Press `Ctrl+C` to stop the verifier.

## Configuration

All values are optional for local development.

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap endpoint. |
| `KAFKA_TOPIC` | `market-ticks` | Destination topic. |
| `KAFKA_CLIENT_ID` | `market-data-producer` | Kafka client identifier. |
| `KAFKA_LINGER_MS` | `10` | Small batching delay for Kafka sends. |
| `MARKET_SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | Comma-separated Binance symbols to ingest. |
| `MARKET_DATA_WEBSOCKET_BASE_URL` | Binance WebSocket base URL | Source endpoint base; a symbol trade stream is appended. |
| `RECONNECT_INITIAL_DELAY_SECONDS` | `1` | First reconnect delay after a disconnect. |
| `RECONNECT_MAX_DELAY_SECONDS` | `30` | Maximum exponential-backoff delay. |
| `SAMPLE_LOG_INTERVAL_SECONDS` | `30` | Minimum interval between normalized-tick sample logs. |
| `SHUTDOWN_FLUSH_TIMEOUT_SECONDS` | `10` | Maximum graceful Kafka flush time. |
| `LOG_LEVEL` | `INFO` | Python logging level. |

Each stream must emit Binance-compatible events for its configured symbol. Kafka
records use the symbol as their key, so Kafka can consistently assign each
symbol to a partition while preserving per-symbol ordering.

## Unit test

After installing the requirements:

```bash
python -m unittest producer/test_producer.py
```

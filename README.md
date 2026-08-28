# Real-Time Financial Portfolio & Risk Analytics Dashboard

A local-development prototype that streams live BTCUSDT, ETHUSDT, and SOLUSDT trades from Binance,
normalizes them into Kafka, calculates risk analytics, and presents them in a
React dashboard.

## Current setup

```text
Binance public BTCUSDT trade WebSocket
        │
        ▼
Python market-data producer
        │  normalized market tick JSON
        ▼
Kafka topic: market-ticks
        │
        ▼
Python risk analytics processor
        │  FastAPI WebSocket: /ws/analytics
        ▼
React + Recharts dashboard
```

The project currently contains these completed prototype components:

| Phase | Component | Location | Default endpoint |
| --- | --- | --- | --- |
| 1 | Single-node Kafka (KRaft) | `infrastructure/` | `localhost:9092` |
| 2 | Binance market data producer | `producer/` | Binance BTCUSDT trade stream |
| 3 | Risk analytics + WebSocket API | `processor/` | `localhost:8000/ws/analytics` |
| 4 | Real-time dashboard | `frontend/` | `localhost:5173` |

## Prerequisites

- Docker Engine and Docker Compose plugin
- Python 3.11+
- Node.js 20.19+ or 22.12+
- Internet access to Binance's public WebSocket endpoint

## Initial installation

Run these once from the repository root:

```bash
# Producer environment
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r producer/requirements.txt
deactivate

# Processor environment
python -m venv .venv-processor
source .venv-processor/bin/activate
python -m pip install --upgrade pip
python -m pip install -r processor/requirements.txt
deactivate

# Frontend dependencies
cd frontend
npm install
cd ..
```

## Run the complete project

Each component runs independently. Keep each command running in its own
terminal.

### Terminal 1 — Kafka

```bash
docker compose -f infrastructure/docker-compose.yml up -d
docker compose -f infrastructure/docker-compose.yml ps
```

Kafka is ready when the `kafka` service reports `healthy`.

### Terminal 2 — Risk analytics processor

```bash
source .venv-processor/bin/activate
uvicorn processor.app:app --host 0.0.0.0 --port 8000
```

Verify the processor in another shell if needed:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"status":"ok"}
```

### Terminal 3 — Market data producer

```bash
source .venv/bin/activate
python producer/producer.py
```

Expected logs include `Connected to market data WebSocket` and periodic
`Normalized market tick` sample entries.

### Terminal 4 — React dashboard

```bash
cd frontend
npm run dev
```

Open the Vite URL shown in the terminal, normally
`http://localhost:5173`.

## Verify the data flow

When all services are running, this command should print normalized market
events from Kafka:

```bash
docker compose -f infrastructure/docker-compose.yml exec kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic market-ticks \
  --group "market-ticks-debug-$(date +%s)"
```

The producer publishes exactly this schema:

```json
{
  "symbol": "BTCUSDT",
  "price": 104500.25,
  "quantity": 0.0012,
  "timestamp": 1725000000123
}
```

The processor broadcasts analytics like:

```json
{
  "symbol": "BTCUSDT",
  "timestamp": 1725000000123,
  "current_price": 104500.25,
  "sma_30": 104490.12,
  "volatility": 0.0021,
  "var_95": 0.0034545,
  "observations": 30
}
```

`sma_30` is `null` until 30 prices are available. Volatility and VaR are
`null` until three prices are available.

## Analytics assumptions

- Each symbol has an independent `collections.deque` sliding window.
- The default maximum window length is 100 ticks.
- SMA-30 uses the latest 30 price observations.
- Volatility is the sample standard deviation of percentage returns, not raw
  price values.
- VaR 95% is a normalized one-period metric: `1.645 × volatility`.
- VaR is not yet a currency portfolio VaR. A future portfolio layer should
  apply position value and an appropriate time horizon.

Optional positions now add `position_value`, `unrealized_pnl`,
`position_var_95`, and a conservative summed `portfolio_var_95` to analytics
updates. Configure positions with `PORTFOLIO_POSITIONS_JSON`:

```bash
export PORTFOLIO_POSITIONS_JSON='[
  {"symbol":"BTCUSDT","quantity":0.5,"average_entry_price":90000},
  {"symbol":"ETHUSDT","quantity":3,"average_entry_price":3000}
]'
```

## Configuration and modifications

All local defaults work without environment variables. Override settings in
the terminal before starting the corresponding component.

### Kafka

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_MARKET_TICKS_PARTITIONS` | `3` | Number of `market-ticks` partitions on its first creation. |

Example:

```bash
KAFKA_MARKET_TICKS_PARTITIONS=6 docker compose -f infrastructure/docker-compose.yml up -d
```

### Producer

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address. |
| `KAFKA_TOPIC` | `market-ticks` | Destination topic. |
| `KAFKA_CLIENT_ID` | `market-data-producer` | Kafka producer client ID. |
| `KAFKA_LINGER_MS` | `10` | Kafka batching delay. |
| `MARKET_DATA_WEBSOCKET_URL` | Binance BTCUSDT stream | Alternate Binance-compatible test source. |
| `RECONNECT_INITIAL_DELAY_SECONDS` | `1` | Initial WebSocket reconnect delay. |
| `RECONNECT_MAX_DELAY_SECONDS` | `30` | Maximum reconnect delay. |
| `SAMPLE_LOG_INTERVAL_SECONDS` | `30` | Minimum interval between sample event logs. |
| `MARKET_SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | Comma-separated symbols to stream. |

### Processor

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address. |
| `KAFKA_TOPIC` | `market-ticks` | Source topic. |
| `KAFKA_CONSUMER_GROUP` | `risk-analytics-processor` | Kafka consumer group. |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Offset policy for a new consumer group. |
| `SLIDING_WINDOW_SIZE` | `100` | Maximum tick observations per symbol. |
| `ANALYTICS_LOG_INTERVAL_SECONDS` | `30` | Minimum interval between analytics logs. |
| `PORTFOLIO_POSITIONS_JSON` | `[]` | JSON array of portfolio positions. |

Example, with a 200-tick analytics window:

```bash
SLIDING_WINDOW_SIZE=200 uvicorn processor.app:app --host 0.0.0.0 --port 8000
```

### Frontend

The frontend automatically connects to `ws://localhost:8000/ws/analytics`.
For a different backend endpoint, create `frontend/.env.local`:

```bash
VITE_ANALYTICS_WS_URL=ws://analytics-host:8000/ws/analytics
```

Use `wss://` when serving the dashboard through HTTPS.

## Dashboard behavior

- It reconnects automatically with exponential backoff from 1 to 30 seconds.
- It clearly reports `Connecting`, `Live`, `Reconnecting`, or connection error.
- Positive price changes are green; negative changes are red.
- Incoming WebSocket events are buffered and React state updates occur at most
  once every 250 ms.
- The Recharts line has animations disabled and only retains 30 chart points.
- WebSocket and timer cleanup happens when the page/component unmounts.
- The dashboard keeps separate bounded histories for each active asset.

## Processor metrics

The internal metrics endpoint exposes current messages/sec, estimated Kafka
consumer lag, processing latency, WebSocket client count, malformed event
drops, broadcast failures, and throttled event count:

```bash
curl http://localhost:8000/metrics
```

## Tests and builds

```bash
# Producer normalization tests
source .venv/bin/activate
python -m unittest producer/test_producer.py

# Processor analytics tests
source .venv-processor/bin/activate
python -m unittest processor/test_analytics.py

# Frontend production build
cd frontend
npm run build
```

## Troubleshooting

### Dashboard says “Reconnecting”

The FastAPI processor is unavailable. Start it and check:

```bash
curl http://localhost:8000/health
```

### Dashboard is “Live” but contains no values

The browser is connected to FastAPI, but no analytics events are arriving.
Check the data path in this order:

1. Kafka reports `healthy`.
2. The producer logs `Connected to market data WebSocket`.
3. The Kafka consumer verification command prints `market-ticks` messages.
4. The processor logs `Analytics update sample`.

If step 3 prints zero messages, the producer is not publishing market data;
inspect the producer terminal for Binance or Kafka connection errors.

## Stop services

Stop the producer, processor, and Vite server with `Ctrl+C` in their terminals.
Stop Kafka with:

```bash
docker compose -f infrastructure/docker-compose.yml down
```

To remove the persisted Kafka data as well:

```bash
docker compose -f infrastructure/docker-compose.yml down -v
```
>>>>>>> 480cc20 (Initial commit: financial risk analytics dashboard)

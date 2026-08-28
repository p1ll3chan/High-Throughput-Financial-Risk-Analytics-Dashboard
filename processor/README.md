# Risk Analytics Processor

This independently runnable service consumes normalized `market-ticks` from
Kafka, calculates per-symbol risk analytics, and broadcasts each update over a
FastAPI WebSocket.

## Run

Start Kafka and keep the Phase 2 producer running in another terminal:

```bash
docker compose -f infrastructure/docker-compose.yml up -d
python producer/producer.py
```

Create a processor-specific environment and start FastAPI from the repository
root:

```bash
python -m venv .venv-processor
source .venv-processor/bin/activate
python -m pip install --upgrade pip
python -m pip install -r processor/requirements.txt
uvicorn processor.app:app --host 0.0.0.0 --port 8000
```

The HTTP health endpoint is available at `http://localhost:8000/health` and
the analytics WebSocket endpoint is `ws://localhost:8000/ws/analytics`.
Operational metrics are available at `http://localhost:8000/metrics`.

## Verify the live WebSocket flow

With the producer and processor running, execute this in a third terminal
after activating `.venv-processor`:

```bash
python - <<'PY'
import asyncio
from websockets.asyncio.client import connect

async def receive_update():
    async with connect("ws://localhost:8000/ws/analytics") as websocket:
        print(await asyncio.wait_for(websocket.recv(), timeout=15))

asyncio.run(receive_update())
PY
```

It should print an analytics object containing `current_price`, `sma_30`,
`volatility`, and `var_95`. `sma_30` is `null` until 30 observations are
available; volatility and VaR are `null` until three prices are available.

Run unit tests with:

```bash
python -m unittest processor/test_analytics.py
```

## Analytics and assumptions

- Each symbol has an independent `collections.deque` with a configurable
  maximum length (`SLIDING_WINDOW_SIZE`, default `100`).
- SMA-30 is the average of the latest 30 prices, available after 30 ticks.
- Volatility is the sample standard deviation (`ddof=1`) of percentage returns
  over the current sliding window, not raw prices.
- `var_95` is the normalized, one-tick parametric loss fraction
  `1.645 × volatility`, assuming normally distributed returns and zero mean.
  It is not currency VaR: a portfolio layer must later multiply by position
  value and apply a suitable time horizon.
- Configure optional positions with `PORTFOLIO_POSITIONS_JSON` to expose
  position value, unrealized P&L, position VaR, and a conservative summed
  `portfolio_var_95` (no correlation diversification is assumed).

Example:

```bash
export PORTFOLIO_POSITIONS_JSON='[
  {"symbol":"BTCUSDT","quantity":0.5,"average_entry_price":90000},
  {"symbol":"ETHUSDT","quantity":3,"average_entry_price":3000}
]'
uvicorn processor.app:app --host 0.0.0.0 --port 8000
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap endpoint. |
| `KAFKA_TOPIC` | `market-ticks` | Source topic. |
| `KAFKA_CONSUMER_GROUP` | `risk-analytics-processor` | Consumer group identifier. |
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Offset policy for a new consumer group. |
| `KAFKA_POLL_TIMEOUT_SECONDS` | `1` | Maximum blocking Kafka poll time. |
| `SLIDING_WINDOW_SIZE` | `100` | Maximum per-symbol tick window length. |
| `ANALYTICS_LOG_INTERVAL_SECONDS` | `30` | Minimum interval between analytics sample logs. |
| `PORTFOLIO_POSITIONS_JSON` | `[]` | JSON array of local portfolio positions. |
| `LOG_LEVEL` | `INFO` | Python logging level. |

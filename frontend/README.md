# Real-Time Risk Dashboard

This Vite + React dashboard connects directly to the processor WebSocket and
shows real-time market and risk analytics with a bounded Recharts price chart.

## Run

Start the Kafka infrastructure, producer, and processor first. Then run the
frontend independently:

```bash
cd frontend
npm install
npm run dev
```

Open the URL printed by Vite (normally `http://localhost:5173`). Build a
production bundle with:

```bash
npm run build
```

Vite currently requires Node.js `20.19+` or `22.12+`.

## WebSocket configuration

The local default is automatically derived as:

```text
ws://localhost:8000/ws/analytics
```

For another backend, create `frontend/.env.local`:

```bash
VITE_ANALYTICS_WS_URL=ws://analytics-host:8000/ws/analytics
```

Use a `wss://` URL when the dashboard itself is served over HTTPS.

## Runtime behavior

- The WebSocket reconnects with exponential backoff from 1 second to 30 seconds.
- Cleanup closes the active socket and cancels pending reconnect/update timers.
- Incoming messages are kept in refs and rendered at most once every 250 ms.
- The chart history is capped at 30 points and resets if the active symbol changes.
- Recharts animations are explicitly disabled for the live price line.

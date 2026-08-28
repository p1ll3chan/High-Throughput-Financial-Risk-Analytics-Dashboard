import { useEffect, useMemo, useRef, useState } from "react";

const MAX_HISTORY_POINTS = 30;
const MAX_TRACKED_SYMBOLS = 10;
const UI_UPDATE_INTERVAL_MS = 250;
const MAX_RECONNECT_DELAY_MS = 30_000;

function defaultAnalyticsWebSocketUrl() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.hostname}:8000/ws/analytics`;
}

function isAnalyticsUpdate(value) {
  return (
    value &&
    typeof value.symbol === "string" &&
    Number.isFinite(value.current_price) &&
    Number.isFinite(value.timestamp)
  );
}

export function useAnalyticsWebSocket() {
  const url = useMemo(
    () => import.meta.env.VITE_ANALYTICS_WS_URL || defaultAnalyticsWebSocketUrl(),
    [],
  );
  const [connectionStatus, setConnectionStatus] = useState("connecting");
  const [dashboard, setDashboard] = useState({
    latestBySymbol: {},
    historyBySymbol: {},
    priceDirections: {},
  });

  const socketRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const updateTimerRef = useRef(null);
  const pendingUpdateRef = useRef(new Map());
  const reconnectAttemptRef = useRef(0);

  useEffect(() => {
    let disposed = false;

    const flushLatestUpdate = () => {
      updateTimerRef.current = null;
      const updates = [...pendingUpdateRef.current.values()];
      pendingUpdateRef.current.clear();
      if (!updates.length || disposed) {
        return;
      }

      setDashboard((previous) => {
        const latestBySymbol = { ...previous.latestBySymbol };
        const historyBySymbol = { ...previous.historyBySymbol };
        const priceDirections = { ...previous.priceDirections };
        for (const update of updates) {
          const prior = latestBySymbol[update.symbol];
          if (!prior && Object.keys(latestBySymbol).length >= MAX_TRACKED_SYMBOLS) continue;
          latestBySymbol[update.symbol] = update;
          historyBySymbol[update.symbol] = [
            ...(historyBySymbol[update.symbol] || []).slice(-(MAX_HISTORY_POINTS - 1)),
            { price: update.current_price, timestamp: update.timestamp },
          ];
          priceDirections[update.symbol] = !prior ? "neutral" : update.current_price > prior.current_price ? "up" : update.current_price < prior.current_price ? "down" : "neutral";
        }
        return { latestBySymbol, historyBySymbol, priceDirections };
      });
    };

    const scheduleReconnect = () => {
      if (disposed || reconnectTimerRef.current !== null) {
        return;
      }

      const delay = Math.min(1_000 * 2 ** reconnectAttemptRef.current, MAX_RECONNECT_DELAY_MS);
      reconnectAttemptRef.current += 1;
      setConnectionStatus("reconnecting");
      reconnectTimerRef.current = window.setTimeout(() => {
        reconnectTimerRef.current = null;
        connect();
      }, delay);
    };

    const connect = () => {
      if (disposed) {
        return;
      }

      setConnectionStatus("connecting");
      const socket = new WebSocket(url);
      socketRef.current = socket;

      socket.onopen = () => {
        if (!disposed && socketRef.current === socket) {
          reconnectAttemptRef.current = 0;
          setConnectionStatus("connected");
        }
      };

      socket.onmessage = (event) => {
        try {
          const update = JSON.parse(event.data);
          if (!isAnalyticsUpdate(update)) {
            return;
          }
          pendingUpdateRef.current.set(update.symbol, update);
          if (updateTimerRef.current === null) {
            updateTimerRef.current = window.setTimeout(flushLatestUpdate, UI_UPDATE_INTERVAL_MS);
          }
        } catch {
          // Ignore malformed WebSocket messages without disrupting a live connection.
        }
      };

      socket.onerror = () => {
        if (!disposed) {
          setConnectionStatus("error");
          socket.close();
        }
      };

      socket.onclose = () => {
        if (!disposed && socketRef.current === socket) {
          socketRef.current = null;
          scheduleReconnect();
        }
      };
    };

    connect();

    return () => {
      disposed = true;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      if (updateTimerRef.current !== null) {
        window.clearTimeout(updateTimerRef.current);
        updateTimerRef.current = null;
      }
      pendingUpdateRef.current.clear();
      socketRef.current?.close();
      socketRef.current = null;
    };
  }, [url]);

  return { ...dashboard, connectionStatus, webSocketUrl: url };
}

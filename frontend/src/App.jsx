import { useEffect, useState } from "react";

import MetricCard from "./components/MetricCard";
import PriceChart from "./components/PriceChart";
import { useAnalyticsWebSocket } from "./hooks/useAnalyticsWebSocket";

const priceFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

const percentFormatter = new Intl.NumberFormat("en-US", {
  style: "percent",
  minimumFractionDigits: 3,
  maximumFractionDigits: 3,
});

const statusLabels = {
  connecting: "Connecting",
  connected: "Live",
  reconnecting: "Reconnecting",
  error: "Connection error",
};

function optionalPrice(value) {
  return Number.isFinite(value) ? priceFormatter.format(value) : "—";
}

function optionalPercent(value) {
  return Number.isFinite(value) ? percentFormatter.format(value) : "—";
}

export default function App() {
  const { latestBySymbol, historyBySymbol, priceDirections, connectionStatus } = useAnalyticsWebSocket();
  const symbols = Object.keys(latestBySymbol).sort();
  const [selectedSymbol, setSelectedSymbol] = useState(null);

  useEffect(() => {
    if (!selectedSymbol && symbols.length) setSelectedSymbol(symbols[0]);
  }, [selectedSymbol, symbols]);

  const activeSymbol = selectedSymbol && latestBySymbol[selectedSymbol] ? selectedSymbol : symbols[0];
  const latest = activeSymbol ? latestBySymbol[activeSymbol] : null;
  const history = activeSymbol ? historyBySymbol[activeSymbol] || [] : [];
  const priceDirection = activeSymbol ? priceDirections[activeSymbol] : "neutral";
  const priceTone = priceDirection === "up" ? "gain" : priceDirection === "down" ? "loss" : "default";

  return (
    <main className="dashboard-shell">
      <header className="dashboard-header">
        <div>
          <p className="eyebrow">Portfolio risk analytics</p>
          <h1>Real-time market dashboard</h1>
          <p className="dashboard-header__subtext">Streaming analytics from the local risk processor.</p>
        </div>
        <div className={`connection-status connection-status--${connectionStatus}`}>
          <span className="connection-status__dot" />
          {statusLabels[connectionStatus]}
        </div>
      </header>

      <section className="symbol-bar" aria-label="Active market">
        <span className="symbol-bar__label">Active symbol</span>
        <div className="symbol-tabs">
          {symbols.length ? symbols.map((symbol) => (
            <button className={symbol === activeSymbol ? "symbol-tab symbol-tab--active" : "symbol-tab"} key={symbol} onClick={() => setSelectedSymbol(symbol)} type="button">
              {symbol}
            </button>
          )) : <strong>Awaiting stream</strong>}
        </div>
        {latest && <span>{latest.observations} observations in risk window</span>}
      </section>

      <section className="metrics-grid" aria-label="Current analytics">
        <MetricCard
          label="Current price"
          value={optionalPrice(latest?.current_price)}
          note={priceDirection === "up" ? "↑ Latest update gained" : priceDirection === "down" ? "↓ Latest update dropped" : "Awaiting comparison"}
          tone={priceTone}
        />
        <MetricCard label="SMA-30" value={optionalPrice(latest?.sma_30)} note="Latest 30 prices" />
        <MetricCard label="Volatility" value={optionalPercent(latest?.volatility)} note="Return standard deviation" />
        <MetricCard label="VaR 95%" value={optionalPercent(latest?.var_95)} note="Normalized one-period loss" tone="risk" />
      </section>

      <PriceChart history={history} priceDirection={priceDirection} />
    </main>
  );
}

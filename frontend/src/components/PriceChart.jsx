import { memo } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const formatPrice = (value) =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);

const formatTime = (timestamp) =>
  new Date(timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });

function PriceChart({ history, priceDirection }) {
  const stroke = priceDirection === "down" ? "#ef5b5b" : "#22c58b";

  return (
    <section className="chart-card">
      <div className="chart-card__header">
        <div>
          <p className="eyebrow">Live market</p>
          <h2>Price history</h2>
        </div>
        <span className="chart-card__caption">Last {history.length}/30 updates</span>
      </div>
      <div className="chart-card__canvas">
        {history.length ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={history} margin={{ top: 10, right: 12, left: 8, bottom: 0 }}>
              <CartesianGrid stroke="#26324a" strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="timestamp"
                tickFormatter={formatTime}
                tick={{ fill: "#8492ad", fontSize: 12 }}
                tickLine={false}
                axisLine={false}
                minTickGap={38}
              />
              <YAxis
                dataKey="price"
                tickFormatter={(value) => `$${Number(value).toFixed(0)}`}
                tick={{ fill: "#8492ad", fontSize: 12 }}
                tickLine={false}
                axisLine={false}
                width={72}
                domain={["dataMin", "dataMax"]}
              />
              <Tooltip
                formatter={(value) => formatPrice(value)}
                labelFormatter={formatTime}
                contentStyle={{ background: "#121b2d", border: "1px solid #2b3955", borderRadius: 8 }}
                labelStyle={{ color: "#c7d2e8" }}
              />
              <Line
                type="monotone"
                dataKey="price"
                stroke={stroke}
                strokeWidth={2.25}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="chart-card__empty">Waiting for the first analytics update…</div>
        )}
      </div>
    </section>
  );
}

export default memo(PriceChart);

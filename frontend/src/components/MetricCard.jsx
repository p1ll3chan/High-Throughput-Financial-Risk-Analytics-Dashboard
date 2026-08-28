import { memo } from "react";

function MetricCard({ label, value, note, tone = "default" }) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <p className="metric-card__label">{label}</p>
      <p className="metric-card__value">{value}</p>
      {note && <p className="metric-card__note">{note}</p>}
    </article>
  );
}

export default memo(MetricCard);

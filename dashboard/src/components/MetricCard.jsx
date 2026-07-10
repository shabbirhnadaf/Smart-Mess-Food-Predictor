import React from "react";

export default function MetricCard({ label, value, hint, accent = "teal", icon }) {
  return (
    <article className={`metric-card metric-card--${accent}`}>
      <div className="metric-card__topline">
        <span className="metric-card__icon">{icon}</span>
        <span className="metric-card__label">{label}</span>
      </div>
      <div className="metric-card__value">{value}</div>
      <div className="metric-card__hint">{hint}</div>
    </article>
  );
}

import React from "react";
import StatusPill from "./StatusPill";

function metric(value, fallback = "n/a") {
  return value === undefined || value === null || value === "" ? fallback : value;
}

export default function ServiceStatusPanel({ serviceInfo, health, kafkaStatus, socketConnected, socketError }) {
  const healthTone = health?.status === "healthy" ? "success" : "warning";
  const kafkaTone =
    kafkaStatus?.status === "streaming"
      ? "success"
      : kafkaStatus?.status === "waiting"
        ? "warning"
        : "neutral";

  return (
    <div className="status-panel">
      <div className="status-panel__row">
        <span>API</span>
        <StatusPill tone={healthTone}>{metric(health?.status, "unknown")}</StatusPill>
      </div>
      <div className="status-panel__row">
        <span>Kafka</span>
        <StatusPill tone={kafkaTone}>{metric(kafkaStatus?.status, "unknown")}</StatusPill>
      </div>
      <div className="status-panel__row">
        <span>WebSocket</span>
        <StatusPill tone={socketConnected ? "success" : "danger"}>{socketConnected ? "connected" : "offline"}</StatusPill>
      </div>
      <div className="status-panel__row">
        <span>Stack</span>
        <span>{metric(serviceInfo?.stack)}</span>
      </div>
      <div className="status-panel__row">
        <span>Spark Records</span>
        <span>{metric(health?.spark_records, 0)}</span>
      </div>
      <div className="status-panel__row">
        <span>Parquet Files</span>
        <span>{metric(health?.parquet_files, 0)}</span>
      </div>
      <div className="status-panel__row">
        <span>Food Items Tracked</span>
        <span>{metric(health?.food_items_tracked, 0)}</span>
      </div>
      <div className="status-panel__row">
        <span>Latest Window</span>
        <span className="status-panel__mono">{metric(kafkaStatus?.last_window)}</span>
      </div>
      <div className="status-panel__hint">
        {socketError
          ? socketError
          : "The dashboard uses REST snapshots for stable reads and a reconnecting WebSocket for live movement."}
      </div>
    </div>
  );
}

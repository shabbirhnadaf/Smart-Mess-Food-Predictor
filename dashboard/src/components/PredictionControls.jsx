import React from "react";

const slots = ["breakfast", "lunch", "dinner"];
const dayTypes = ["weekday", "weekend", "holiday"];

export default function PredictionControls({
  selectedSlot,
  onSlotChange,
  selectedDayType,
  onDayTypeChange,
  onRefresh,
  onRetrain,
  busy,
  lastUpdated,
}) {
  return (
    <div className="controls-card">
      <div className="controls-card__group">
        <div className="controls-card__label">Meal Slot</div>
        <div className="chip-row">
          {slots.map((slot) => (
            <button
              key={slot}
              type="button"
              className={`chip-button ${selectedSlot === slot ? "chip-button--active" : ""}`}
              onClick={() => onSlotChange(slot)}
            >
              {slot}
            </button>
          ))}
        </div>
      </div>

      <div className="controls-card__group">
        <div className="controls-card__label">Day Type</div>
        <div className="chip-row">
          {dayTypes.map((dayType) => (
            <button
              key={dayType}
              type="button"
              className={`chip-button chip-button--soft ${selectedDayType === dayType ? "chip-button--active" : ""}`}
              onClick={() => onDayTypeChange(dayType)}
            >
              {dayType}
            </button>
          ))}
        </div>
      </div>

      <div className="controls-card__actions">
        <button type="button" className="action-button" onClick={onRefresh}>
          Refresh Snapshot
        </button>
        <button type="button" className="action-button action-button--secondary" onClick={onRetrain} disabled={busy}>
          {busy ? "Submitting Retrain..." : "Trigger Model Retrain"}
        </button>
      </div>

      <div className="controls-card__meta">
        Dashboard snapshot targets the live API while the WebSocket keeps streaming the current meal cycle.
        {lastUpdated ? ` Last refresh: ${lastUpdated}.` : ""}
      </div>
    </div>
  );
}

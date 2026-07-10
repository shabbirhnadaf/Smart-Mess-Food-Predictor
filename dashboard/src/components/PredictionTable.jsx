import React from "react";

export default function PredictionTable({ rows }) {
  if (!rows.length) {
    return <div className="empty-state">No prediction rows are available yet. Start the stream and refresh again.</div>;
  }

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            <th>Food Item</th>
            <th>Consumption</th>
            <th>Preparation</th>
            <th>Est. Waste</th>
            <th>Source</th>
            <th>Status</th>
            <th>Recommendation</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.meal_slot}-${row.food_item}`}>
              <td className="data-table__primary">{row.food_item}</td>
              <td>{row.predicted_consumption}</td>
              <td>{row.predicted_preparation}</td>
              <td>{row.predicted_waste} ({row.waste_percentage}%)</td>
              <td>{row.data_source}</td>
              <td>
                <span className={`inline-status ${row.is_high_waste_alert ? "inline-status--warning" : "inline-status--good"}`}>
                  {row.is_high_waste_alert ? "High waste" : "Normal"}
                </span>
              </td>
              <td className="data-table__recommendation">{row.recommendation}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

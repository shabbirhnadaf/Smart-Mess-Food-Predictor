import React from "react";
import { Bar } from "react-chartjs-2";
import "./chartSetup";

export default function TrendBarChart({ rows }) {
  const trimmed = rows.slice(0, 8);
  const data = {
    labels: trimmed.map((row) => `${row.food_item} / ${row.meal_slot}`),
    datasets: [
      {
        label: "Total Quantity",
        data: trimmed.map((row) => row.total_quantity),
        backgroundColor: [
          "#154c79",
          "#127a6c",
          "#f08a4b",
          "#9d5c63",
          "#40798c",
          "#5c6f68",
          "#8b6f47",
          "#5a4e7c",
        ],
        borderRadius: 10,
      },
    ],
  };

  const options = {
    indexAxis: "y",
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
    },
    scales: {
      x: {
        ticks: {
          color: "#47625a",
        },
        grid: {
          color: "rgba(71, 98, 90, 0.12)",
        },
      },
      y: {
        ticks: {
          color: "#47625a",
        },
        grid: {
          display: false,
        },
      },
    },
  };

  return (
    <div className="chart-box chart-box--short">
      <Bar data={data} options={options} />
    </div>
  );
}

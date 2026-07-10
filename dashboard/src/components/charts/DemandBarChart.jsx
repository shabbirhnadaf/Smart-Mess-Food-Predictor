import React from "react";
import { Bar } from "react-chartjs-2";
import "./chartSetup";

export default function DemandBarChart({ predictions }) {
  const labels = predictions.map((row) => row.food_item);

  const data = {
    labels,
    datasets: [
      {
        label: "Expected Consumption",
        data: predictions.map((row) => row.predicted_consumption),
        backgroundColor: "#127a6c",
        borderRadius: 8,
      },
      {
        label: "Prepared Qty",
        data: predictions.map((row) => row.predicted_preparation),
        backgroundColor: "#f08a4b",
        borderRadius: 8,
      },
      {
        label: "Expected Waste",
        data: predictions.map((row) => row.predicted_waste),
        backgroundColor: "#d1405c",
        borderRadius: 8,
      },
    ],
  };

  const options = {
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: "bottom",
        labels: {
          color: "#25403b",
          boxWidth: 10,
          usePointStyle: true,
        },
      },
    },
    scales: {
      x: {
        ticks: {
          color: "#47625a",
        },
        grid: {
          display: false,
        },
      },
      y: {
        ticks: {
          color: "#47625a",
        },
        grid: {
          color: "rgba(71, 98, 90, 0.12)",
        },
      },
    },
  };

  return (
    <div className="chart-box">
      <Bar data={data} options={options} />
    </div>
  );
}

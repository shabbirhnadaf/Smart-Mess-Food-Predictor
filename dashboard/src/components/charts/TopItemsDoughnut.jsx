import React from "react";
import { Doughnut } from "react-chartjs-2";
import "./chartSetup";

export default function TopItemsDoughnut({ rows }) {
  const data = {
    labels: rows.map((row) => row.food_item),
    datasets: [
      {
        data: rows.map((row) => row.total_quantity),
        backgroundColor: ["#127a6c", "#f08a4b", "#154c79", "#d66a6a", "#8b6f47"],
        borderWidth: 0,
      },
    ],
  };

  const options = {
    cutout: "65%",
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: "bottom",
        labels: {
          color: "#25403b",
          usePointStyle: true,
          boxWidth: 10,
        },
      },
    },
  };

  return (
    <div className="chart-box chart-box--donut">
      <Doughnut data={data} options={options} />
    </div>
  );
}

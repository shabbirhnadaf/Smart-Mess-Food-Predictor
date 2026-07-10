import React from "react";

const toneClassMap = {
  neutral: "pill pill--neutral",
  success: "pill pill--success",
  warning: "pill pill--warning",
  danger: "pill pill--danger",
  info: "pill pill--info",
};

export default function StatusPill({ children, tone = "neutral" }) {
  return <span className={toneClassMap[tone] || toneClassMap.neutral}>{children}</span>;
}

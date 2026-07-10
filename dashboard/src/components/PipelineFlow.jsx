import React from "react";

export default function PipelineFlow({ steps }) {
  return (
    <div className="pipeline-grid">
      {steps.map((step) => (
        <article key={step.name} className="pipeline-step">
          <div className="pipeline-step__kicker">{step.stage}</div>
          <div className="pipeline-step__title">{step.name}</div>
          <div className="pipeline-step__body">{step.description}</div>
        </article>
      ))}
    </div>
  );
}

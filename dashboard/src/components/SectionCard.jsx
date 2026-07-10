import React from "react";

export default function SectionCard({ eyebrow, title, subtitle, action, children, className = "" }) {
  return (
    <section className={`section-card ${className}`.trim()}>
      <div className="section-card__header">
        <div>
          {eyebrow ? <div className="section-card__eyebrow">{eyebrow}</div> : null}
          <h2 className="section-card__title">{title}</h2>
          {subtitle ? <p className="section-card__subtitle">{subtitle}</p> : null}
        </div>
        {action ? <div className="section-card__action">{action}</div> : null}
      </div>
      <div className="section-card__body">{children}</div>
    </section>
  );
}

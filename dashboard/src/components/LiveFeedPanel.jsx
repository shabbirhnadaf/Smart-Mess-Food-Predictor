import React from "react";

export default function LiveFeedPanel({ logs }) {
  if (!logs.length) {
    return <div className="empty-state">Waiting for `/ws/live` messages from FastAPI.</div>;
  }

  return (
    <div className="feed-panel">
      {logs.map((entry, index) => (
        <div key={`${entry.timestamp}-${index}`} className="feed-panel__entry">
          <div className="feed-panel__time">{entry.timestamp}</div>
          <div className="feed-panel__message">{entry.message}</div>
        </div>
      ))}
    </div>
  );
}

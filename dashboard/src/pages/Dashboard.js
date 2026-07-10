import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, Database, Radar, Rows3, SatelliteDish, TimerReset } from "lucide-react";
import SectionCard from "../components/SectionCard";
import MetricCard from "../components/MetricCard";
import PredictionControls from "../components/PredictionControls";
import PredictionTable from "../components/PredictionTable";
import PipelineFlow from "../components/PipelineFlow";
import ServiceStatusPanel from "../components/ServiceStatusPanel";
import LiveFeedPanel from "../components/LiveFeedPanel";
import DemandBarChart from "../components/charts/DemandBarChart";
import TrendBarChart from "../components/charts/TrendBarChart";
import TopItemsDoughnut from "../components/charts/TopItemsDoughnut";
import StatusPill from "../components/StatusPill";
import { useWebSocket } from "../hooks/useWebSocket";
import { getJson, postJson, WS_URL } from "../lib/api";

function formatTime(value) {
  if (!value) {
    return null;
  }

  return new Intl.DateTimeFormat("en-IN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function pushLog(existingLogs, message) {
  const nextLog = {
    timestamp: formatTime(new Date()) || "--:--:--",
    message,
  };

  return [nextLog, ...existingLogs].slice(0, 24);
}

export default function Dashboard() {
  const [selectedSlot, setSelectedSlot] = useState("lunch");
  const [selectedDayType, setSelectedDayType] = useState("weekday");
  const [serviceInfo, setServiceInfo] = useState(null);
  const [health, setHealth] = useState(null);
  const [kafkaStatus, setKafkaStatus] = useState(null);
  const [trends, setTrends] = useState([]);
  const [topItems, setTopItems] = useState([]);
  const [hostelRows, setHostelRows] = useState([]);
  const [hostelSummary, setHostelSummary] = useState([]);
  const [predictions, setPredictions] = useState([]);
  const [fetchError, setFetchError] = useState(null);
  const [lastRefreshAt, setLastRefreshAt] = useState(null);
  const [retrainMessage, setRetrainMessage] = useState("");
  const [isRetraining, setIsRetraining] = useState(false);
  const [logs, setLogs] = useState([]);

  const { data: liveData, isConnected, error: socketError, lastMessageAt } = useWebSocket(WS_URL);

  const refreshDashboard = useCallback(async () => {
    try {
      setFetchError(null);

      const [
        serviceResponse,
        healthResponse,
        kafkaResponse,
        trendsResponse,
        topItemsResponse,
        hostelResponse,
        predictionsResponse,
      ] = await Promise.all([
        getJson("/"),
        getJson("/health"),
        getJson("/kafka/status"),
        getJson("/analytics/trends"),
        getJson(`/analytics/top-items/${selectedSlot}`),
        getJson("/analytics/hostel"),
        getJson(`/predict/all/${selectedSlot}`, {
          params: { day_type: selectedDayType },
        }),
      ]);

      setServiceInfo(serviceResponse);
      setHealth(healthResponse);
      setKafkaStatus(kafkaResponse);
      setTrends(trendsResponse.trends || []);
      setTopItems(topItemsResponse.top_items || []);
      setHostelRows(hostelResponse.breakdown || []);
      setHostelSummary(hostelResponse.summary || []);
      setPredictions(predictionsResponse.predictions || []);
      setLastRefreshAt(new Date());
    } catch (caughtError) {
      setFetchError(
        caughtError?.response?.data?.message ||
          caughtError.message ||
          "Dashboard refresh failed. Check that the API is running."
      );
    }
  }, [selectedDayType, selectedSlot]);

  useEffect(() => {
    refreshDashboard();
    const intervalId = window.setInterval(refreshDashboard, 20000);
    return () => window.clearInterval(intervalId);
  }, [refreshDashboard]);

  useEffect(() => {
    if (!liveData) {
      return;
    }

    const predictionsCount = liveData.predictions?.length || 0;
    const kafkaRecords = liveData.kafka_records || 0;
    const liveSlot = liveData.meal_slot || "unknown";
    setLogs((existingLogs) =>
      pushLog(existingLogs, `Live WebSocket batch for ${liveSlot}: ${predictionsCount} items, ${kafkaRecords} records observed.`)
    );
  }, [liveData]);

  const handleRetrain = useCallback(async () => {
    try {
      setIsRetraining(true);
      const result = await postJson("/model/retrain");
      setRetrainMessage(result.message || "Retraining job submitted.");
      setLogs((existingLogs) => pushLog(existingLogs, `Retrain requested: ${result.job_id || "job submitted"}.`));
    } catch (caughtError) {
      setRetrainMessage(
        caughtError?.response?.data?.message || caughtError.message || "Could not submit retraining job."
      );
    } finally {
      setIsRetraining(false);
    }
  }, []);

  const activePredictions = useMemo(() => {
    const source = predictions.length ? predictions : liveData?.predictions || [];
    return [...source].sort((left, right) => right.predicted_waste - left.predicted_waste);
  }, [liveData, predictions]);

  const alertCount = activePredictions.filter((row) => row.is_high_waste_alert).length;
  const totalPredicted = activePredictions.reduce((sum, row) => sum + (row.predicted_consumption || 0), 0);
  const totalWaste = activePredictions.reduce((sum, row) => sum + (row.predicted_waste || 0), 0);
  const totalPrepared = activePredictions.reduce((sum, row) => sum + (row.predicted_preparation || 0), 0);
  const wastePercentage = totalPrepared > 0 ? Math.round((totalWaste / totalPrepared) * 100) : 0;
  const totalCostLost = activePredictions.reduce((sum, row) => sum + (row.cost_lost || 0), 0);
  const liveSlot = liveData?.meal_slot || selectedSlot;
  const topPrediction = activePredictions[0];
  const topHostel = hostelSummary[0];

  const metrics = [
    {
      label: "Live Meal Slot",
      value: liveSlot,
      hint: "Current slot broadcast on the WebSocket feed",
      accent: "teal",
      icon: <Activity size={18} />,
    },
    {
      label: "Total Predicted Waste",
      value: `${totalWaste.toLocaleString()} portions`,
      hint: `Est. ${wastePercentage}% of prepared food`,
      accent: "amber",
      icon: <Rows3 size={18} />,
    },
    {
      label: "High-Waste Alerts",
      value: alertCount.toString(),
      hint: "Items crossing the waste alert threshold (20%)",
      accent: alertCount ? "rose" : "slate",
      icon: <Radar size={18} />,
    },
    {
      label: "Cost Lost",
      value: `₹${totalCostLost.toLocaleString()}`,
      hint: "Est. monetary loss due to wasted food",
      accent: "rose",
      icon: <Radar size={18} />,
    },
    
    {
      label: "Top Predicted Item",
      value: topPrediction?.food_item || "n/a",
      hint: topPrediction ? `${topPrediction.predicted_waste} wasted portions` : "No active prediction payload yet",
      accent: "teal",
      icon: <SatelliteDish size={18} />,
    },
    {
      label: "Last Live Message",
      value: formatTime(lastMessageAt) || "waiting",
      hint: "Updated whenever `/ws/live` pushes a fresh batch",
      accent: "slate",
      icon: <TimerReset size={18} />,
    },
  ];

  const pipelineSteps = [
    {
      stage: "ingest",
      name: "Kafka Producers",
      description: "RFID-style order events, weather events, and campus events are published to local Kafka topics.",
    },
    {
      stage: "stream",
      name: "Spark Aggregator",
      description: "Structured Streaming aggregates per-food demand and writes parquet to local disk and HDFS every 30 seconds.",
    },
    {
      stage: "store",
      name: "Parquet + HDFS",
      description: "Local parquet powers the live API immediately while HDFS keeps the broader big-data layout intact.",
    },
    {
      stage: "serve",
      name: "FastAPI Service",
      description: "REST endpoints provide health, analytics, and predictions while `/ws/live` broadcasts the current slot.",
    },
    {
      stage: "observe",
      name: "React Dashboard",
      description: "The frontend combines REST snapshots with WebSocket updates into an operator-friendly control surface.",
    },
  ];

  return (
    <div className="app-shell">
      <div className="app-shell__backdrop app-shell__backdrop--left" />
      <div className="app-shell__backdrop app-shell__backdrop--right" />

      <header className="hero">
        <div className="hero__copy">
          <div className="hero__eyebrow">Campus Waste Intelligence</div>
          <h1 className="hero__title">Smart Waste Management Intelligence</h1>
          <p className="hero__subtitle">
            A live operations dashboard for a local Hadoop, Kafka, Spark, FastAPI, and React pipeline. It is designed
            to show what the repo actually runs today, not an imagined future state.
          </p>
          <div className="hero__status-row">
            <StatusPill tone={isConnected ? "success" : "danger"}>{isConnected ? "WebSocket live" : "WebSocket offline"}</StatusPill>
            <StatusPill tone={health?.status === "healthy" ? "success" : "warning"}>
              API {health?.status || "unknown"}
            </StatusPill>
            <StatusPill tone={kafkaStatus?.status === "streaming" ? "success" : "warning"}>
              Kafka {kafkaStatus?.status || "waiting"}
            </StatusPill>
          </div>
        </div>

        <div className="hero__panel">
          <div className="hero__panel-label">Current Snapshot</div>
          <div className="hero__panel-value">{totalWaste.toLocaleString()}</div>
          <div className="hero__panel-caption">portions of expected waste for {selectedSlot} on a {selectedDayType}</div>
          <div className="hero__panel-meta">
            {serviceInfo?.parquet_files || 0} parquet files | {health?.food_items_tracked || 0} tracked food items
          </div>
        </div>
      </header>

      <PredictionControls
        selectedSlot={selectedSlot}
        onSlotChange={setSelectedSlot}
        selectedDayType={selectedDayType}
        onDayTypeChange={setSelectedDayType}
        onRefresh={refreshDashboard}
        onRetrain={handleRetrain}
        busy={isRetraining}
        lastUpdated={formatTime(lastRefreshAt)}
      />

      {fetchError ? <div className="callout callout--danger">{fetchError}</div> : null}
      {retrainMessage ? <div className="callout callout--info">{retrainMessage}</div> : null}

      <div className="metric-grid">
        {metrics.map((metric) => (
          <MetricCard key={metric.label} {...metric} />
        ))}
      </div>

      <div className="content-grid">
        <SectionCard
          eyebrow="Predictions"
          title="Forecast vs Buffer"
          subtitle="The selected meal slot is fetched from REST so you can compare a stable snapshot against the live stream."
        >
          {activePredictions.length ? (
            <DemandBarChart predictions={activePredictions.slice(0, 8)} />
          ) : (
            <div className="empty-state">Prediction data will appear here after the API returns a populated snapshot.</div>
          )}
        </SectionCard>

        <SectionCard
          eyebrow="Runtime Health"
          title="Service Status"
          subtitle="Live status derived from the API root, `/health`, `/kafka/status`, and the WebSocket connection."
        >
          <ServiceStatusPanel
            serviceInfo={serviceInfo}
            health={health}
            kafkaStatus={kafkaStatus}
            socketConnected={isConnected}
            socketError={socketError}
          />
        </SectionCard>
      </div>

      <div className="content-grid">
        <SectionCard
          eyebrow="Streaming Analytics"
          title="Top Demand Trends"
          subtitle="This comes from `/analytics/trends`, which summarizes Spark parquet output by food item and meal slot."
        >
          {trends.length ? (
            <TrendBarChart rows={trends} />
          ) : (
            <div className="empty-state">Trend analytics will populate once the streaming aggregator has written parquet data.</div>
          )}
        </SectionCard>

        <SectionCard
          eyebrow="Selected Slot"
          title="Top Items Mix"
          subtitle={`The donut reflects /analytics/top-items/${selectedSlot} for the current dashboard selection.`}
        >
          {topItems.length ? (
            <TopItemsDoughnut rows={topItems} />
          ) : (
            <div className="empty-state">Top-item analytics are not available yet for this meal slot.</div>
          )}
        </SectionCard>
      </div>

      <div className="content-grid">
        <SectionCard
          eyebrow="Operations Table"
          title="Prediction Breakdown"
          subtitle="Actionable quantities, buffer sizes, confidence estimates, and rule-based recommendations."
        >
          <PredictionTable rows={activePredictions} />
        </SectionCard>

        <SectionCard
          eyebrow="Live Feed"
          title="WebSocket Activity"
          subtitle="Recent live events derived from `/ws/live`. This is the fastest way to confirm the stream is moving."
        >
          <LiveFeedPanel logs={logs} />
        </SectionCard>
      </div>

      <div className="content-grid">
        <SectionCard
          eyebrow="Hostel Distribution"
          title="Hostel Breakdown"
          subtitle="Top hostel-food demand distribution for the current streaming window."
        >
          {hostelRows.length ? (
            <div className="hostel-panel">
              <div className="hostel-panel__summary">
                <div className="hostel-kpi">
                  <div className="hostel-kpi__label">Top Hostel Block</div>
                  <div className="hostel-kpi__value">{topHostel?.hostel_block || "n/a"}</div>
                </div>
                <div className="hostel-kpi">
                  <div className="hostel-kpi__label">Top Hostel Quantity</div>
                  <div className="hostel-kpi__value">{(topHostel?.total_quantity || 0).toLocaleString()}</div>
                </div>
                <div className="hostel-kpi">
                  <div className="hostel-kpi__label">Hostels Tracked</div>
                  <div className="hostel-kpi__value">{hostelSummary.length.toLocaleString()}</div>
                </div>
              </div>
              <div className="mini-table">
                {hostelRows.slice(0, 10).map((row) => (
                  <div key={`${row.hostel_block}-${row.food_item}`} className="mini-table__row mini-table__row--hostel">
                    <span>{row.hostel_block}</span>
                    <span>{row.food_item}</span>
                    <span>{(row.hostel_total_quantity || row.hostel_order_count || 0).toLocaleString()} qty</span>
                    <strong>{(row.hostel_order_count || 0).toLocaleString()} orders</strong>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="empty-state">
              Hostel aggregation data is not available yet. Once the streaming aggregator writes `hostel_agg`, this panel
              will show top hostel-food demand combinations automatically.
            </div>
          )}
        </SectionCard>

        <SectionCard
          eyebrow="Startup Map"
          title="Pipeline Structure"
          subtitle="This reflects the repo as analyzed: Kafka producers, Spark aggregation, parquet storage, FastAPI, and the React dashboard."
        >
          <PipelineFlow steps={pipelineSteps} />
        </SectionCard>
      </div>
    </div>
  );
}

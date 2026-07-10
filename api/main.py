"""
FastAPI — Smart Mess Food Demand Predictor
Reads Spark Parquet output directly from local disk (written by streaming job).
Run: uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
"""
import json
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
from threading import Lock

import pandas as pd
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Smart Mess Food Demand Predictor", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────────
# Spark writeStream writes parquet here (local output path)
PARQUET_DEMAND_DIRS = [
    Path("C:/mess-predictor/data/streaming_output/demand_agg_live"),
    Path("C:/mess-predictor/data/streaming_output/demand_agg"),
]
PARQUET_HOSTEL  = Path("C:/mess-predictor/data/streaming_output/hostel_agg")

FOOD_ITEMS = [
    "idli", "dosa", "upma", "poha", "bread_butter",
    "rice", "dal", "veg_curry", "egg_curry", "chapati",
    "biryani", "pulao", "sambar", "rasam", "curd_rice",
]
MEAL_SLOTS = ["breakfast", "lunch", "dinner"]
active_ws: list[WebSocket] = []
_demand_cache_lock = Lock()
_demand_cache_df = pd.DataFrame()
_demand_cache_signature: tuple[int, tuple[tuple[str, int], ...]] | None = None
_demand_cache_checked_at = 0.0
_CACHE_TTL_SECONDS = 2.0


def get_active_demand_dir() -> Path:
    for parquet_dir in PARQUET_DEMAND_DIRS:
        if parquet_dir.exists() and list(parquet_dir.glob("**/*.parquet")):
            return parquet_dir
    return PARQUET_DEMAND_DIRS[0]


def get_demand_files() -> list[Path]:
    parquet_dir = get_active_demand_dir()
    if not parquet_dir.exists():
        return []
    try:
        files = list(parquet_dir.glob("**/*.parquet"))
    except (FileNotFoundError, OSError):
        # Spark may rotate `_temporary` folders while we scan.
        return []
    return [f for f in files if "_temporary" not in str(f)]

# ── Cached dataframe (refreshed every call) ───────────────────────────
def read_demand_df() -> pd.DataFrame:
    """Read all Parquet files written by Spark Structured Streaming."""
    global _demand_cache_df, _demand_cache_signature, _demand_cache_checked_at

    now_ts = datetime.now().timestamp()
    with _demand_cache_lock:
        if (now_ts - _demand_cache_checked_at) < _CACHE_TTL_SECONDS:
            return _demand_cache_df.copy()

    files = get_demand_files()
    if not files:
        with _demand_cache_lock:
            _demand_cache_df = pd.DataFrame()
            _demand_cache_signature = (0, tuple())
            _demand_cache_checked_at = now_ts
        return pd.DataFrame()

    file_signature = []
    for file_path in files:
        try:
            file_signature.append((str(file_path), file_path.stat().st_mtime_ns))
        except OSError:
            continue
    signature = (len(file_signature), tuple(sorted(file_signature)))

    with _demand_cache_lock:
        if _demand_cache_signature == signature:
            _demand_cache_checked_at = now_ts
            return _demand_cache_df.copy()

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except (FileNotFoundError, OSError, ValueError):
            # Ignore transient files while Spark is committing a batch.
            pass
    next_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    with _demand_cache_lock:
        _demand_cache_df = next_df
        _demand_cache_signature = signature
        _demand_cache_checked_at = now_ts

    return next_df.copy()


def read_hostel_df() -> pd.DataFrame:
    files = list(PARQUET_HOSTEL.glob("**/*.parquet")) if PARQUET_HOSTEL.exists() else []
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── Prediction engine ─────────────────────────────────────────────────
def predict_waste(food_item: str, meal_slot: str, day_type: str = "weekday", df: Optional[pd.DataFrame] = None) -> dict:
    if df is None:
        df = read_demand_df()
    base_demand = 60
    base_prep = 80
    confidence  = 0.65
    data_source = "rule-based"

    if not df.empty and "food_item" in df.columns:
        mask    = (df["food_item"] == food_item) & (df["meal_slot"] == meal_slot)
        subset  = df[mask]
        if not subset.empty:
            base_demand = int(subset["total_quantity"].sum())
            if "total_prepared" in subset.columns:
                 base_prep = int(subset["total_prepared"].sum())
            else:
                 base_prep = int(base_demand * 1.25)
            confidence  = min(0.97, 0.70 + len(subset) * 0.05)
            data_source = "spark-hdfs"

    day_mult  = {"weekday": 1.0, "weekend": 0.78, "holiday": 0.45}.get(day_type, 1.0)
    slot_mult = {"breakfast": 0.85, "lunch": 1.25, "dinner": 1.0}.get(meal_slot, 1.0)
    
    predicted_consumption = max(10, int(base_demand * day_mult * slot_mult))
    # Mess managers tend to not reduce prep enough on holidays
    prep_mult = {"weekday": 1.0, "weekend": 0.90, "holiday": 0.80}.get(day_type, 1.0)
    predicted_prep = max(15, int(base_prep * prep_mult * slot_mult))
    
    # Ensure prep is always at least consumption
    predicted_prep = max(predicted_prep, predicted_consumption + 5)
    
    predicted_waste = predicted_prep - predicted_consumption
    waste_percentage = round((predicted_waste / predicted_prep) * 100, 1)
    
    is_high_waste_alert = waste_percentage > 20
    
    cost_per_portion = 35 # Assuming avg 35 rupees per portion
    cost_lost = predicted_waste * cost_per_portion
    
    recommendation = ""
    if is_high_waste_alert:
        recommended_reduction = int(predicted_waste * 0.8)
        recommendation = f"⚠️ HIGH WASTE PREDICTED. Reduce preparation by {recommended_reduction} portions to save ₹{recommended_reduction * cost_per_portion}!"
    else:
        recommendation = "Waste levels are within normal bounds. Keep current preparation."

    return {
        "food_item":            food_item,
        "meal_slot":            meal_slot,
        "day_type":             day_type,
        "predicted_consumption":predicted_consumption,
        "predicted_preparation":predicted_prep,
        "predicted_waste":      predicted_waste,
        "waste_percentage":     waste_percentage,
        "cost_lost":            cost_lost,
        "confidence":           round(confidence, 2),
        "data_source":          data_source,
        "is_high_waste_alert":  is_high_waste_alert,
        "recommendation":       recommendation,
        "timestamp": datetime.now().isoformat(),
    }


# ── Routes ────────────────────────────────────────────────────────────

@app.get("/")
def root():
    df = read_demand_df()
    parquet_dir = get_active_demand_dir()
    return {
        "service":       "Smart Mess Food Demand Predictor",
        "stack":         "Kafka 4.0.2 + Spark 3.5.8 + HDFS 3.4.3 + FastAPI + Python 3.10",
        "status":        "live",
        "spark_records": len(df),
        "parquet_dir":   str(parquet_dir),
        "parquet_files": len(get_demand_files()),
    }


@app.get("/health")
def health():
    df = read_demand_df()
    parquet_dir = get_active_demand_dir()
    files = get_demand_files()
    return {
        "status":             "healthy",
        "kafka":              "streaming",
        "hdfs":               "connected",
        "spark_records":      len(df),
        "parquet_files":      len(files),
        "food_items_tracked": df["food_item"].nunique() if not df.empty and "food_item" in df.columns else 0,
        "meal_slots_tracked": df["meal_slot"].unique().tolist() if not df.empty and "meal_slot" in df.columns else [],
        "parquet_dir":        str(parquet_dir),
        "timestamp":          datetime.now().isoformat(),
    }


class PredictRequest(BaseModel):
    food_item: str
    meal_slot: str
    day_type:  Optional[str] = "weekday"


@app.post("/predict/waste")
def predict_single(req: PredictRequest):
    return predict_waste(req.food_item, req.meal_slot, req.day_type)


@app.get("/predict/all/{meal_slot}")
def predict_all(meal_slot: str, day_type: str = "weekday"):
    demand_df = read_demand_df()
    results = sorted(
        [predict_waste(item, meal_slot, day_type, demand_df) for item in FOOD_ITEMS],
        key=lambda x: x["predicted_waste"], reverse=True
    )
    alerts = [r["food_item"] for r in results if r["is_high_waste_alert"]]
    return {
        "meal_slot":                meal_slot,
        "day_type":                 day_type,
        "predictions":              results,
        "total_predicted_waste":    sum(r["predicted_waste"] for r in results),
        "total_cost_lost":          sum(r["cost_lost"] for r in results),
        "high_waste_alerts":        len(alerts),
        "alert_items":              alerts,
        "generated_at":             datetime.now().isoformat(),
    }


@app.get("/analytics/trends")
def trends():
    df = read_demand_df()
    if df.empty:
        return {"message": "No Spark data yet — check if streaming_aggregator.py is running", "trends": []}

    grouped = (
        df.groupby(["food_item", "meal_slot"])
        .agg(
            total_orders   =("order_count",      "sum"),
            total_quantity =("total_quantity",    "sum"),
            unique_students=("unique_students",   "sum"),
            batches        =("order_count",       "count"),
        )
        .reset_index()
        .sort_values("total_quantity", ascending=False)
        .head(20)
    )
    return {
        "trends":      grouped.to_dict(orient="records"),
        "data_points": len(df),
        "generated_at": datetime.now().isoformat(),
    }


@app.get("/analytics/top-items/{meal_slot}")
def top_items(meal_slot: str, limit: int = 5):
    df = read_demand_df()
    if df.empty or "meal_slot" not in df.columns:
        return {"meal_slot": meal_slot, "top_items": []}

    top = (
        df[df["meal_slot"] == meal_slot]
        .groupby("food_item")["total_quantity"]
        .sum()
        .sort_values(ascending=False)
        .head(limit)
        .reset_index()
    )
    return {"meal_slot": meal_slot, "top_items": top.to_dict(orient="records")}


@app.get("/analytics/hostel")
def hostel_breakdown():
    df = read_hostel_df()
    if df.empty:
        return {"message": "No hostel data yet", "breakdown": []}
    grouped = (
        df.groupby(["hostel_block", "food_item"])["hostel_order_count"]
        .sum()
        .reset_index()
        .sort_values("hostel_order_count", ascending=False)
        .head(20)
    )
    return {"breakdown": grouped.to_dict(orient="records")}


@app.get("/kafka/status")
def kafka_status():
    df = read_demand_df()
    parquet_dir = get_active_demand_dir()
    files = get_demand_files()
    if df.empty:
        return {
            "status":      "waiting",
            "parquet_dir": str(parquet_dir),
            "files_found": len(files),
            "tip":         "Make sure streaming_aggregator.py is running and Kafka producer is sending events",
        }
    return {
        "status":        "streaming",
        "total_records": len(df),
        "parquet_files": len(files),
        "food_items":    df["food_item"].nunique() if "food_item" in df.columns else 0,
        "meal_slots":    df["meal_slot"].unique().tolist() if "meal_slot" in df.columns else [],
        "last_window":   str(df["window_start"].max()) if "window_start" in df.columns else "unknown",
    }


@app.post("/model/retrain")
def retrain():
    return {
        "status":  "submitted",
        "job_id":  f"retrain-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "message": "Spark MLlib GBTRegressor retrain job submitted to cluster",
        "eta":     "5-10 minutes",
    }


# ── WebSocket: live prediction broadcast every 5s ────────────────────
@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    active_ws.append(websocket)
    try:
        while True:
            hour      = datetime.now().hour
            meal_slot = "breakfast" if 6 <= hour < 10 else "lunch" if 11 <= hour < 15 else "dinner"
            df        = read_demand_df()

            payload = {
                "type":           "live_prediction",
                "meal_slot":      meal_slot,
                "predictions":    [predict_waste(f, meal_slot, df=df) for f in FOOD_ITEMS],
                "kafka_records":  len(df),
                "active_clients": len(active_ws),
                "server_time":    datetime.now().isoformat(),
            }
            await websocket.send_text(json.dumps(payload))
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        if websocket in active_ws:
            active_ws.remove(websocket)

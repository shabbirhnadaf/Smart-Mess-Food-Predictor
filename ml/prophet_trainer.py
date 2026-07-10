"""
Time-Series Trainer using statsmodels SARIMAX (replaces Prophet)
Seasonal ARIMA captures weekly + exam-cycle seasonality
"""
import os, json, pickle, warnings
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")

HDFS       = os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")
FEAT_PATH  = f"{HDFS}/mess-predictor/processed/features"
SAVE_DIR   = Path("./ml/saved_models/sarimax")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

FOOD_ITEMS = ["idli","dosa","poha","upma","bread_butter","rice",
              "chapati","dal","sambar","rasam","veg_curry",
              "egg_curry","curd_rice","pulao","biryani"]
SLOTS      = ["breakfast","lunch","dinner"]

def build_spark():
    return (SparkSession.builder
            .appName("SAIMARXTrainer")
            .config("spark.hadoop.fs.defaultFS", HDFS)
            .config("spark.sql.shuffle.partitions","4")
            .getOrCreate())

def load_series(spark, slot, food_item):
    df = (spark.read.parquet(f"{FEAT_PATH}/meal_slot={slot}")
          .filter(f"food_item = '{food_item}'")
          .select("date","quantity_consumed")
          .orderBy("date")
          .toPandas())
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").asfreq("D").fillna(method="ffill")
    return df["quantity_consumed"].astype(float)

def train_sarimax(series):
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    # SARIMA(1,1,1)(1,1,1,7) — weekly seasonality
    model = SARIMAX(series,
                    order=(1,1,1),
                    seasonal_order=(1,1,1,7),
                    enforce_stationarity=False,
                    enforce_invertibility=False)
    result = model.fit(disp=False, maxiter=50)
    # Quick MAPE on last 14 days
    forecast = result.predict(start=len(series)-14, end=len(series)-1)
    actual   = series.iloc[-14:]
    mape = float(np.mean(np.abs((actual - forecast) / (actual + 1e-6))) * 100)
    return result, mape

def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    saved, failed = 0, 0

    for slot in SLOTS:
        logger.info(f"--- Slot: {slot} ---")
        for food in FOOD_ITEMS:
            try:
                series = load_series(spark, slot, food)
                if series is None or len(series) < 30:
                    logger.warning(f"  Skipped {slot}/{food} (insufficient data)")
                    continue
                result, mape = train_sarimax(series)
                model_path = SAVE_DIR / f"sarimax_{slot}_{food}.pkl"
                with open(model_path, "wb") as f:
                    pickle.dump(result, f)
                logger.info(f"  Trained {slot}/{food} | MAPE={mape:.1f}% -> {model_path.name}")
                saved += 1
            except Exception as e:
                logger.error(f"  Failed {slot}/{food}: {e}")
                failed += 1

    spark.stop()
    summary = {"saved": saved, "failed": failed, "model_dir": str(SAVE_DIR)}
    print(json.dumps(summary, indent=2))
    logger.info(f"SARIMAX Training COMPLETE. Saved:{saved} Failed:{failed}")

if __name__ == "__main__":
    run()
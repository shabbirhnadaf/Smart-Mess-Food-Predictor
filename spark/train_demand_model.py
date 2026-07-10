"""
Trains PySpark MLlib GBTRegressor per meal_slot, saves models.
Run: spark-submit --master local[*] spark/train_demand_model.py
"""
import os,json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from dotenv import load_dotenv

load_dotenv()
HDFS_NN    = os.getenv("HDFS_NAMENODE","file:///c:/mess-predictor/data/hdfs_mock")
HDFS_BASE  = os.getenv("HDFS_BASE_PATH","/")
FEAT_PATH  = f"{HDFS_NN}{HDFS_BASE}/processed/features"
MDL_HDFS   = f"{HDFS_NN}{HDFS_BASE}/models"
MDL_LOCAL  = os.getenv("MODEL_LOCAL_PATH","./ml/saved_models")
SEED_CSV   = os.getenv("SEED_CSV","data/seed/historical_mess_data.csv")
SLOTS      = ["breakfast","lunch","dinner"]
FEAT_COLS  = ["dow_sin","dow_cos","month_sin","month_cos","week_sin","week_cos",
              "hostel_occupancy_rate","event_impact_score","weather_temp",
              "is_holiday","is_exam_week","rolling_7d_avg","lag1","lag7","lag14",
              "rolling_7d_std","occ_event_interaction"]

def spark_session():
    return (SparkSession.builder.appName("MessModelTraining")
            .master(os.getenv("SPARK_MASTER","local[*]"))
            .config("spark.executor.memory","2g").config("spark.driver.memory","2g")
            .config("spark.sql.shuffle.partitions","8")
            .config("spark.hadoop.fs.defaultFS",HDFS_NN).getOrCreate())

def load_features(spark):
    try:
        df=spark.read.parquet(FEAT_PATH)
        print(f"[INFO] Feature rows:{df.count():,}"); return df
    except Exception as e:
        print(f"[WARN] Features not found({e}). Generating from CSV...")
        import subprocess
        subprocess.run(["python","spark/batch_feature_engineering.py"])
        return spark.read.parquet(FEAT_PATH)

def build_pipeline(feat_cols):
    assembler=VectorAssembler(inputCols=feat_cols,outputCol="raw_features",
                              handleInvalid="keep")
    scaler=StandardScaler(inputCol="raw_features",outputCol="features",
                          withStd=True,withMean=True)
    gbt=GBTRegressor(featuresCol="features",labelCol="quantity_consumed",
                     maxIter=50,maxDepth=6,stepSize=0.1,subsamplingRate=0.8,
                     featureSubsetStrategy="sqrt",seed=42)
    return Pipeline(stages=[assembler,scaler,gbt])

def run():
    spark=spark_session(); spark.sparkContext.setLogLevel("WARN")
    os.makedirs(MDL_LOCAL,exist_ok=True)
    df=load_features(spark)
    # fill missing feature cols with 0
    for c in FEAT_COLS:
        if c not in df.columns:
            df=df.withColumn(c,F.lit(0.0))
    metrics={}
    ev=RegressionEvaluator(labelCol="quantity_consumed",predictionCol="prediction")
    for slot in SLOTS:
        print(f"\n--- Training slot: {slot} ---")
        slot_df=df.filter(F.col("meal_slot")==slot).cache()
        row_count=slot_df.count()
        if row_count<50:
            print(f"[WARN] Not enough rows for {slot}({row_count}). Skipping."); continue
        train,test=slot_df.randomSplit([0.8,0.2],seed=42)
        pipe=build_pipeline(FEAT_COLS)
        model=pipe.fit(train)
        preds=model.transform(test)
        rmse=ev.setMetricName("rmse").evaluate(preds)
        mae =ev.setMetricName("mae").evaluate(preds)
        r2  =ev.setMetricName("r2").evaluate(preds)
        metrics[slot]={"rmse":round(rmse,3),"mae":round(mae,3),"r2":round(r2,4),"rows":row_count}
        print(f"  RMSE={rmse:.2f}  MAE={mae:.2f}  R²={r2:.4f}")
        # Save to HDFS
        hdfs_path=f"{MDL_HDFS}/gbt_{slot}"
        try: model.write().overwrite().save(hdfs_path); print(f"  Saved HDFS → {hdfs_path}")
        except Exception as e: print(f"  [WARN] HDFS save failed:{e}")
        # Save locally
        local_path=f"{MDL_LOCAL}/gbt_{slot}"
        model.write().overwrite().save(local_path)
        print(f"  Saved local -> {local_path}")
        slot_df.unpersist()
    with open(f"{MDL_LOCAL}/metrics.json","w") as f:
        json.dump(metrics,f,indent=2)
    print(f"\n=== Training complete. Metrics:\n{json.dumps(metrics,indent=2)}")
    spark.stop()

if __name__=="__main__": run()
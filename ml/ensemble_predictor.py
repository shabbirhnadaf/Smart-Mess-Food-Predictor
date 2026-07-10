"""
Ensemble predictor: combines GBT (PySpark) + Prophet predictions.
Used by FastAPI prediction service.
"""
import os,pickle,json,math
from datetime import date,datetime
from typing import Optional
import pandas as pd
import numpy as np
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

MDL_LOCAL = os.getenv("MODEL_LOCAL_PATH","./ml/saved_models")
FOODS = ["idli","dosa","poha","upma","bread_butter","rice","chapati","dal",
         "sambar","rasam","veg_curry","egg_curry","curd_rice","pulao","biryani"]
SLOTS = ["breakfast","lunch","dinner"]
IMPACT= {"cultural_fest":1.30,"sports_day":1.20,"fresher_party":1.40,
         "convocation":1.25,"exam_week":0.95,"holiday":0.60,"none":1.00}
PI2   = 2*math.pi

_gbt_models:    dict = {}
_prophet_cache: dict = {}
_spark: Optional[SparkSession] = None

def get_spark():
    global _spark
    if _spark is None:
        _spark=(SparkSession.builder.appName("MessPredictor")
                .master("local[2]").config("spark.driver.memory","1g")
                .config("spark.executor.memory","1g").getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark

def load_gbt(slot:str) -> Optional[PipelineModel]:
    if slot not in _gbt_models:
        path=os.path.join(MDL_LOCAL,f"gbt_{slot}")
        try:
            _gbt_models[slot]=PipelineModel.load(path)
            logger.info(f"GBT loaded:{slot}")
        except Exception as e:
            logger.error(f"GBT load failed {slot}:{e}"); return None
    return _gbt_models.get(slot)

def load_prophet(slot:str,item:str) -> Optional[object]:
    key=f"{slot}_{item}"
    if key not in _prophet_cache:
        path=os.path.join(MDL_LOCAL,f"prophet_{key}.pkl")
        try:
            with open(path,"rb") as f: _prophet_cache[key]=pickle.load(f)
        except Exception as e:
            logger.warning(f"Prophet not found {key}:{e}"); return None
    return _prophet_cache.get(key)

def make_features(target_date:date,slot:str,hostel_count:int,
                  weather_temp:float,event_name:str,
                  is_holiday:int,is_exam_week:int,
                  hist_df: Optional[pd.DataFrame]=None) -> dict:
    d=target_date
    dow=d.weekday()+1
    m=d.month
    woy=d.isocalendar()[1]
    occ=hostel_count/500.0
    ei=IMPACT.get(event_name,1.0)
    lag1=lag7=lag14=rolling_7d_avg=rolling_7d_std=0.0
    if hist_df is not None and len(hist_df)>0:
        vals=hist_df["quantity_consumed"].values
        rolling_7d_avg=float(np.mean(vals[-7:])) if len(vals)>=7 else float(np.mean(vals))
        rolling_7d_std=float(np.std(vals[-7:])) if len(vals)>=7 else 0.0
        lag1 =float(vals[-1]) if len(vals)>=1 else 0.0
        lag7 =float(vals[-7]) if len(vals)>=7 else lag1
        lag14=float(vals[-14]) if len(vals)>=14 else lag1
    return {
        "dow_sin":math.sin(PI2*dow/7),"dow_cos":math.cos(PI2*dow/7),
        "month_sin":math.sin(PI2*m/12),"month_cos":math.cos(PI2*m/12),
        "week_sin":math.sin(PI2*woy/52),"week_cos":math.cos(PI2*woy/52),
        "hostel_occupancy_rate":occ,"event_impact_score":ei,
        "weather_temp":weather_temp,"is_holiday":is_holiday,"is_exam_week":is_exam_week,
        "rolling_7d_avg":rolling_7d_avg,"rolling_7d_std":rolling_7d_std,
        "lag1":lag1,"lag7":lag7,"lag14":lag14,
        "occ_event_interaction":occ*ei
    }

def predict_gbt(slot:str,features:dict) -> Optional[float]:
    model=load_gbt(slot)
    if model is None: return None
    spark=get_spark()
    row=[features]
    df=spark.createDataFrame(row)
    preds=model.transform(df)
    val=preds.select("prediction").first()
    return max(0.0,float(val["prediction"])) if val else None

def predict_prophet(slot:str,item:str,target_date:date) -> Optional[float]:
    m=load_prophet(slot,item)
    if m is None: return None
    future=pd.DataFrame({"ds":[pd.Timestamp(target_date)]})
    fc=m.predict(future)
    return max(0.0,float(fc["yhat"].values[0]))

def predict_demand(slot:str,item:str,target_date:date,
                   hostel_count:int=480,weather_temp:float=27.0,
                   event_name:str="none",is_holiday:int=0,
                   is_exam_week:int=0,
                   hist_df:Optional[pd.DataFrame]=None) -> dict:
    feats=make_features(target_date,slot,hostel_count,weather_temp,
                        event_name,is_holiday,is_exam_week,hist_df)
    gbt_pred   =predict_gbt(slot,feats)
    prophet_pred=predict_prophet(slot,item,target_date)
    if gbt_pred is not None and prophet_pred is not None:
        final=round(0.60*gbt_pred+0.40*prophet_pred,1)
        source="ensemble"
    elif gbt_pred is not None:
        final=round(gbt_pred,1); source="gbt_only"
    elif prophet_pred is not None:
        final=round(prophet_pred,1); source="prophet_only"
    else:
        final=float(feats["rolling_7d_avg"]); source="fallback_avg"
    mean=feats["rolling_7d_avg"]; std=feats["rolling_7d_std"]
    is_anomaly=bool(std>0 and abs(final-mean)>2*std)
    return {"food_item":item,"meal_slot":slot,"predicted_qty":final,
            "gbt_pred":gbt_pred,"prophet_pred":prophet_pred,
            "source":source,"is_anomaly":is_anomaly,
            "anomaly_msg":f"Demand spike: {final:.0f} vs avg {mean:.0f}" if is_anomaly else None}
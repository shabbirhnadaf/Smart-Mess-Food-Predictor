from fastapi import APIRouter, BackgroundTasks
import subprocess, os
from datetime import datetime

router = APIRouter()

def run_training():
    subprocess.run(["spark-submit","--master","local[*]",
                    "spark/train_demand_model.py"])

def run_feature_engineering():
    subprocess.run(["spark-submit","--master","local[*]",
                    "spark/batch_feature_engineering.py"])

@router.post("/retrain")
async def trigger_retrain(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_training)
    return {"status":"training_started",
            "message":"GBT model retraining triggered in background",
            "timestamp":datetime.now().isoformat()}

@router.post("/rerun-features")
async def trigger_features(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_feature_engineering)
    return {"status":"feature_engineering_started",
            "timestamp":datetime.now().isoformat()}

@router.get("/health")
async def health():
    mdl_dir = os.getenv("MODEL_LOCAL_PATH","./ml/saved_models")
    models  = os.listdir(mdl_dir) if os.path.exists(mdl_dir) else []
    return {"status":"healthy","models_available":models,
            "timestamp":datetime.now().isoformat()}
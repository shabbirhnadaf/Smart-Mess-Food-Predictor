from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from datetime import date
from typing import Optional, List
from ml.ensemble_predictor import predict_demand, SLOTS, FOODS

router = APIRouter()

class PredictRequest(BaseModel):
    meal_slot:    str   = Field(..., example="lunch")
    target_date:  date  = Field(default_factory=date.today)
    hostel_count: int   = Field(default=480, ge=0, le=600)
    weather_temp: float = Field(default=27.0)
    event_name:   str   = Field(default="none")
    is_holiday:   int   = Field(default=0, ge=0, le=1)
    is_exam_week: int   = Field(default=0, ge=0, le=1)
    food_items:   Optional[List[str]] = None

class PredictResponse(BaseModel):
    meal_slot:    str
    target_date:  date
    predictions:  list
    alert_count:  int
    timestamp:    str

@router.post("/demand", response_model=PredictResponse)
async def predict_demand_endpoint(req: PredictRequest):
    if req.meal_slot not in SLOTS:
        raise HTTPException(400, f"meal_slot must be one of {SLOTS}")
    items = req.food_items or FOODS
    results = []
    for item in items:
        if item not in FOODS:
            continue
        pred = predict_demand(
            slot=req.meal_slot, item=item,
            target_date=req.target_date,
            hostel_count=req.hostel_count,
            weather_temp=req.weather_temp,
            event_name=req.event_name,
            is_holiday=req.is_holiday,
            is_exam_week=req.is_exam_week
        )
        results.append(pred)
    from datetime import datetime
    return PredictResponse(
        meal_slot=req.meal_slot,
        target_date=req.target_date,
        predictions=results,
        alert_count=sum(1 for r in results if r.get("is_anomaly")),
        timestamp=datetime.now().isoformat()
    )

@router.get("/slots")
async def get_slots():
    return {"slots": SLOTS}

@router.get("/food-items")
async def get_food_items():
    return {"food_items": FOODS}
from fastapi import APIRouter
from datetime import date, timedelta
import pandas as pd, os
from dotenv import load_dotenv

load_dotenv()
router    = APIRouter()
SEED_CSV  = os.getenv("SEED_CSV","data/seed/historical_mess_data.csv")

@router.get("/trends")
async def get_trends(days: int = 7, meal_slot: str = "lunch"):
    try:
        df = pd.read_csv(SEED_CSV, parse_dates=["date"])
        cutoff = pd.Timestamp(date.today() - timedelta(days=days))
        filtered = df[(df["meal_slot"]==meal_slot) & (df["date"]>=cutoff)]
        grouped = (filtered.groupby(["date","food_item"])["quantity_consumed"]
                           .sum().reset_index())
        return {"status":"ok","meal_slot":meal_slot,"days":days,
                "data":grouped.to_dict(orient="records")}
    except Exception as e:
        return {"status":"error","message":str(e),"data":[]}

@router.get("/summary")
async def get_summary():
    try:
        df = pd.read_csv(SEED_CSV)
        summary = (df.groupby(["meal_slot","food_item"])["quantity_consumed"]
                     .agg(["mean","std","min","max"])
                     .round(2).reset_index())
        return {"status":"ok","data":summary.to_dict(orient="records")}
    except Exception as e:
        return {"status":"error","message":str(e)}

@router.get("/top-items")
async def top_items(meal_slot: str = "lunch", top_n: int = 5):
    try:
        df = pd.read_csv(SEED_CSV)
        top = (df[df["meal_slot"]==meal_slot]
               .groupby("food_item")["quantity_consumed"].mean()
               .nlargest(top_n).reset_index())
        return {"meal_slot":meal_slot,
                "top_items":top.to_dict(orient="records")}
    except Exception as e:
        return {"status":"error","message":str(e)}